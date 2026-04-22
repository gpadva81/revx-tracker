// RevX Click + Funnel Tracker v3
// Drop on driverrates.com / save-on-insurance.com / quoteshiftauto.com
// Non-redirect, Meta-compliant click tracking + full funnel visibility
//
// Usage: add before </body>:
//   <script>var REVX_SOURCE = 'meta';</script>
//   <script>var REVX_STEP = 'offer';</script>  <!-- optional: override step detection -->
//   <script src="https://track.activefunnel.ai/tracker.js"></script>

(function() {
  'use strict';

  var TRACKER_URL = 'https://track.activefunnel.ai';
  var SOURCE = window.REVX_SOURCE || 'unknown';
  var CAMPAIGN = window.REVX_CAMPAIGN || '';

  // Click ID starts null — set once server responds
  var clickId = null;
  var clickReady = false;
  var pendingClicks = [];

  // Offer URL returned by server for campaign mode (replaces hardcoded offer links)
  var serverOfferUrl = null;

  // Visitor ID — persisted in cookie
  var visitorId = null;

  // Page start time for time-on-page tracking
  var pageStartTime = Date.now();

  // Scroll milestones already sent
  var scrollMilestonesSent = {};

  // Form interaction tracking
  var formInteracted = false;

  // Event queue for batching non-critical events
  var eventQueue = [];
  var eventFlushTimer = null;

  // --- UUID Generation ---
  function generateUUID() {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
    // Fallback for older browsers
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = Math.random() * 16 | 0;
      var v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  // --- Cookie Helpers ---
  function setCookie(name, value, days) {
    var expires = '';
    if (days) {
      var date = new Date();
      date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
      expires = '; expires=' + date.toUTCString();
    }
    document.cookie = name + '=' + encodeURIComponent(value) + expires + '; path=/; SameSite=Lax';
  }

  function getCookie(name) {
    var nameEQ = name + '=';
    var ca = document.cookie.split(';');
    for (var i = 0; i < ca.length; i++) {
      var c = ca[i];
      while (c.charAt(0) === ' ') c = c.substring(1, c.length);
      if (c.indexOf(nameEQ) === 0) {
        try {
          return decodeURIComponent(c.substring(nameEQ.length, c.length));
        } catch (e) {
          return c.substring(nameEQ.length, c.length);
        }
      }
    }
    return null;
  }

  // --- Get or Create Visitor ID ---
  function getOrCreateVisitorId() {
    var existing = getCookie('_rvx');
    if (existing && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(existing)) {
      return existing;
    }
    var newId = generateUUID();
    setCookie('_rvx', newId, 365); // 1 year persistence
    return newId;
  }

  // --- URL Helpers ---
  function getUrlParam(name) {
    var match = RegExp('[?&]' + name + '=([^&]*)').exec(window.location.search);
    return match ? decodeURIComponent(match[1]) : null;
  }

  // --- Funnel Step Detection ---
  function detectStep() {
    // Allow page-level override
    if (window.REVX_STEP) return window.REVX_STEP;
    var p = (window.location.pathname + window.location.search).toLowerCase();
    if (/thank|confirm|success|complete|done|finish|receipt/.test(p)) return 'conversion';
    if (/offer|quote|compare|rate|result|price/.test(p)) return 'offer';
    if (/presale|presell|bridge|warm|intro/.test(p)) return 'presale';
    return 'landing';
  }

  // --- Capture inbound params ---
  var gclid = getUrlParam('gclid');
  var fbclid = getUrlParam('fbclid');
  var campaignId = getUrlParam('utm_campaign') || getUrlParam('campaign_id') || getUrlParam('sub1');
  var adgroupId = getUrlParam('utm_content') || getUrlParam('adgroup_id') || getUrlParam('sub2');
  var keyword = getUrlParam('utm_term') || getUrlParam('keyword') || getUrlParam('sub3');

  // --- Detect current funnel step ---
  var currentStep = detectStep();
  var currentStepUrl = window.location.href;

  // --- Safe JSON POST ---
  function jsonPost(url, payload, keepalive) {
    try {
      return fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        keepalive: !!keepalive,
      });
    } catch (e) {
      return Promise.reject(e);
    }
  }

  // --- Send Beacon (survives page unload) ---
  function sendBeacon(url, payload) {
    try {
      var blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
      if (typeof navigator !== 'undefined' && navigator.sendBeacon) {
        return navigator.sendBeacon(url, blob);
      }
    } catch (e) {}
    // Fallback: sync XHR (last resort)
    try {
      var xhr = new XMLHttpRequest();
      xhr.open('POST', url, false);
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.send(JSON.stringify(payload));
    } catch (e) {}
  }

  // --- Build event payload ---
  function buildEventPayload(events) {
    return {
      visitor_id: visitorId,
      click_id: clickId || undefined,
      step: currentStep,
      step_url: currentStepUrl,
      events: events,
    };
  }

  // --- Flush queued events to /event ---
  function flushEvents(force) {
    if (eventQueue.length === 0) return;
    var toSend = eventQueue.splice(0, eventQueue.length);
    var payload = buildEventPayload(toSend);
    if (force) {
      sendBeacon(TRACKER_URL + '/event', payload);
    } else {
      jsonPost(TRACKER_URL + '/event', payload).catch(function() {});
    }
  }

  // --- Queue an event (batched) ---
  function queueEvent(type, extra) {
    var ev = {
      type: type,
      page_url: window.location.href,
      referrer: document.referrer || '',
      timestamp: new Date().toISOString(),
    };
    if (extra) ev.metadata = extra;
    eventQueue.push(ev);

    // Debounce flush: send after 2s of no new events
    if (eventFlushTimer) clearTimeout(eventFlushTimer);
    eventFlushTimer = setTimeout(function() { flushEvents(false); }, 2000);
  }

  // --- Send a single immediate event ---
  function sendEvent(type, extra) {
    var ev = {
      type: type,
      page_url: window.location.href,
      referrer: document.referrer || '',
      timestamp: new Date().toISOString(),
    };
    if (extra) ev.metadata = extra;
    jsonPost(TRACKER_URL + '/event', buildEventPayload([ev]), true).catch(function() {});
  }

  // --- Register click with server ---
  function registerClick() {
    var payload = {
      source: SOURCE,
      campaign: CAMPAIGN || '',
      gclid: gclid || '',
      fbclid: fbclid || '',
      campaign_id: campaignId || '',
      adgroup_id: adgroupId || '',
      keyword: keyword || '',
      landing_page: window.location.hostname,
      offer_url: '',
      visitor_id: visitorId,
    };

    jsonPost(TRACKER_URL + '/click', payload, true)
      .then(function(resp) { return resp.json(); })
      .then(function(data) {
        if (data && data.click_id) {
          clickId = data.click_id;
          clickReady = true;

          // Store server-provided offer URL (campaign mode)
          if (data.offer_url) {
            serverOfferUrl = data.offer_url;
            try { sessionStorage.setItem('revx_offer_url', serverOfferUrl); } catch(e) {}
          }

          try {
            sessionStorage.setItem('revx_click_id', clickId);
            sessionStorage.setItem('revx_gclid', gclid || '');
            sessionStorage.setItem('revx_fbclid', fbclid || '');
          } catch(e) {}

          pendingClicks.forEach(function(fn) { fn(clickId); });
          pendingClicks = [];

          console.log('[RevX] Click registered: ' + clickId + (serverOfferUrl ? ' (offer routed)' : ''));
        }
      })
      .catch(function(err) {
        console.warn('[RevX] Click registration failed:', err);
        clickId = 'client-' + Math.random().toString(36).substr(2, 12);
        clickReady = true;
        try { sessionStorage.setItem('revx_click_id', clickId); } catch(e) {}
        pendingClicks.forEach(function(fn) { fn(clickId); });
        pendingClicks = [];
      });
  }

  // --- Build tracked URL ---
  function buildTrackedUrl(baseUrl, cid) {
    var separator = baseUrl.indexOf('?') !== -1 ? '&' : '?';
    return baseUrl +
      separator + 'tid=' + encodeURIComponent(cid) +
      '&sub1=' + encodeURIComponent(campaignId || '') +
      '&sub2=' + encodeURIComponent(adgroupId || '') +
      '&sub3=' + encodeURIComponent(keyword || '');
  }

  // --- Check if link is an offer URL ---
  function isOfferLink(href) {
    if (!href) return false;
    return href.indexOf('midasrates.com') !== -1 ||
           href.indexOf('smartfinancial.com') !== -1 ||
           href.indexOf('get-fast-quote') !== -1;
  }

  // --- Navigate to tracked offer ---
  // In campaign mode, serverOfferUrl is pre-built by the server (has tid baked in).
  // In legacy mode (no campaign), buildTrackedUrl appends tid + sub params.
  function goToOffer(href, cid) {
    var destination = serverOfferUrl || buildTrackedUrl(href, cid);
    sendEvent('cta_click', { destination: destination });
    window.location.href = destination;
  }

  // --- Intercept outbound clicks ---
  function setupClickInterception() {
    document.addEventListener('click', function(e) {
      var link = e.target.closest ? e.target.closest('a') : null;
      if (!link) return;
      var href = link.href || '';
      if (!isOfferLink(href)) return;
      e.preventDefault();
      if (clickReady) {
        goToOffer(href, clickId);
      } else {
        pendingClicks.push(function(cid) { goToOffer(href, cid); });
      }
    });

    var offerButtons = document.querySelectorAll('[data-offer-url]');
    for (var i = 0; i < offerButtons.length; i++) {
      (function(el) {
        el.addEventListener('click', function(e) {
          e.preventDefault();
          var baseUrl = el.getAttribute('data-offer-url');
          if (clickReady) {
            goToOffer(baseUrl, clickId);
          } else {
            pendingClicks.push(function(cid) { goToOffer(baseUrl, cid); });
          }
        });
      })(offerButtons[i]);
    }
  }

  // --- Scroll Depth Tracking ---
  function setupScrollTracking() {
    var milestones = [25, 50, 75, 100];
    var ticking = false;

    function checkScroll() {
      var docHeight = Math.max(
        document.body.scrollHeight, document.documentElement.scrollHeight,
        document.body.offsetHeight, document.documentElement.offsetHeight
      );
      var winHeight = window.innerHeight;
      var scrolled = window.pageYOffset || document.documentElement.scrollTop;
      var pct = Math.round(((scrolled + winHeight) / docHeight) * 100);

      milestones.forEach(function(m) {
        if (pct >= m && !scrollMilestonesSent[m]) {
          scrollMilestonesSent[m] = true;
          queueEvent('scroll', { depth: m });
        }
      });
      ticking = false;
    }

    window.addEventListener('scroll', function() {
      if (!ticking) {
        ticking = true;
        if (typeof requestAnimationFrame !== 'undefined') {
          requestAnimationFrame(checkScroll);
        } else {
          setTimeout(checkScroll, 16);
        }
      }
    }, { passive: true });
  }

  // --- Time on Page Tracking ---
  function setupTimeTracking() {
    function sendTimeOnPage() {
      var duration = Date.now() - pageStartTime;
      if (duration < 1000) return; // ignore sub-second visits
      var payload = buildEventPayload([{
        type: 'time_on_page',
        page_url: window.location.href,
        referrer: document.referrer || '',
        timestamp: new Date().toISOString(),
        metadata: { duration_ms: duration },
      }]);
      sendBeacon(TRACKER_URL + '/event', payload);
    }

    // visibilitychange fires when tab is hidden (more reliable than beforeunload)
    document.addEventListener('visibilitychange', function() {
      if (document.visibilityState === 'hidden') {
        // Flush queued events first
        if (eventQueue.length > 0) {
          var toSend = eventQueue.splice(0, eventQueue.length);
          var payload = buildEventPayload(toSend);
          sendBeacon(TRACKER_URL + '/event', payload);
        }
        sendTimeOnPage();
      }
    });

    // Fallback: beforeunload
    window.addEventListener('beforeunload', function() {
      sendTimeOnPage();
    });
  }

  // --- Form Interaction Tracking ---
  function setupFormTracking() {
    // form_start: first time user interacts with a form field
    document.addEventListener('focusin', function(e) {
      var el = e.target;
      if (!el) return;
      var tag = (el.tagName || '').toLowerCase();
      if ((tag === 'input' || tag === 'textarea' || tag === 'select') && !formInteracted) {
        formInteracted = true;
        queueEvent('form_start', { field_type: el.type || tag });
      }
    });

    // form_submit: any form submission
    document.addEventListener('submit', function(e) {
      var form = e.target;
      queueEvent('form_submit', {
        form_id: form.id || null,
        form_action: (form.action || '').slice(0, 200),
      });
    });
  }

  // --- Pageview Event ---
  function sendPageview() {
    sendEvent('pageview', {
      title: document.title || '',
      source: SOURCE,
      step: currentStep,
    });
  }

  // --- Init ---
  function init() {
    // Get or create persistent visitor ID
    visitorId = getOrCreateVisitorId();

    // Restore click ID + offer URL from session storage if available
    try {
      var existingId = sessionStorage.getItem('revx_click_id');
      if (existingId) {
        clickId = existingId;
        clickReady = true;
        console.log('[RevX] Restored click ID from session: ' + clickId);
      }
      var existingOffer = sessionStorage.getItem('revx_offer_url');
      if (existingOffer) {
        serverOfferUrl = existingOffer;
      }
    } catch(e) {}

    // Register a fresh click for this page
    registerClick();

    // Send pageview (after a brief wait so clickId might be available)
    setTimeout(sendPageview, 100);

    // Set up tracking
    setupClickInterception();
    setupScrollTracking();
    setupTimeTracking();
    setupFormTracking();
  }

  // Run when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
