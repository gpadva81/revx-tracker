// RevX Click Tracker v2
// Drop on driverrates.com / save-on-insurance.com
// Non-redirect, Meta-compliant click tracking
//
// Usage: add before </body>:
//   <script>var REVX_SOURCE = 'meta';</script>
//   <script src="https://track.revxglobal.com/tracker.js"></script>

(function() {
  'use strict';

  var TRACKER_URL = 'https://track.activefunnel.ai';
  var SOURCE = window.REVX_SOURCE || 'unknown';

  // Click ID starts null — set once server responds
  var clickId = null;
  var clickReady = false;
  var pendingClicks = []; // queue outbound clicks until we have the ID

  // --- Helpers ---
  function getUrlParam(name) {
    var match = RegExp('[?&]' + name + '=([^&]*)').exec(window.location.search);
    return match ? decodeURIComponent(match[1]) : null;
  }

  // --- Capture inbound params ---
  var gclid = getUrlParam('gclid');
  var fbclid = getUrlParam('fbclid');
  var campaignId = getUrlParam('utm_campaign') || getUrlParam('campaign_id') || getUrlParam('sub1');
  var adgroupId = getUrlParam('utm_content') || getUrlParam('adgroup_id') || getUrlParam('sub2');
  var keyword = getUrlParam('utm_term') || getUrlParam('keyword') || getUrlParam('sub3');

  // --- Register click with server (POST, gets back server-generated click_id) ---
  function registerClick() {
    var payload = {
      source: SOURCE,
      gclid: gclid || '',
      fbclid: fbclid || '',
      campaign_id: campaignId || '',
      adgroup_id: adgroupId || '',
      keyword: keyword || '',
      landing_page: window.location.hostname,
      offer_url: '' // set later if needed
    };

    fetch(TRACKER_URL + '/click', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      keepalive: true, // survives page unload
    })
    .then(function(resp) { return resp.json(); })
    .then(function(data) {
      if (data && data.click_id) {
        clickId = data.click_id;
        clickReady = true;

        // Store in sessionStorage for cross-page persistence
        try {
          sessionStorage.setItem('revx_click_id', clickId);
          sessionStorage.setItem('revx_gclid', gclid || '');
          sessionStorage.setItem('revx_fbclid', fbclid || '');
        } catch(e) {}

        // Process any queued outbound clicks
        pendingClicks.forEach(function(fn) { fn(clickId); });
        pendingClicks = [];

        console.log('[RevX] Click registered: ' + clickId);
      }
    })
    .catch(function(err) {
      console.warn('[RevX] Click registration failed:', err);
      // Fallback: generate client-side ID so tracking isn't completely lost
      // SmartFinancial postbacks will create orphan records (handled server-side)
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
  function goToOffer(href, cid) {
    window.location.href = buildTrackedUrl(href, cid);
  }

  // --- Intercept outbound clicks ---
  function setupClickInterception() {
    // Links (<a> tags)
    document.addEventListener('click', function(e) {
      var link = e.target.closest ? e.target.closest('a') : null;
      if (!link) return;

      var href = link.href || '';
      if (!isOfferLink(href)) return;

      e.preventDefault();

      if (clickReady) {
        goToOffer(href, clickId);
      } else {
        // Click happened before server responded — queue it
        pendingClicks.push(function(cid) { goToOffer(href, cid); });
      }
    });

    // Buttons with data-offer-url attribute
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

  // --- Init ---
  function init() {
    // Check if we already have a click ID from a previous page in this session
    try {
      var existingId = sessionStorage.getItem('revx_click_id');
      if (existingId) {
        clickId = existingId;
        clickReady = true;
        console.log('[RevX] Restored click ID from session: ' + clickId);
      }
    } catch(e) {}

    // Always register a new click (tracks each page view)
    registerClick();
    setupClickInterception();
  }

  // Run when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
