// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// The script of every console page. The console's Content-Security-Policy
// runs no inline script, so this file holds what the pages ran as inline
// code.

// Add the session's CSRF token to every POST form.
document.addEventListener('DOMContentLoaded', function () {
  var t = document.querySelector('meta[name="csrf-token"]');
  if (!t) return;
  var v = t.getAttribute('content');
  document.querySelectorAll('form').forEach(function (f) {
    if (f.method && f.method.toLowerCase() === 'post') {
      var i = document.createElement('input');
      i.type = 'hidden';
      i.name = '_csrf';
      i.value = v;
      f.appendChild(i);
    }
  });
});

// A form with a data-confirm attribute asks before it submits. The question
// is attribute text, so a quote in a name cannot end a script string.
document.addEventListener('submit', function (e) {
  var question = e.target.getAttribute('data-confirm');
  if (question !== null && !window.confirm(question)) {
    e.preventDefault();
  }
});

// Cache page: hide the selector fields that do not apply to the chosen
// scope. The server ignores the values of hidden fields (the shared `apply`
// helper validates per scope), so this only changes what the form shows.
// Without the script, every field is visible.
(function () {
  var sel = document.getElementById('scope');
  if (!sel) return;
  var rows = document.querySelectorAll('.sel');
  function apply() {
    var s = sel.value;
    rows.forEach(function (row) {
      var scopes = row.getAttribute('data-scopes').split(' ');
      row.style.display = scopes.indexOf(s) >= 0 ? '' : 'none';
    });
  }
  sel.addEventListener('change', apply);
  apply();
})();
