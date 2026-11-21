// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Static HTML for the metrics dashboard.
//!
//! The HTML defines the dashboard layout (D15: control/data plane split). The
//! script `console/assets/metrics.js` draws the line charts (D13), the latency
//! drill-down (D14) and the availability chart (D16), and exports CSV and
//! JSON (D19).

/// Static HTML for the redesigned metrics dashboard.
pub(crate) const METRICS_HTML: &str = r##"<h1>Metrics</h1>
<div class="card">
<p style="font-size:0.85rem;color:#666;margin-bottom:0.5rem">
Source: <span id="data-source">memory</span>.
</p>
<div style="margin-bottom:1rem;display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap">
  <strong style="font-size:0.85rem">Window:</strong>
  <a href="#" class="window-link" data-window="Last5Minutes" style="font-size:0.85rem;padding:0.25rem 0.5rem;border:1px solid #ccc;border-radius:4px;text-decoration:none;background:#2563eb;color:#fff">5 min</a>
  <a href="#" class="window-link" data-window="LastHour" style="font-size:0.85rem;padding:0.25rem 0.5rem;border:1px solid #ccc;border-radius:4px;text-decoration:none">1 hour</a>
  <a href="#" class="window-link" data-window="LastDay" style="font-size:0.85rem;padding:0.25rem 0.5rem;border:1px solid #ccc;border-radius:4px;text-decoration:none">24 hours</a>
  <span style="margin-left:1rem;font-size:0.85rem">Custom:</span>
  <input type="datetime-local" id="custom-start" style="font-size:0.8rem;padding:0.2rem" aria-label="Start time">
  <span style="font-size:0.85rem">to</span>
  <input type="datetime-local" id="custom-end" style="font-size:0.8rem;padding:0.2rem" aria-label="End time">
  <button id="custom-go" style="font-size:0.85rem;padding:0.25rem 0.5rem;cursor:pointer">Go</button>
</div>
<h2 style="font-size:1.1rem;margin-bottom:0.75rem">Data Plane</h2>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem">
  <div class="card" id="chart-latency">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h2 style="font-size:1rem;margin:0">Request Latency (&#956;s)</h2>
      <div style="display:flex;gap:0.25rem">
        <button class="btn btn-sm export-btn" data-chart="latency" data-format="csv" title="Export CSV">CSV</button>
        <button class="btn btn-sm export-btn" data-chart="latency" data-format="json" title="Export JSON">JSON</button>
      </div>
    </div>
    <div id="latency-ops" style="margin:0.5rem 0;display:flex;gap:0.25rem;flex-wrap:wrap"></div>
    <div id="latency-legend" style="font-size:0.75rem;margin-bottom:0.25rem;display:flex;gap:1rem">
      <span><span style="color:#2563eb">&#9644;</span> avg</span>
      <span><span style="color:#93c5fd">&#9644;</span> p50</span>
      <span><span style="color:#f97316">&#9644;</span> p99</span>
    </div>
    <canvas id="c-latency" height="200"></canvas>
  </div>
  <div class="card" id="chart-availability">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h2 style="font-size:1rem;margin:0">Availability (%)</h2>
      <div style="display:flex;gap:0.25rem">
        <button class="btn btn-sm export-btn" data-chart="availability" data-format="csv">CSV</button>
        <button class="btn btn-sm export-btn" data-chart="availability" data-format="json">JSON</button>
      </div>
    </div>
    <canvas id="c-availability" height="200"></canvas>
  </div>
  <div class="card" id="chart-rcu">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h2 style="font-size:1rem;margin:0">Read Capacity Units</h2>
      <div style="display:flex;gap:0.25rem">
        <button class="btn btn-sm export-btn" data-chart="rcu" data-format="csv">CSV</button>
        <button class="btn btn-sm export-btn" data-chart="rcu" data-format="json">JSON</button>
      </div>
    </div>
    <canvas id="c-rcu" height="200"></canvas>
  </div>
  <div class="card" id="chart-wcu">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h2 style="font-size:1rem;margin:0">Write Capacity Units</h2>
      <div style="display:flex;gap:0.25rem">
        <button class="btn btn-sm export-btn" data-chart="wcu" data-format="csv">CSV</button>
        <button class="btn btn-sm export-btn" data-chart="wcu" data-format="json">JSON</button>
      </div>
    </div>
    <canvas id="c-wcu" height="200"></canvas>
  </div>
</div>
<h2 style="font-size:1.1rem;margin:1.5rem 0 0.75rem">Control Plane</h2>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem">
  <div class="card" id="chart-cp-latency">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h2 style="font-size:1rem;margin:0">Control Plane Latency (&#956;s)</h2>
      <div style="display:flex;gap:0.25rem">
        <button class="btn btn-sm export-btn" data-chart="cp-latency" data-format="csv">CSV</button>
        <button class="btn btn-sm export-btn" data-chart="cp-latency" data-format="json">JSON</button>
      </div>
    </div>
    <canvas id="c-cp-latency" height="200"></canvas>
  </div>
  <div class="card" id="chart-cp-errors">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h2 style="font-size:1rem;margin:0">Control Plane Errors</h2>
      <div style="display:flex;gap:0.25rem">
        <button class="btn btn-sm export-btn" data-chart="cp-errors" data-format="csv">CSV</button>
        <button class="btn btn-sm export-btn" data-chart="cp-errors" data-format="json">JSON</button>
      </div>
    </div>
    <canvas id="c-cp-errors" height="200"></canvas>
  </div>
</div>
<h2 style="margin-top:1.5rem">Latency Breakdown</h2>
<div class="card" id="latency-breakdown">
<p style="font-size:0.85rem;color:#666;margin-bottom:0.5rem">
Per-operation average latency by request phase. Segments sum to total.
</p>
<div id="segment-bars"></div>
</div>
<h2 style="margin-top:1.5rem">Raw Metrics</h2>
<table id="metrics-table">
<thead><tr><th>Metric</th><th>Account</th><th>Table</th><th>Operation</th><th>Plane</th><th>Sum</th><th>Count</th><th>Min</th><th>Max</th></tr></thead>
<tbody id="metrics-body"></tbody>
</table>
</div>"##;
