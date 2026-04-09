"""Generate a self-contained HTML report from test harness results."""

import html
import math
from datetime import datetime
from typing import Any, Dict, List, Optional


def generate_html_report(report: Dict[str, Any], output_path: str, tenant: Optional[str] = None):
    """Generate a polished dark-theme HTML report and write it to *output_path*.

    *report* uses the same dict structure produced by ``Reporter.write_json_report``:
      - summary: {total, passed, failed, errors, skipped, pass_rate}
      - suites: {name: {tests: [...], passed, failed, errors, skipped}}
      - endpoint_latency: {endpoint: {count, min_ms, max_ms, avg_ms, p50_ms, p95_ms, p99_ms}}
      - timestamp, duration_s
    """
    summary = report.get("summary", {})
    suites = report.get("suites", {})
    endpoint_latency = report.get("endpoint_latency", {})
    timestamp = report.get("timestamp", datetime.now().isoformat())
    duration_s = report.get("duration_s", 0)

    total = summary.get("total", 0)
    passed = summary.get("passed", 0)
    failed = summary.get("failed", 0)
    errors = summary.get("errors", 0)
    skipped = summary.get("skipped", 0)
    pass_rate = (passed / total * 100) if total else 0

    # Collect failures across all suites
    failures: List[Dict] = []
    for suite_name, suite_data in suites.items():
        for t in suite_data.get("tests", []):
            if t.get("status") in ("FAIL", "ERROR"):
                failures.append({
                    "suite": suite_name,
                    "name": t["name"],
                    "status": t["status"],
                    "latency_ms": t.get("latency_ms", 0),
                    "error": t.get("error", ""),
                })

    # Format timestamp for display
    try:
        dt = datetime.fromisoformat(timestamp)
        display_date = dt.strftime("%B %d, %Y %H:%M")
    except (ValueError, TypeError):
        display_date = str(timestamp)

    # Format duration
    if duration_s >= 60:
        duration_str = f"{duration_s / 60:.1f}min"
    else:
        duration_str = f"{duration_s:.1f}s"

    suite_count = len(suites)
    tenant_display = _esc(tenant) if tenant else "local"

    # Ring chart geometry
    radius = 45
    circumference = 2 * math.pi * radius
    dash_offset = circumference * (1 - pass_rate / 100)
    ring_color = _rate_color(pass_rate)

    parts = [_CSS, _header(tenant_display, display_date, suite_count, total, duration_str,
                           pass_rate, circumference, dash_offset, ring_color)]

    # Stat cards
    parts.append(_stat_cards(total, passed, failed, errors, skipped))

    # Per-suite table
    parts.append(_suite_table(suites))

    # Failure details
    if failures:
        parts.append(_failure_section(failures))

    # Endpoint latency table
    if endpoint_latency:
        parts.append(_latency_section(endpoint_latency))

    # Footer
    parts.append(_footer(tenant_display, display_date))
    parts.append("</body>\n</html>")

    content = "\n".join(parts)
    with open(output_path, "w") as f:
        f.write(content)


# ── Helpers ────────────────────────────────────────────────────────────────

def _esc(text: Any) -> str:
    return html.escape(str(text)) if text else ""


def _rate_color(rate: float) -> str:
    if rate >= 95:
        return "#3fb950"
    if rate >= 80:
        return "#d29922"
    return "#f85149"


def _rate_css_class(rate: float) -> str:
    if rate >= 95:
        return "green"
    if rate >= 80:
        return "yellow"
    return "red"


def _cnt_class(count: int, kind: str) -> str:
    if count == 0:
        return "cnt-zero"
    return {"pass": "cnt-pass", "fail": "cnt-fail", "error": "cnt-err", "skip": "cnt-skip"}.get(kind, "")


# ── Section Builders ───────────────────────────────────────────────────────

def _header(tenant, date, suite_count, total, duration, pass_rate, circ, offset, color):
    return f"""\
<div class="header">
  <div class="header-left">
    <h1>Atlas Metastore Test Harness</h1>
    <div class="subtitle">Tenant: <span>{tenant}</span> &middot; {date} &middot; {suite_count} suites &middot; {total} tests &middot; {duration}</div>
  </div>
  <div class="header-right">
    <div class="ring-container">
      <svg width="110" height="110" viewBox="0 0 110 110">
        <circle class="ring-bg" cx="55" cy="55" r="45"/>
        <circle class="ring-fill" cx="55" cy="55" r="45"
                style="stroke:{color}"
                stroke-dasharray="{circ:.1f}"
                stroke-dashoffset="{offset:.1f}"/>
      </svg>
      <div class="ring-label">
        <div class="pct" style="color:{color}">{pass_rate:.1f}%</div>
        <div class="lbl">Pass Rate</div>
      </div>
    </div>
  </div>
</div>"""


def _stat_cards(total, passed, failed, errors, skipped):
    return f"""\
<div class="stats">
  <div class="stat-card total"><div class="num">{total}</div><div class="lbl">Total Tests</div></div>
  <div class="stat-card pass"><div class="num">{passed}</div><div class="lbl">Passed</div></div>
  <div class="stat-card fail"><div class="num">{failed}</div><div class="lbl">Failed</div></div>
  <div class="stat-card error"><div class="num">{errors}</div><div class="lbl">Errors</div></div>
  <div class="stat-card skip"><div class="num">{skipped}</div><div class="lbl">Skipped</div></div>
</div>"""


def _suite_table(suites: Dict) -> str:
    rows = []
    for idx, (name, data) in enumerate(sorted(suites.items()), 1):
        tests = data.get("tests", [])
        s_total = len(tests)
        s_pass = sum(1 for t in tests if t.get("status") == "PASS")
        s_fail = sum(1 for t in tests if t.get("status") == "FAIL")
        s_err = sum(1 for t in tests if t.get("status") == "ERROR")
        s_skip = sum(1 for t in tests if t.get("status") == "SKIP")
        rate = (s_pass / s_total * 100) if s_total else 0
        css = _rate_css_class(rate)
        perfect = ' class="perfect"' if rate == 100 else ""
        rows.append(
            f'  <tr{perfect}>'
            f"<td>{idx}</td>"
            f'<td class="suite-name">{_esc(name)}</td>'
            f"<td>{s_total}</td>"
            f'<td class="{_cnt_class(s_pass, "pass")}">{s_pass}</td>'
            f'<td class="{_cnt_class(s_fail, "fail")}">{s_fail}</td>'
            f'<td class="{_cnt_class(s_err, "error")}">{s_err}</td>'
            f'<td class="{_cnt_class(s_skip, "skip")}">{s_skip}</td>'
            f'<td class="bar-cell"><div class="bar-bg"><div class="bar-fill {css}" style="width:{rate:.0f}%"></div></div></td>'
            f'<td><span class="pct-badge {css}">{rate:.0f}%</span></td>'
            f"</tr>"
        )
    return f"""\
<div class="section-title"><span class="icon">&#9776;</span> Per-Suite Results</div>
<div class="suite-table-wrapper">
<table>
<thead>
  <tr>
    <th>#</th><th>Suite</th><th>Total</th><th>Pass</th><th>Fail</th><th>Error</th><th>Skip</th><th style="width:120px">Coverage</th><th>Rate</th>
  </tr>
</thead>
<tbody>
{chr(10).join(rows)}
</tbody>
</table>
</div>"""


def _failure_section(failures: List[Dict]) -> str:
    cards = []
    for f in failures:
        error_text = _esc(f["error"][:500]) if f["error"] else "No error message"
        status_class = "bug" if f["status"] == "FAIL" else "known"
        cards.append(
            f'  <div class="issue-card">'
            f'<div class="issue-title">{_esc(f["suite"])}::{_esc(f["name"])}</div>'
            f'<div class="issue-desc">{error_text}</div>'
            f'<span class="issue-tag {status_class}">{f["status"]}</span>'
            f'<span class="issue-tag latency">{f["latency_ms"]:.0f}ms</span>'
            f"</div>"
        )
    return f"""\
<div class="section-title"><span class="icon">&#9888;</span> Failures &amp; Errors ({len(failures)})</div>
<div class="issues">
{chr(10).join(cards)}
</div>"""


def _latency_section(endpoint_latency: Dict) -> str:
    # Sort by p95 descending, take top 10
    sorted_eps = sorted(endpoint_latency.items(), key=lambda x: x[1].get("p95_ms", 0), reverse=True)[:10]
    rows = []
    for idx, (ep, stats) in enumerate(sorted_eps, 1):
        rows.append(
            f"  <tr>"
            f"<td>{idx}</td>"
            f'<td class="suite-name">{_esc(ep)}</td>'
            f"<td>{stats.get('count', 0)}</td>"
            f"<td>{stats.get('p50_ms', 0):.0f}</td>"
            f"<td>{stats.get('p95_ms', 0):.0f}</td>"
            f"<td>{stats.get('p99_ms', 0):.0f}</td>"
            f"<td>{stats.get('max_ms', 0):.0f}</td>"
            f"</tr>"
        )
    return f"""\
<div class="section-title"><span class="icon">&#9201;</span> Top 10 Slowest Endpoints (by p95)</div>
<div class="suite-table-wrapper">
<table>
<thead>
  <tr>
    <th>#</th><th>Endpoint</th><th>Calls</th><th>p50 (ms)</th><th>p95 (ms)</th><th>p99 (ms)</th><th>Max (ms)</th>
  </tr>
</thead>
<tbody>
{chr(10).join(rows)}
</tbody>
</table>
</div>"""


def _footer(tenant, date):
    return f"""\
<div class="footer">
  Atlas Metastore REST API Test Harness &middot; Generated {date} &middot; Tenant: {tenant}
</div>"""


# ── Full CSS + HTML skeleton ──────────────────────────────────────────────

_CSS = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlas Metastore — Test Harness Report</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    background: #0f1117;
    color: #e1e4e8;
    padding: 32px 40px;
    min-width: 900px;
  }

  /* Header */
  .header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 28px;
  }
  .header-left h1 {
    font-size: 26px;
    font-weight: 700;
    color: #fff;
    letter-spacing: -0.3px;
  }
  .header-left .subtitle {
    font-size: 14px;
    color: #8b949e;
    margin-top: 4px;
  }
  .header-left .subtitle span {
    color: #58a6ff;
    font-weight: 600;
  }
  .header-right {
    display: flex;
    align-items: center;
    gap: 32px;
  }

  /* Ring Chart */
  .ring-container {
    position: relative;
    width: 110px;
    height: 110px;
  }
  .ring-container svg { transform: rotate(-90deg); }
  .ring-bg { fill: none; stroke: #21262d; stroke-width: 10; }
  .ring-fill { fill: none; stroke-width: 10; stroke-linecap: round; transition: stroke-dashoffset .6s ease; }
  .ring-label {
    position: absolute;
    top: 50%; left: 50%;
    transform: translate(-50%, -50%);
    text-align: center;
  }
  .ring-label .pct { font-size: 26px; font-weight: 800; }
  .ring-label .lbl { font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }

  /* Stat Cards */
  .stats { display: flex; gap: 14px; margin-bottom: 24px; }
  .stat-card {
    flex: 1;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 18px 20px;
    text-align: center;
  }
  .stat-card .num { font-size: 32px; font-weight: 800; }
  .stat-card .lbl { font-size: 12px; color: #8b949e; margin-top: 2px; text-transform: uppercase; letter-spacing: .6px; }
  .stat-card.pass .num { color: #3fb950; }
  .stat-card.fail .num { color: #f85149; }
  .stat-card.error .num { color: #d29922; }
  .stat-card.skip .num { color: #8b949e; }
  .stat-card.total .num { color: #58a6ff; }

  /* Section Headers */
  .section-title {
    font-size: 16px;
    font-weight: 700;
    color: #fff;
    margin-bottom: 14px;
    margin-top: 8px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .section-title .icon { font-size: 18px; }

  /* Failure Cards */
  .issues {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
    margin-bottom: 28px;
  }
  .issue-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-left: 3px solid #f85149;
    border-radius: 8px;
    padding: 14px 18px;
  }
  .issue-card .issue-title {
    font-size: 13px;
    font-weight: 700;
    color: #f0f3f6;
    margin-bottom: 4px;
  }
  .issue-card .issue-desc {
    font-size: 12px;
    color: #8b949e;
    line-height: 1.45;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 120px;
    overflow-y: auto;
  }
  .issue-card .issue-tag {
    display: inline-block;
    font-size: 10px;
    font-weight: 600;
    padding: 2px 8px;
    border-radius: 10px;
    margin-top: 8px;
    margin-right: 6px;
  }
  .issue-tag.bug { background: #f8514920; color: #f85149; }
  .issue-tag.known { background: #d2992220; color: #d29922; }
  .issue-tag.latency { background: #388bfd20; color: #58a6ff; }

  /* Suite Table */
  .suite-table-wrapper {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: 24px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  thead th {
    background: #1c2128;
    padding: 10px 14px;
    text-align: left;
    font-weight: 600;
    color: #8b949e;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .5px;
    border-bottom: 1px solid #30363d;
  }
  thead th:nth-child(n+3) { text-align: center; }
  tbody td {
    padding: 9px 14px;
    border-bottom: 1px solid #21262d;
    color: #c9d1d9;
  }
  tbody td:nth-child(n+3) { text-align: center; }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: #1c212833; }
  .suite-name { font-weight: 600; color: #e1e4e8; }
  .bar-cell { width: 120px; }
  .bar-bg {
    width: 100%;
    height: 6px;
    background: #21262d;
    border-radius: 3px;
    overflow: hidden;
  }
  .bar-fill {
    height: 100%;
    border-radius: 3px;
    transition: width .4s ease;
  }
  .bar-fill.green { background: #3fb950; }
  .bar-fill.yellow { background: #d29922; }
  .bar-fill.red { background: #f85149; }
  .pct-badge {
    display: inline-block;
    font-size: 12px;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 6px;
    min-width: 48px;
  }
  .pct-badge.green { background: #23882620; color: #3fb950; }
  .pct-badge.yellow { background: #d2992220; color: #d29922; }
  .pct-badge.red { background: #f8514920; color: #f85149; }
  .cnt-pass { color: #3fb950; font-weight: 600; }
  .cnt-fail { color: #f85149; font-weight: 600; }
  .cnt-err { color: #d29922; font-weight: 600; }
  .cnt-skip { color: #8b949e; }
  .cnt-zero { color: #30363d; }

  /* Footer */
  .footer {
    text-align: center;
    font-size: 11px;
    color: #484f58;
    padding: 12px 0 0;
  }

  /* 100% row highlight */
  tr.perfect td { background: #23882608; }
</style>
</head>
<body>
"""
