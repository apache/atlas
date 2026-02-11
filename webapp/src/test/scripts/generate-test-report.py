#!/usr/bin/env python3
"""
Generate test reports from Maven Surefire XML results.

Usage:
    python3 generate-test-report.py <surefire-reports-dir> <output-html> [--markdown <output-md>]

Example:
    python3 generate-test-report.py webapp/target/surefire-reports webapp/target/site/test-report.html
    python3 generate-test-report.py webapp/target/surefire-reports webapp/target/site/test-report.html --markdown webapp/target/site/test-summary.md
"""

import xml.etree.ElementTree as ET
import glob
import html
import os
import sys
from datetime import datetime


def parse_reports(reports_dir):
    xml_files = sorted(glob.glob(os.path.join(reports_dir, "TEST-*.xml")))
    if not xml_files:
        print(f"No TEST-*.xml files found in {reports_dir}", file=sys.stderr)
        sys.exit(1)

    suites = []
    for xf in xml_files:
        tree = ET.parse(xf)
        root = tree.getroot()
        s = {
            "name": root.get("name", ""),
            "tests": int(root.get("tests", 0)),
            "failures": int(root.get("failures", 0)),
            "errors": int(root.get("errors", 0)),
            "skipped": int(root.get("skipped", 0)),
            "time": float(root.get("time", 0)),
            "cases": [],
        }
        s["passed"] = s["tests"] - s["failures"] - s["errors"] - s["skipped"]
        s["short_name"] = s["name"].rsplit(".", 1)[-1]

        for tc in root.findall("testcase"):
            c = {"name": tc.get("name", ""), "time": float(tc.get("time", 0))}
            skip_el = tc.find("skipped")
            fail_el = tc.find("failure")
            err_el = tc.find("error")
            if skip_el is not None:
                c["status"] = "skipped"
                c["message"] = skip_el.get("message", "")
            elif fail_el is not None:
                c["status"] = "failed"
                c["message"] = fail_el.get("message", "")
                c["type"] = fail_el.get("type", "")
                c["detail"] = (fail_el.text or "")[:2000]
            elif err_el is not None:
                c["status"] = "error"
                c["message"] = err_el.get("message", "")
                c["type"] = err_el.get("type", "")
                c["detail"] = (err_el.text or "")[:2000]
            else:
                c["status"] = "passed"
            s["cases"].append(c)
        suites.append(s)
    return suites


def compute_totals(suites):
    t = {"tests": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0, "time": 0.0}
    for s in suites:
        t["tests"] += s["tests"]
        t["passed"] += s["passed"]
        t["failed"] += s["failures"]
        t["errors"] += s["errors"]
        t["skipped"] += s["skipped"]
        t["time"] += s["time"]
    return t


def suite_status(s):
    if s["failures"] > 0 or s["errors"] > 0:
        return "fail"
    if s["skipped"] == s["tests"]:
        return "skip"
    if s["skipped"] > 0:
        return "warn"
    return "pass"


def suite_label(sc, s):
    if sc == "pass":
        return "PASS"
    if sc == "fail":
        return "FAIL"
    if s["skipped"] == s["tests"]:
        return "SKIP"
    return "PARTIAL"


# ---------------------------------------------------------------------------
# Markdown report (for GitHub Step Summary)
# ---------------------------------------------------------------------------
def generate_markdown(suites, totals):
    fail_count = totals["failed"] + totals["errors"]
    if fail_count > 0:
        icon = "&#x274C;"  # red X
    elif totals["skipped"] > 0:
        icon = "&#x26A0;&#xFE0F;"  # warning
    else:
        icon = "&#x2705;"  # green check

    status_emoji = {"pass": "&#x2705;", "fail": "&#x274C;", "skip": "&#x23ED;&#xFE0F;", "warn": "&#x26A0;&#xFE0F;"}
    test_emoji = {"passed": "&#x2705;", "failed": "&#x274C;", "error": "&#x274C;", "skipped": "&#x23ED;&#xFE0F;"}

    md = f"## {icon} Integration Test Results\n\n"
    md += f"**{totals['tests']}** tests | "
    md += f"**{totals['passed']}** passed | "
    md += f"**{fail_count}** failed | "
    md += f"**{totals['skipped']}** skipped | "
    md += f"**{totals['time']:.1f}s** duration\n\n"

    # Suite summary table
    md += "| Status | Test Suite | Tests | Passed | Failed | Skipped | Time |\n"
    md += "|:------:|------------|------:|-------:|-------:|--------:|-----:|\n"
    for s in suites:
        sc = suite_status(s)
        emoji = status_emoji.get(sc, "")
        md += (
            f"| {emoji} | {s['short_name']} | {s['tests']} | {s['passed']} | "
            f"{s['failures'] + s['errors']} | {s['skipped']} | {s['time']:.2f}s |\n"
        )
    md += (
        f"| | **Total** | **{totals['tests']}** | **{totals['passed']}** | "
        f"**{fail_count}** | **{totals['skipped']}** | **{totals['time']:.1f}s** |\n\n"
    )

    # Per-suite details (expandable)
    md += "### Test Details\n\n"
    for s in suites:
        sc = suite_status(s)
        emoji = status_emoji.get(sc, "")
        lbl = suite_label(sc, s)
        md += f"<details>\n<summary>{emoji} <b>{s['short_name']}</b> &mdash; {s['passed']}/{s['tests']} passed ({s['time']:.2f}s)</summary>\n\n"
        md += "| | Test | Time | Status |\n"
        md += "|---|------|-----:|--------|\n"
        for c in s["cases"]:
            te = test_emoji.get(c["status"], "")
            md += f"| {te} | `{c['name']}` | {c['time']:.3f}s | {c['status']} |\n"
            if c["status"] in ("failed", "error") and c.get("detail"):
                detail = c.get("detail", "").strip()
                if len(detail) > 500:
                    detail = detail[:500] + "..."
                md += f"\n**{c.get('type', 'Error')}**: {c.get('message', '')}\n"
                md += f"```\n{detail}\n```\n\n"
        md += "\n</details>\n\n"

    return md


# ---------------------------------------------------------------------------
# HTML report (self-contained, for artifact download)
# ---------------------------------------------------------------------------
def generate_html(suites, totals):
    def esc(t):
        return html.escape(str(t))

    def css_class(s):
        return {"passed": "pass", "failed": "fail", "error": "fail", "skipped": "skip"}.get(s, "")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pct_pass = totals["passed"] / totals["tests"] * 100 if totals["tests"] else 0
    pct_fail = (totals["failed"] + totals["errors"]) / totals["tests"] * 100 if totals["tests"] else 0
    pct_skip = totals["skipped"] / totals["tests"] * 100 if totals["tests"] else 0

    h = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Integration Test Report</title>
<style>
  :root {{
    --pass: #22c55e; --pass-bg: #f0fdf4; --pass-border: #bbf7d0;
    --fail: #ef4444; --fail-bg: #fef2f2; --fail-border: #fecaca;
    --skip: #f59e0b; --skip-bg: #fffbeb; --skip-border: #fde68a;
    --bg: #f8fafc; --card: #ffffff; --border: #e2e8f0;
    --text: #1e293b; --muted: #64748b; --heading: #0f172a;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: var(--bg); color: var(--text); line-height: 1.5; padding: 24px; }}
  .container {{ max-width: 1200px; margin: 0 auto; }}
  h1 {{ color: var(--heading); font-size: 24px; margin-bottom: 4px; }}
  h2 {{ margin: 32px 0 16px; font-size: 18px; color: var(--heading); }}
  .subtitle {{ color: var(--muted); font-size: 14px; margin-bottom: 24px; }}
  .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
              gap: 12px; margin-bottom: 32px; }}
  .card {{ background: var(--card); border: 1px solid var(--border);
           border-radius: 8px; padding: 16px; text-align: center; }}
  .card .value {{ font-size: 32px; font-weight: 700; }}
  .card .label {{ font-size: 12px; color: var(--muted);
                  text-transform: uppercase; letter-spacing: 0.05em; }}
  .card.total .value {{ color: var(--heading); }}
  .card.passed .value {{ color: var(--pass); }}
  .card.failed .value {{ color: var(--fail); }}
  .card.skipped .value {{ color: var(--skip); }}
  .card.time .value {{ color: var(--muted); font-size: 24px; }}
  .progress-bar {{ display: flex; height: 8px; border-radius: 4px;
                   overflow: hidden; margin-bottom: 32px; background: var(--border); }}
  .progress-bar .seg-pass {{ background: var(--pass); }}
  .progress-bar .seg-fail {{ background: var(--fail); }}
  .progress-bar .seg-skip {{ background: var(--skip); }}
  table {{ width: 100%; border-collapse: collapse; background: var(--card);
           border: 1px solid var(--border); border-radius: 8px;
           overflow: hidden; margin-bottom: 16px; }}
  th {{ background: #f1f5f9; text-align: left; padding: 10px 14px; font-size: 13px;
       font-weight: 600; color: var(--muted); text-transform: uppercase;
       letter-spacing: 0.04em; border-bottom: 1px solid var(--border); }}
  td {{ padding: 10px 14px; border-bottom: 1px solid var(--border); font-size: 14px; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover {{ background: #f8fafc; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 9999px;
            font-size: 12px; font-weight: 600; }}
  .badge.pass {{ background: var(--pass-bg); color: #166534; border: 1px solid var(--pass-border); }}
  .badge.fail {{ background: var(--fail-bg); color: #991b1b; border: 1px solid var(--fail-border); }}
  .badge.skip {{ background: var(--skip-bg); color: #92400e; border: 1px solid var(--skip-border); }}
  .badge.warn {{ background: var(--skip-bg); color: #92400e; border: 1px solid var(--skip-border); }}
  details {{ margin-bottom: 8px; }}
  details summary {{ cursor: pointer; padding: 8px 0; font-weight: 500; }}
  details summary:hover {{ color: #2563eb; }}
  .detail-table {{ margin: 8px 0 16px 0; }}
  .detail-table td {{ padding: 6px 14px; font-size: 13px; }}
  .detail-table .test-name {{ font-family: 'SF Mono', Menlo, monospace; font-size: 13px; }}
  .dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%;
          margin-right: 6px; vertical-align: middle; }}
  .dot.pass {{ background: var(--pass); }}
  .dot.fail {{ background: var(--fail); }}
  .dot.skip {{ background: var(--skip); }}
  .failure-detail {{ background: var(--fail-bg); border: 1px solid var(--fail-border);
                     border-radius: 6px; padding: 10px 12px; margin: 4px 0;
                     font-family: 'SF Mono', Menlo, monospace; font-size: 12px;
                     white-space: pre-wrap; word-break: break-all;
                     max-height: 200px; overflow-y: auto; color: #991b1b; }}
</style>
</head>
<body>
<div class="container">
<h1>Atlas Integration Test Report</h1>
<p class="subtitle">Generated {esc(now)} &mdash; {len(suites)} test suites, {totals['time']:.1f}s total</p>

<div class="summary">
  <div class="card total"><div class="value">{totals['tests']}</div><div class="label">Total</div></div>
  <div class="card passed"><div class="value">{totals['passed']}</div><div class="label">Passed</div></div>
  <div class="card failed"><div class="value">{totals['failed'] + totals['errors']}</div><div class="label">Failed</div></div>
  <div class="card skipped"><div class="value">{totals['skipped']}</div><div class="label">Skipped</div></div>
  <div class="card time"><div class="value">{totals['time']:.1f}s</div><div class="label">Duration</div></div>
</div>

<div class="progress-bar">
  <div class="seg-pass" style="width:{pct_pass:.1f}%"></div>
  <div class="seg-fail" style="width:{pct_fail:.1f}%"></div>
  <div class="seg-skip" style="width:{pct_skip:.1f}%"></div>
</div>

<table>
<thead><tr>
  <th>Test Suite</th><th class="num">Tests</th><th class="num">Passed</th>
  <th class="num">Failed</th><th class="num">Skipped</th><th class="num">Time</th><th>Status</th>
</tr></thead>
<tbody>
"""

    for s in suites:
        sc = suite_status(s)
        lbl = suite_label(sc, s)
        h += f"""<tr>
  <td><a href="#{esc(s['short_name'])}">{esc(s['short_name'])}</a></td>
  <td class="num">{s['tests']}</td>
  <td class="num">{s['passed']}</td>
  <td class="num">{s['failures'] + s['errors']}</td>
  <td class="num">{s['skipped']}</td>
  <td class="num">{s['time']:.2f}s</td>
  <td><span class="badge {sc}">{lbl}</span></td>
</tr>\n"""

    h += f"""<tr style="font-weight:600;background:#f1f5f9">
  <td>Total</td><td class="num">{totals['tests']}</td><td class="num">{totals['passed']}</td>
  <td class="num">{totals['failed'] + totals['errors']}</td><td class="num">{totals['skipped']}</td>
  <td class="num">{totals['time']:.1f}s</td><td></td>
</tr>
</tbody></table>

<h2>Test Details</h2>
"""

    for s in suites:
        sc = suite_status(s)
        lbl = suite_label(sc, s)
        h += f"""<details id="{esc(s['short_name'])}">
<summary><span class="badge {sc}" style="margin-right:8px">{lbl}</span> \
{esc(s['short_name'])} &mdash; {s['passed']}/{s['tests']} passed ({s['time']:.2f}s)</summary>
<table class="detail-table">
<thead><tr><th style="width:30px"></th><th>Test</th>\
<th class="num" style="width:80px">Time</th><th style="width:80px">Status</th></tr></thead>
<tbody>
"""
        for c in s["cases"]:
            cs = css_class(c["status"])
            clbl = c["status"].upper()
            h += f"""<tr>
  <td><span class="dot {cs}"></span></td>
  <td class="test-name">{esc(c['name'])}</td>
  <td class="num">{c['time']:.3f}s</td>
  <td><span class="badge {cs}">{clbl}</span></td>
</tr>\n"""
            if c["status"] in ("failed", "error") and c.get("detail"):
                h += (
                    f'<tr><td colspan="4"><div class="failure-detail">'
                    f'{esc(c.get("type", ""))}: {esc(c.get("message", ""))}\n'
                    f'{esc(c["detail"])}</div></td></tr>\n'
                )

        h += "</tbody></table></details>\n"

    h += "</div>\n</body>\n</html>"
    return h


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <surefire-reports-dir> <output-html> [--markdown <output-md>]",
              file=sys.stderr)
        sys.exit(1)

    reports_dir = sys.argv[1]
    html_path = sys.argv[2]

    md_path = None
    if "--markdown" in sys.argv:
        idx = sys.argv.index("--markdown")
        if idx + 1 < len(sys.argv):
            md_path = sys.argv[idx + 1]

    suites = parse_reports(reports_dir)
    totals = compute_totals(suites)

    # Generate HTML
    html_content = generate_html(suites, totals)
    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, "w") as f:
        f.write(html_content)
    print(f"HTML report: {html_path}")

    # Generate Markdown
    if md_path:
        md_content = generate_markdown(suites, totals)
        os.makedirs(os.path.dirname(md_path), exist_ok=True)
        with open(md_path, "w") as f:
            f.write(md_content)
        print(f"Markdown report: {md_path}")

    fail_count = totals["failed"] + totals["errors"]
    print(f"Suites: {len(suites)}, Tests: {totals['tests']}, Passed: {totals['passed']}, "
          f"Failed: {fail_count}, Skipped: {totals['skipped']}")


if __name__ == "__main__":
    main()
