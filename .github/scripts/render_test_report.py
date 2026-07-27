#!/usr/bin/env python3
"""
Render a pytest JSON report into a PNG "evidence card" plus a GitHub Job
Summary, for CI reporting.

Usage:
    python render_test_report.py report.json out.png \
        --pr 12 --sha abc1234 --branch fix/issue-46 --run-id 123

Reads the JSON produced by `pytest --json-report` (pytest-json-report plugin),
builds a self-contained HTML card, screenshots it with Playwright, and writes
a markdown summary to $GITHUB_STEP_SUMMARY when running in Actions.

Note on GitHub Job Summary: GitHub renders markdown, and sanitizes most inline
CSS/HTML styling. The summary therefore uses markdown tables + emoji rather
than the styled HTML card - the PNG artifact carries the full visual fidelity.
"""

import argparse
import html
import json
import os
import pathlib
import sys
from collections import defaultdict

# Palette (validated, light surface)
SURFACE = "#fcfcfb"
PLANE = "#f9f9f7"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
RULE = "#e1e0d9"
GOOD = "#0ca30c"
CRITICAL = "#d03b3b"
WARNING = "#fab219"
ACCENT = "#2a78d6"


def load_report(path):
    with open(path) as f:
        return json.load(f)


def summarize(report):
    summary = report.get("summary", {})
    return {
        "passed": summary.get("passed", 0),
        "failed": summary.get("failed", 0),
        "error": summary.get("error", 0),
        "skipped": summary.get("skipped", 0),
        "xfailed": summary.get("xfailed", 0),
        "total": summary.get("total", summary.get("collected", 0)),
        "duration": report.get("duration", 0.0),
    }


def group_by_file(report):
    """Return {file: {'passed': n, 'failed': n, 'skipped': n}} preserving order."""
    groups = defaultdict(lambda: {"passed": 0, "failed": 0, "skipped": 0, "error": 0})
    order = []
    for test in report.get("tests", []):
        nodeid = test.get("nodeid", "")
        fname = nodeid.split("::")[0] if "::" in nodeid else nodeid
        if fname not in groups:
            order.append(fname)
        outcome = test.get("outcome", "")
        if outcome in groups[fname]:
            groups[fname][outcome] += 1
        elif outcome in ("xfailed", "xpassed"):
            groups[fname]["skipped"] += 1
    return [(f, groups[f]) for f in order]


def failed_tests(report, limit=8):
    out = []
    for test in report.get("tests", []):
        if test.get("outcome") in ("failed", "error"):
            nodeid = test.get("nodeid", "")
            call = test.get("call") or test.get("setup") or {}
            crash = (call or {}).get("crash") or {}
            msg = crash.get("message", "") or ""
            out.append((nodeid, msg.split("\n")[0][:160]))
        if len(out) >= limit:
            break
    return out


def build_html(s, groups, failures, meta):
    ok = (s["failed"] + s["error"]) == 0
    status_text = "ALL TESTS PASSED" if ok else "TESTS FAILED"
    status_color = GOOD if ok else CRITICAL

    def esc(x):
        return html.escape(str(x))

    rows = []
    for fname, counts in groups:
        total = sum(counts.values())
        bad = counts["failed"] + counts["error"]
        dot = GOOD if bad == 0 else CRITICAL
        detail = f"{counts['passed']} passed"
        if bad:
            detail += f" · {bad} failed"
        if counts["skipped"]:
            detail += f" · {counts['skipped']} skipped"
        rows.append(
            f'<tr><td><span class="dot" style="background:{dot}"></span>'
            f'<span class="fname">{esc(fname)}</span></td>'
            f'<td class="num">{total}</td>'
            f'<td class="detail">{esc(detail)}</td></tr>'
        )
    rows_html = "\n".join(rows) if rows else '<tr><td colspan="3" class="detail">No tests collected</td></tr>'

    fail_html = ""
    if failures:
        items = "\n".join(
            f'<li><code>{esc(n)}</code><span class="msg">{esc(m)}</span></li>'
            for n, m in failures
        )
        fail_html = f"""
        <div class="failures">
          <p class="sectitle">Failures</p>
          <ul>{items}</ul>
        </div>"""

    skipped_tile = ""
    if s["skipped"]:
        skipped_tile = f"""
        <div class="tile"><p class="tv" style="color:{MUTED}">{s['skipped']}</p><p class="tl">Skipped</p></div>"""

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
  * {{ box-sizing: border-box; margin:0; padding:0; }}
  body {{
    width: 900px; background: {PLANE};
    font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
    color: {INK}; -webkit-font-smoothing: antialiased;
  }}
  .card {{ background: {SURFACE}; margin: 20px; border: 1px solid {RULE}; border-radius: 12px; overflow: hidden; }}
  .band {{ height: 5px; background: {status_color}; }}
  .head {{ padding: 22px 26px 18px; border-bottom: 1px solid {RULE}; }}
  .eyebrow {{ font-size: 11.5px; letter-spacing: .09em; text-transform: uppercase;
              color: {MUTED}; font-weight: 650; margin-bottom: 8px; }}
  h1 {{ font-size: 22px; font-weight: 660; letter-spacing: -.01em; color: {status_color}; }}
  .meta {{ margin-top: 10px; font-size: 13px; color: {INK2}; }}
  .meta code {{ background: {PLANE}; padding: 1px 6px; border-radius: 4px;
                font-size: 12.5px; border: 1px solid {RULE}; }}
  .tiles {{ display: flex; border-bottom: 1px solid {RULE}; }}
  .tile {{ flex: 1; padding: 16px 26px; border-right: 1px solid {RULE}; }}
  .tile:last-child {{ border-right: 0; }}
  .tv {{ font-size: 27px; font-weight: 660; letter-spacing: -.02em; line-height: 1.1; }}
  .tl {{ font-size: 12px; color: {MUTED}; margin-top: 3px;
         letter-spacing: .04em; text-transform: uppercase; font-weight: 600; }}
  table {{ width: 100%; border-collapse: collapse; }}
  td {{ padding: 9px 26px; font-size: 13.5px; border-bottom: 1px solid {RULE}; vertical-align: middle; }}
  tr:last-child td {{ border-bottom: 0; }}
  .dot {{ display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:10px; vertical-align: middle; }}
  .fname {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12.5px; }}
  .num {{ text-align: right; width: 60px; font-variant-numeric: tabular-nums; color: {INK2}; }}
  .detail {{ color: {MUTED}; font-size: 12.5px; width: 300px; }}
  .sectitle {{ font-size: 11.5px; letter-spacing: .08em; text-transform: uppercase;
               color: {CRITICAL}; font-weight: 650; margin-bottom: 8px; }}
  .failures {{ padding: 16px 26px 18px; border-top: 1px solid {RULE}; background: {PLANE}; }}
  .failures li {{ list-style: none; margin-bottom: 7px; font-size: 12.5px; }}
  .failures code {{ font-family: ui-monospace, Menlo, monospace; font-size: 12px; color: {INK}; }}
  .msg {{ display:block; color: {INK2}; margin-top: 2px; padding-left: 2px; }}
  .foot {{ padding: 12px 26px; font-size: 11.5px; color: {MUTED}; border-top: 1px solid {RULE}; }}
</style></head><body>
<div class="card">
  <div class="band"></div>
  <div class="head">
    <p class="eyebrow">Automated verification · pytest</p>
    <h1>{status_text}</h1>
    <p class="meta">PR <code>#{esc(meta['pr'])}</code> · branch <code>{esc(meta['branch'])}</code> · commit <code>{esc(meta['sha'][:7])}</code></p>
  </div>
  <div class="tiles">
    <div class="tile"><p class="tv" style="color:{GOOD}">{s['passed']}</p><p class="tl">Passed</p></div>
    <div class="tile"><p class="tv" style="color:{CRITICAL if (s['failed']+s['error']) else MUTED}">{s['failed'] + s['error']}</p><p class="tl">Failed</p></div>
    {skipped_tile}
    <div class="tile"><p class="tv">{s['total']}</p><p class="tl">Total</p></div>
    <div class="tile"><p class="tv">{s['duration']:.1f}s</p><p class="tl">Duration</p></div>
  </div>
  <table>{rows_html}</table>
  {fail_html}
  <div class="foot">{esc(meta['timestamp'])} · workflow run {esc(meta['run_id'])}</div>
</div>
</body></html>"""


def build_job_summary(s, groups, failures, meta):
    """
    Markdown for $GITHUB_STEP_SUMMARY. GitHub sanitizes inline CSS, so this
    uses markdown tables + emoji rather than the styled HTML card. The PNG
    artifact carries the full visual.
    """
    ok = (s["failed"] + s["error"]) == 0
    status = "✅ **All tests passed**" if ok else "❌ **Tests failed**"

    lines = [
        "## Test Results",
        "",
        status,
        "",
        f"PR `#{meta['pr']}` · branch `{meta['branch']}` · commit `{meta['sha'][:7]}`",
        "",
        "| Passed | Failed | Skipped | Total | Duration |",
        "|-------:|-------:|--------:|------:|---------:|",
        f"| {s['passed']} | {s['failed'] + s['error']} | {s['skipped']} | "
        f"{s['total']} | {s['duration']:.1f}s |",
        "",
        "### By file",
        "",
        "| | File | Tests | Detail |",
        "|---|------|------:|--------|",
    ]

    for fname, counts in groups:
        total = sum(counts.values())
        bad = counts["failed"] + counts["error"]
        icon = "🟢" if bad == 0 else "🔴"
        detail = f"{counts['passed']} passed"
        if bad:
            detail += f" · {bad} failed"
        if counts["skipped"]:
            detail += f" · {counts['skipped']} skipped"
        lines.append(f"| {icon} | `{fname}` | {total} | {detail} |")

    if failures:
        lines += ["", "### Failures", ""]
        for nodeid, msg in failures:
            lines.append(f"- **`{nodeid}`**")
            if msg:
                lines.append(f"  <br>`{msg}`")

    lines += [
        "",
        "---",
        "",
        "_The full visual report card is attached to this run as the "
        "`test-report-card` artifact._",
    ]

    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("report")
    ap.add_argument("out")
    ap.add_argument("--pr", default="?")
    ap.add_argument("--sha", default="0000000")
    ap.add_argument("--branch", default="unknown")
    ap.add_argument("--run-id", default="local")
    ap.add_argument("--timestamp", default="")
    ap.add_argument("--html-only", action="store_true")
    args = ap.parse_args()

    report = load_report(args.report)
    s = summarize(report)
    groups = group_by_file(report)
    failures = failed_tests(report)

    meta = {
        "pr": args.pr,
        "sha": args.sha,
        "branch": args.branch,
        "run_id": args.run_id,
        "timestamp": args.timestamp or "",
    }

    doc = build_html(s, groups, failures, meta)
    html_path = pathlib.Path(args.out).with_suffix(".html")
    html_path.write_text(doc)

    # Job Summary - rendered on the Actions run page, no hosting needed.
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(build_job_summary(s, groups, failures, meta))
            f.write("\n")

    # Emit counts for the workflow to reuse in the PR comment.
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"passed={s['passed']}\n")
            f.write(f"failed={s['failed'] + s['error']}\n")
            f.write(f"skipped={s['skipped']}\n")
            f.write(f"total={s['total']}\n")
            f.write(f"duration={s['duration']:.1f}\n")
            f.write(f"ok={'true' if (s['failed'] + s['error']) == 0 else 'false'}\n")

    if args.html_only:
        print(f"wrote {html_path}")
        return

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 900, "height": 600},
                                device_scale_factor=2)
        page.goto(html_path.resolve().as_uri())
        page.wait_for_timeout(250)
        card = page.query_selector(".card")
        card.screenshot(path=args.out)
        browser.close()

    print(f"wrote {args.out}")


if __name__ == "__main__":
    sys.exit(main())