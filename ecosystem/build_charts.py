#!/usr/bin/env python3
"""Render the standalone Plotly charts served from GitHub Pages.

Reads dashboard-data/dashboard.json -- the same payload the Insights dashboard
renders -- and writes one self-contained HTML page per chart to docs/charts/.
Pages serves /docs from main, so committing these publishes them.

The charts were previously hand-written with their numbers pasted in, which
meant they drifted from the payload as soon as a crawl landed. Generating them
here keeps the two in step by construction.

Nothing is imported from plotly: the pages call Plotly.newPlot with literal
JSON from the CDN, so the traces and layout are built as plain dicts and
serialised into the template. That keeps the workflow dependency-free.

Colours and type match the dashboard on the catalogue, so a chart opened from
the export icon looks like the section it came from.
"""
import argparse
import datetime
import json
import pathlib

# Mirrors the tokens in the catalogue theme's insights template.
INK = "#101828"
BODY = "#101828"
MUTE = "#667085"
LINE = "#e4e7ec"
FAINT = "#f2f4f7"
PANEL = "#ffffff"
STATUS = {
    "current": "#3A833A",
    "behind": "#fd7e14",
    "eol": "#d9534f",
    "unknown": "#98a2b3",
}
# The dashboard draws chart fills at 80%; rgba keeps that in Plotly.
STATUS_FILL = {
    "current": "rgba(58,131,58,.8)",
    "behind": "rgba(253,126,20,.8)",
    "eol": "rgba(217,83,79,.8)",
    "unknown": "rgba(152,162,179,.8)",
}
STATUS_LABEL = {
    "current": "On latest patch",
    "behind": "Behind on patches",
    "eol": "End of life",
    "unknown": "No version reported",
}
FONT = ("Satoshi, system-ui, -apple-system, 'Segoe UI', "
        "'Helvetica Neue', Helvetica, Arial, sans-serif")

BASE_FONT = {"family": FONT, "size": 13, "color": MUTE}

TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<meta name="description" content="{subtitle}">
<meta name="dc.date" content="{generated}">
<meta name="dc.source" content="CKAN Ecosystem Catalog">
<script src="https://cdn.plot.ly/plotly-4.0.0.min.js" charset="utf-8"></script>
<style>
  html, body {{ height: 100%; }}
  body {{
    margin: 0; padding: 20px 22px 16px;
    background: {panel}; color: {body};
    font-family: {font};
    font-size: 14px; line-height: 1.5;
    display: flex; flex-direction: column;
  }}
  h1 {{ margin: 0; font-size: 1.25rem; font-weight: 500; color: {ink}; }}
  p.sub {{ margin: 6px 0 0; font-size: .875rem; color: {mute}; max-width: 70ch; }}
  #plot {{ flex: 1 1 auto; min-height: 320px; margin-top: 14px; }}
  footer {{
    margin-top: 10px; padding-top: 10px; border-top: 1px solid {line};
    font-size: .75rem; color: {mute};
    display: flex; gap: 14px; flex-wrap: wrap; justify-content: space-between;
  }}
  footer a {{ color: {accent}; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="sub">{subtitle}</p>
<div id="plot"></div>
<footer>
  <span>Crawl of {generated} &middot; CKAN Ecosystem Catalog</span>
  <span><a href="https://ecosystem.ckan.org/insights">Insights dashboard</a>
    &middot; <a href="https://github.com/dathere/pose-ckanext-metadata">Source data</a></span>
</footer>
<script>
Plotly.newPlot("plot", {traces}, {layout}, {config});
</script>
</body>
</html>
"""


def config(filename):
    """Modebar on, trimmed to the export button.

    The previous pages set displayModeBar:false, which removed the only way to
    get an image out of them -- the point of publishing them separately.
    """
    return {
        "displayModeBar": True,
        "displaylogo": False,
        "responsive": True,
        "modeBarButtonsToRemove": [
            "zoom2d", "pan2d", "select2d", "lasso2d", "zoomIn2d", "zoomOut2d",
            "autoScale2d", "resetScale2d", "hoverClosestCartesian",
            "hoverCompareCartesian", "toggleSpikelines",
        ],
        "toImageButtonOptions": {
            "format": "png",
            "filename": filename,
            "scale": 2,
        },
    }


def layout(**over):
    base = {
        "font": BASE_FONT,
        "paper_bgcolor": PANEL,
        "plot_bgcolor": PANEL,
        "margin": {"l": 8, "r": 8, "t": 10, "b": 8},
        "hoverlabel": {
            "bgcolor": PANEL, "bordercolor": LINE,
            "font": {"family": FONT, "size": 13, "color": INK},
            "align": "left",
        },
        "showlegend": False,
    }
    base.update(over)
    return base


AXIS = {
    "showgrid": True, "gridcolor": FAINT, "zeroline": False,
    "linecolor": LINE, "tickfont": {"family": FONT, "size": 12, "color": MUTE},
    "automargin": True,
}


# A crawl reporting fewer versions than this is a crawler failure, not a
# measurement. Matches SHARE_MIN in the dashboard template.
SHARE_MIN = 50


def _reporting(row):
    return row.get("current", 0) + row.get("behind", 0) + row.get("eol", 0)


def _usable(row):
    return row.get("q") == "ok" and not row.get("sus") and _reporting(row) >= SHARE_MIN


def eol_share_weeks(rows):
    """One point per ISO week from its last usable crawl, with gaps marked.

    Mirrors the dashboard's "Past end of life" chart so the embed and the
    page agree to the decimal. Returns (weeks, gaps): weeks are dicts with
    w, eol, n and v (the share, 0-100); gaps are (before, after) index pairs
    into weeks where unusable crawls sat between them.
    """
    weeks, first_ix, last_ix = [], [], []
    for i, row in enumerate(rows):
        if not _usable(row):
            continue
        day = datetime.date.fromisoformat(row["w"])
        wk = day - datetime.timedelta(days=day.weekday())
        n = _reporting(row)
        point = {"w": row["w"], "wk": wk, "eol": row.get("eol", 0), "n": n,
                 "v": round(100 * row.get("eol", 0) / n, 1)}
        if weeks and weeks[-1]["wk"] == wk:
            weeks[-1] = point
            last_ix[-1] = i
        else:
            weeks.append(point)
            first_ix.append(i)
            last_ix.append(i)
    gaps = [(k - 1, k) for k in range(1, len(weeks))
            if any(not _usable(r) for r in rows[last_ix[k - 1] + 1:first_ix[k]])]
    return weeks, gaps


def chart_support_status(d):
    """Past end of life: the share of portals reporting a version on an EOL branch.

    Replaces a donut of the latest crawl's four counts, which restated the
    headline and the last week-by-week column. Raw counts move with how many
    portals answered; a share of those that reported is comparable crawl to
    crawl, and among them the three statuses sum to 100%, so one line carries
    the whole split.
    """
    weeks, gaps = eol_share_weeks(d["timeline"])
    gap_after = {b for _, b in gaps}
    # A None between two points is how Plotly breaks a line.
    x, y, custom = [], [], []
    for k, p in enumerate(weeks):
        if k in gap_after:
            x.append(None)
            y.append(None)
            custom.append([None, None])
        x.append(p["w"])
        y.append(p["v"])
        custom.append([p["eol"], p["n"]])
    traces = [{
        "type": "scatter", "mode": "lines", "connectgaps": False,
        "x": x, "y": y, "customdata": custom,
        "line": {"color": STATUS["eol"], "width": 2, "shape": "linear"},
        "hovertemplate": ("%{x}<br><b>%{y:.1f}%</b> past end of life"
                          "<br>%{customdata[0]} of %{customdata[1]} reporting a version"
                          "<extra></extra>"),
    }]
    shapes, annotations = [], []
    if weeks:
        vals = [p["v"] for p in weeks]
        lo = max(0, (int(min(vals)) // 10) * 10 - 10)
        hi = min(100, -(-int(max(vals)) // 10) * 10 + 10)
        for p, pos in ((weeks[0], "top right"), (weeks[-1], "middle right")):
            traces.append({
                "type": "scatter", "mode": "markers+text", "x": [p["w"]], "y": [p["v"]],
                "marker": {"color": STATUS["eol"], "size": 9,
                           "line": {"color": PANEL, "width": 2}},
                "text": [f"{p['v']:.1f}%"], "textposition": pos,
                "textfont": {"family": FONT, "size": 12, "color": INK},
                "hoverinfo": "skip",
            })
        for a, b in gaps:
            shapes.append({"type": "rect", "xref": "x", "yref": "paper",
                           "x0": weeks[a]["w"], "x1": weeks[b]["w"], "y0": 0, "y1": 1,
                           "fillcolor": "#f7f8fa", "line": {"width": 0}, "layer": "below"})
        start, end = weeks[0]["wk"].isoformat(), weeks[-1]["w"]
        by_day = {}
        for r in d.get("releases", []):
            if start <= r["d"] <= end:
                by_day.setdefault(r["d"], []).append(r["v"])
        for day, vs in sorted(by_day.items()):
            vs.sort(key=lambda v: [int(n) for n in v.split(".")], reverse=True)
            at = max(day, weeks[0]["w"])
            shapes.append({"type": "line", "xref": "x", "yref": "paper",
                           "x0": at, "x1": at, "y0": 0, "y1": 1,
                           "line": {"color": STATUS["unknown"], "width": 1, "dash": "dot"}})
            annotations.append({"x": at, "y": 1.02, "xref": "x", "yref": "paper",
                                "yanchor": "bottom", "showarrow": False,
                                "text": f"CKAN {vs[0]}" + (f" +{len(vs) - 1}" if len(vs) > 1 else ""),
                                "font": {"family": FONT, "size": 11, "color": MUTE}})
    else:
        lo, hi = 0, 100
    lay = layout(
        xaxis=dict(AXIS, showgrid=False, type="date"),
        yaxis=dict(AXIS, range=[lo, hi], dtick=10, ticksuffix="%"),
        shapes=shapes, annotations=annotations,
        margin={"l": 8, "r": 48, "t": 34, "b": 8},
    )
    return ("chart-support-status.html",
            "Past end of life",
            "Share of the portals that reported a version whose CKAN branch no "
            "longer receives patches, one point per week. Portals that did not "
            "answer are left out, so a smaller crawl does not read as movement.",
            traces, lay, "ckan-eol-share")


def chart_support_timeline(d):
    """Week by week -- the section that had no standalone chart at all."""
    rows = d["timeline"]
    weeks = [r["w"] for r in rows]
    traces = []
    for key in ["current", "behind", "eol", "unknown"]:
        traces.append({
            "type": "bar", "name": STATUS_LABEL[key],
            "x": weeks, "y": [r.get(key, 0) for r in rows],
            "marker": {"color": STATUS_FILL[key]},
            "hovertemplate": "%{x}<br>%{y} portals — " + STATUS_LABEL[key] + "<extra></extra>",
        })
    lay = layout(
        barmode="stack",
        showlegend=True,
        legend={"orientation": "h", "y": 1.08, "x": 0,
                "font": {"family": FONT, "size": 12, "color": MUTE}},
        xaxis=dict(AXIS, showgrid=False, type="category"),
        yaxis=dict(AXIS, title={"text": "portals",
                                "font": {"family": FONT, "size": 12, "color": MUTE}}),
        margin={"l": 8, "r": 8, "t": 34, "b": 8},
    )
    return ("chart-support-timeline.html",
            "The fleet, week by week",
            "One column per archived crawl, split by support status. Crawls that "
            "reached very little of the fleet are crawler failures rather than "
            "real movement.",
            traces, lay, "ckan-support-timeline")


def chart_version_spread(d, top=16):
    rows = d["versions"][:top][::-1]
    labels = [v for v, _ in rows]
    counts = [n for _, n in rows]
    # colour each bar by the branch's support status, using the instance rows
    status_by_version = {}
    for inst in d["instances"]:
        if inst.get("v"):
            status_by_version.setdefault(inst["v"], inst["s"])
    colors = [STATUS_FILL.get(status_by_version.get(v, "unknown"), STATUS_FILL["unknown"])
              for v in labels]
    traces = [{
        "type": "bar", "orientation": "h",
        "x": counts, "y": labels,
        "marker": {"color": colors},
        "hovertemplate": "CKAN %{y}<br>%{x} portals<extra></extra>",
    }]
    lay = layout(
        xaxis=dict(AXIS, title={"text": "portals",
                                "font": {"family": FONT, "size": 12, "color": MUTE}}),
        yaxis=dict(AXIS, showgrid=False, type="category"),
        margin={"l": 8, "r": 8, "t": 10, "b": 8},
    )
    return ("chart-version-spread.html",
            "Which CKAN versions are actually in the wild?",
            f"The {len(rows)} most common version strings reported in the latest "
            "crawl, coloured by whether that version still receives patches.",
            traces, lay, "ckan-version-spread")


def chart_extension_adoption(d, top=20):
    rows = d["exts"][:top][::-1]
    traces = [{
        "type": "bar", "orientation": "h",
        "x": [e["count"] for e in rows],
        "y": [e["name"] for e in rows],
        "marker": {"color": ["rgba(102,112,133,.8)" if e["core"] else "rgba(58,131,58,.8)"
                             for e in rows]},
        "customdata": [["core" if e["core"] else "third-party", e["pct"]] for e in rows],
        "hovertemplate": "%{y}<br>%{x} portals — %{customdata[1]}%"
                         "<br>%{customdata[0]}<extra></extra>",
    }]
    lay = layout(
        xaxis=dict(AXIS, title={"text": "portals reporting the plugin",
                                "font": {"family": FONT, "size": 12, "color": MUTE}}),
        yaxis=dict(AXIS, showgrid=False, type="category"),
    )
    return ("chart-extension-adoption.html",
            "Which plugins does the ecosystem actually run?",
            f"The {len(rows)} most installed plugins in the latest crawl, of the "
            f"{d['with_ext']} portals that reported a plugin list. Green is "
            "third-party, grey is shipped with CKAN.",
            traces, lay, "ckan-extension-adoption")


def chart_crawl_reliability(d):
    rel = d["reliability"]
    order = [("always", "Answered every crawl", STATUS_FILL["current"]),
             ("flaky", "Answered some crawls", STATUS_FILL["behind"]),
             ("never", "Never answered", STATUS_FILL["eol"])]
    traces = [{
        "type": "bar",
        "x": [lbl for _, lbl, _ in order],
        "y": [rel.get(k, 0) for k, _, _ in order],
        "marker": {"color": [c for _, _, c in order]},
        "hovertemplate": "%{x}<br>%{y} portals<extra></extra>",
    }]
    lay = layout(
        xaxis=dict(AXIS, showgrid=False),
        yaxis=dict(AXIS, title={"text": "portals",
                                "font": {"family": FONT, "size": 12, "color": MUTE}}),
    )
    return ("chart-crawl-reliability.html",
            "How reliably do portals answer?",
            "Counted over the crawls that worked. A portal that answers only "
            "sometimes is as much a crawler problem as a portal problem.",
            traces, lay, "ckan-crawl-reliability")


BUILDERS = [
    chart_support_status,
    chart_support_timeline,
    chart_version_spread,
    chart_extension_adoption,
    chart_crawl_reliability,
]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("payload", nargs="?", default="dashboard-data/dashboard.json",
                    help="dashboard.json written by build_dashboard_json.py")
    ap.add_argument("-o", "--out", default="docs/charts",
                    help="directory to write the chart pages into")
    args = ap.parse_args()

    d = json.loads(pathlib.Path(args.payload).read_text(encoding="utf-8"))
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    generated = d.get("generated", "")

    for build in BUILDERS:
        name, title, subtitle, traces, lay, slug = build(d)
        html = TEMPLATE.format(
            title=title, subtitle=subtitle, generated=generated,
            traces=json.dumps(traces), layout=json.dumps(lay),
            config=json.dumps(config(f"{slug}-{generated}")),
            font=FONT, panel=PANEL, body=BODY, ink=INK, mute=MUTE,
            line=LINE, accent="#d9534f",
        )
        (out / name).write_text(html, encoding="utf-8")
        print(f"  wrote {out / name}")

    print(f"{len(BUILDERS)} charts from the crawl of {generated}")


if __name__ == "__main__":
    main()
