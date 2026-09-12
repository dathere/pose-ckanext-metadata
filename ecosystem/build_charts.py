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


def chart_support_status(d):
    order = ["current", "behind", "eol", "unknown"]
    counts = [d["statuses"].get(k, 0) for k in order]
    traces = [{
        "type": "pie", "hole": 0.62, "sort": False, "direction": "clockwise",
        "labels": [f"{STATUS_LABEL[k]} — {d['statuses'].get(k, 0)}" for k in order],
        "values": counts,
        "marker": {"colors": [STATUS_FILL[k] for k in order],
                   "line": {"color": PANEL, "width": 2}},
        "textinfo": "percent", "textposition": "inside",
        "insidetextorientation": "horizontal",
        "textfont": {"family": FONT, "size": 12.5, "color": PANEL},
        "hovertemplate": "%{label}<br>%{percent} of the fleet<extra></extra>",
    }]
    total = d["latest_total"]
    lay = layout(
        showlegend=True,
        legend={"orientation": "v", "x": 1.0, "xanchor": "left", "y": 0.5,
                "font": {"family": FONT, "size": 12, "color": MUTE},
                "itemsizing": "constant"},
        annotations=[{
            "text": (f"<span style='font-size:26px;color:{INK}'>{total}</span>"
                     f"<br><span style='font-size:12px;color:{MUTE}'>portals</span>"),
            "showarrow": False, "x": 0.5, "y": 0.5,
            "xref": "paper", "yref": "paper", "font": {"family": FONT},
        }],
    )
    return ("chart-support-status.html",
            "How much of the fleet runs supported software?",
            "Every portal in the latest crawl, by whether the CKAN version it "
            "reports still receives patches.",
            traces, lay, "ckan-support-status")


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
