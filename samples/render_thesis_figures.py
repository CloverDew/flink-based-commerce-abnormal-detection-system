"""
Build interactive Plotly HTML charts from experiment CSVs.

Usage:
  pip install -r samples/requirements-thesis.txt
  python samples/render_thesis_figures.py --input-dir .data/experiment --out-dir .data/experiment/figures

When available, this renderer prefers real case-level TP/FP/FN-derived metrics and
per-rule rule_metrics.csv output from run_thesis_experiments.py.
"""

from __future__ import annotations

import argparse
import csv
import html
from pathlib import Path
from typing import Any, Dict, List

from experiment_artifacts import CLASSIFICATION_DISCLAIMER, CLASSIFICATION_LABEL, read_json
from thesis_display_metrics import enrich_rows_for_charts, patch_functional_metric_rows


def read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def to_float(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(row.get(key, default) or default)
    except (TypeError, ValueError):
        return default


def alert_count(row: Dict[str, Any]) -> float:
    return max(to_float(row, "alerts"), to_float(row, "alerts_raw_total"))


def normalized_detection_rates(rows: List[Dict[str, Any]]) -> tuple[List[float], float]:
    counts = [alert_count(r) for r in rows]
    reference = max(counts) if counts else 0.0
    if reference <= 0:
        return [0.0 for _ in counts], 0.0
    return [value / reference for value in counts], reference


def has_real_quality_metrics(rows: List[Dict[str, Any]]) -> bool:
    if not rows:
        return False
    required = ("macro_precision", "macro_recall", "macro_f1")
    return all(all(str(row.get(key, "")).strip() for key in required) for row in rows)


def ratio_text(values: List[float]) -> List[str]:
    return [f"{value:.1%}" for value in values]


def y_axis_range(values: List[float], *, pad_ratio: float = 0.14, floor_zero: bool = True) -> tuple[float, float]:
    if not values:
        return 0.0, 1.0
    lo = min(values)
    hi = max(values)
    if lo == hi:
        span = abs(hi) if hi else 1.0
        lo, hi = lo - span * 0.5, hi + span * 0.5
    else:
        span = hi - lo
        lo, hi = lo - span * pad_ratio, hi + span * pad_ratio
    if floor_zero:
        lo = min(0.0, lo)
    return lo, hi


def zoomed_axis_range(values: List[float], *, pad_ratio: float = 0.18, min_relative_span: float = 0.06) -> tuple[float, float]:
    """Tight axis around the data so small but real deltas remain visible."""
    if not values:
        return 0.0, 1.0
    lo = min(values)
    hi = max(values)
    center = (lo + hi) / 2.0
    span = hi - lo
    if span <= 0:
        margin = max(abs(center) * min_relative_span, 1.0)
    else:
        margin = max(span * pad_ratio, abs(center) * min_relative_span)
    return lo - margin, hi + margin


def throughput_axis_range(values: List[float]) -> tuple[float, float]:
    return zoomed_axis_range(values, pad_ratio=0.22, min_relative_span=0.025)


def quality_axis_range(values: List[float]) -> tuple[float, float]:
    lo, hi = zoomed_axis_range(values, pad_ratio=0.16, min_relative_span=0.04)
    return max(0.0, lo), min(1.0, hi)


def clamp_unit_axis(values: List[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 1.0
    lo, hi = min(values), max(values)
    if hi - lo < 0.08:
        return quality_axis_range(values)
    lo, hi = y_axis_range(values, pad_ratio=0.12, floor_zero=False)
    return max(0.0, lo), min(1.0, hi)


def window_latency_cap_ms(row: Dict[str, Any]) -> float:
    return float(max(120_000, int(to_float(row, "window", 60_000)) * 5))


def latency_is_censored(row: Dict[str, Any]) -> bool:
    lat = to_float(row, "avg_latency_ms")
    if lat <= 0:
        return False
    return lat >= window_latency_cap_ms(row) * 0.98


def load_manifest(manifest_path: Path | None, input_dir: Path) -> Dict[str, Any]:
    if manifest_path and manifest_path.exists():
        return read_json(manifest_path)
    candidate = input_dir / "manifest.json"
    if candidate.exists():
        return read_json(candidate)
    sibling = input_dir.parent / "manifest.json"
    if sibling.exists():
        return read_json(sibling)
    return {}


def choose_reference_case(rows: List[Dict[str, Any]]) -> str:
    cases: List[str] = []
    seen = set()
    for row in rows:
        case_name = str(row.get("case", "")).strip()
        if case_name and case_name not in seen:
            seen.add(case_name)
            cases.append(case_name)
    for preferred in ("w60000", "p4"):
        if preferred in seen:
            return preferred
    return cases[0] if cases else ""


def build_index_html(manifest: Dict[str, Any], cards: List[Dict[str, str]]) -> str:
    classification = manifest.get("classification", {})
    label = str(classification.get("label") or CLASSIFICATION_LABEL)
    disclaimer = str(classification.get("disclaimer") or CLASSIFICATION_DISCLAIMER)
    run_id = str(manifest.get("runId") or "unknown-run")
    created_at = str(manifest.get("createdAt") or "")
    git = manifest.get("git", {})
    git_head = str(git.get("head") or "")
    generation = manifest.get("generation", {})
    reproduce = str(generation.get("reproduceCommand") or "")

    chips = [
        f"<span class='chip'>Data: {html.escape(label)}</span>",
        f"<span class='chip'>Run: {html.escape(run_id)}</span>",
    ]
    if created_at:
        chips.append(f"<span class='chip'>Created: {html.escape(created_at)}</span>")
    if git_head:
        chips.append(f"<span class='chip'>Git: {html.escape(git_head[:12])}</span>")

    metadata_links = [
        "<a class='btn' href='../manifest.json' target='_blank' rel='noreferrer'>manifest.json</a>",
        "<a class='btn' href='../summary.json' target='_blank' rel='noreferrer'>summary.json</a>",
    ]
    if reproduce:
        metadata_links.append("<a class='btn' href='../reproduce-command.txt' target='_blank' rel='noreferrer'>reproduce-command.txt</a>")

    cards_html = "\n".join(
        f"""
      <section class="card{' span12' if card.get('span12') else ''}">
        <div class="top">
          <h2>{html.escape(card['title'])}</h2>
          <div class="meta">{html.escape(card['meta'])}</div>
          <div class="actions">
            <a class="btn" href="{html.escape(card['href'])}" target="_blank" rel="noreferrer">查看图表</a>
          </div>
        </div>
        <div class="preview"><iframe loading="lazy" src="{html.escape(card['href'])}"></iframe></div>
      </section>"""
        for card in cards
    )
    if not cards_html:
        cards_html = """
      <section class="card span12">
        <div class="top">
          <h2>暂无图表</h2>
          <div class="meta">当前目录尚未包含可绘图的 CSV 数据文件。</div>
        </div>
      </section>"""

    reproduce_html = f"<div class='subtitle' style='margin-top:10px;'><code>{html.escape(reproduce)}</code></div>" if reproduce else ""
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <title>Thesis Experiment Figures</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {{
      --bg: #0b1220;
      --panel: rgba(255,255,255,0.06);
      --panel2: rgba(255,255,255,0.08);
      --text: #e5e7eb;
      --muted: #94a3b8;
      --link: #60a5fa;
      --ring: rgba(96,165,250,0.35);
      --shadow: 0 10px 30px rgba(0,0,0,0.35);
      --warn: rgba(251,191,36,0.16);
      --warnBorder: rgba(251,191,36,0.45);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(1200px 500px at 15% -10%, rgba(96,165,250,0.22), transparent 60%),
        radial-gradient(900px 450px at 90% 0%, rgba(249,115,22,0.18), transparent 55%),
        radial-gradient(1100px 520px at 40% 110%, rgba(34,197,94,0.12), transparent 60%),
        var(--bg);
    }}
    a {{ color: var(--link); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .container {{ max-width: 1120px; margin: 0 auto; padding: 28px 18px 44px; }}
    .banner {{
      border: 1px solid var(--warnBorder);
      background: linear-gradient(180deg, rgba(251,191,36,0.22), var(--warn));
      border-radius: 16px;
      padding: 14px 16px;
      margin-bottom: 18px;
      box-shadow: var(--shadow);
    }}
    .banner strong {{ display: block; font-size: 15px; margin-bottom: 4px; }}
    header {{ display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }}
    h1 {{ font-size: 22px; margin: 0; letter-spacing: 0.2px; }}
    .subtitle {{ color: var(--muted); margin-top: 6px; font-size: 13px; line-height: 1.45; }}
    .chipRow {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }}
    .chip {{
      display: inline-flex; align-items: center; gap: 8px;
      padding: 8px 10px; border: 1px solid rgba(255,255,255,0.14);
      border-radius: 999px; background: rgba(255,255,255,0.05);
      color: var(--muted); font-size: 12px; white-space: nowrap;
    }}
    .metaPanel {{
      border: 1px solid rgba(255,255,255,0.14);
      background: linear-gradient(180deg, var(--panel2), var(--panel));
      border-radius: 14px;
      padding: 14px;
      box-shadow: var(--shadow);
      margin-bottom: 16px;
    }}
    .metaPanel h2 {{ font-size: 15px; margin: 0 0 8px; }}
    .metaPanel p {{ margin: 0; color: var(--muted); font-size: 13px; line-height: 1.55; }}
    .actions {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }}
    .btn {{
      display: inline-flex; align-items: center; gap: 8px;
      padding: 8px 10px; border-radius: 10px;
      border: 1px solid rgba(255,255,255,0.14);
      background: rgba(15,23,42,0.15);
      color: var(--text);
    }}
    .btn:hover {{ border-color: rgba(96,165,250,0.6); box-shadow: 0 0 0 3px var(--ring); text-decoration: none; }}
    .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 14px; margin-top: 16px; }}
    .card {{
      grid-column: span 6;
      border: 1px solid rgba(255,255,255,0.14);
      background: linear-gradient(180deg, var(--panel2), var(--panel));
      border-radius: 14px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .card.span12 {{ grid-column: span 12; }}
    .card h2 {{ font-size: 15px; margin: 0; }}
    .card .meta {{ color: var(--muted); font-size: 12px; margin-top: 6px; line-height: 1.45; }}
    .card .top {{ padding: 14px 14px 10px; }}
    .preview {{ border-top: 1px solid rgba(255,255,255,0.12); background: rgba(2,6,23,0.35); height: 360px; }}
    iframe {{ width: 100%; height: 100%; border: 0; }}
    .footer {{ margin-top: 18px; color: var(--muted); font-size: 12px; }}
    code {{ white-space: pre-wrap; word-break: break-word; }}
    @media (max-width: 960px) {{
      .card {{ grid-column: span 12; }}
      .preview {{ height: 420px; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="banner">
      <strong>{html.escape(label)}</strong>
      <div>{html.escape(disclaimer)}</div>
    </div>

    <header>
      <div>
        <h1>毕业论文实验结果图表</h1>
        <div class="subtitle">
          由当前运行目录的 CSV 输出生成。当案例级 TP/FP/FN 指标存在时，检测质量图展示真实宏平均 Precision / Recall / F1，否则展示归一化告警数。
        </div>
        <div class="chipRow">
          {''.join(chips)}
        </div>
      </div>
      <div class="chip">交互式图表（Plotly）</div>
    </header>

    <section class="metaPanel">
      <h2>数据溯源</h2>
      <p>引用本图表作为实验证据前，请先查阅 manifest、summary 及复现命令，确认数据来源与生成参数。</p>
      <div class="actions">
        {' '.join(metadata_links)}
      </div>
      {reproduce_html}
    </section>

    <div class="grid">
      {cards_html}
    </div>

    <div class="footer">
      提示：可通过 nginx 或 python -m http.server 托管本目录，在浏览器中直接打开交互图表。
    </div>
  </div>
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default=".data/experiment")
    parser.add_argument("--out-dir", default=".data/experiment/figures")
    parser.add_argument("--manifest", default="", help="Optional manifest.json path for metadata banner and audit links.")
    args = parser.parse_args()
    inp = Path(args.input_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(Path(args.manifest) if args.manifest else None, inp)

    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        print("plotly not installed; skip HTML figures. Run: pip install -r samples/requirements-thesis.txt")
        return

    palette = {
        "primary": "#2563eb",
        "accent": "#f97316",
        "muted": "#64748b",
        "fill": "rgba(37,99,235,0.12)",
    }
    template = "plotly_white"
    cards: List[Dict[str, str]] = []

    def save(fig: go.Figure, name: str, title: str, meta: str, *, span12: bool = False):
        fig.write_html(out / name, include_plotlyjs="cdn", full_html=True)
        cards.append({"href": name, "title": title, "meta": meta, "span12": "1" if span12 else ""})

    p_rows = enrich_rows_for_charts([r for r in read_csv(inp / "perf_parallelism.csv") if r.get("case", "").startswith("p")])
    if p_rows:
        p_rows.sort(key=lambda r: to_float(r, "parallelism"))
        x = [to_float(r, "parallelism") for r in p_rows]
        thr = [to_float(r, "throughput") for r in p_rows]
        lat = [to_float(r, "avg_latency_ms") for r in p_rows]
        thr_lo, thr_hi = throughput_axis_range(thr)
        lat_lo, lat_hi = y_axis_range([v for v in lat if v > 0] or lat, pad_ratio=0.18, floor_zero=True)

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=x,
                y=thr,
                name="Throughput (events/s)",
                marker_color=palette["primary"],
                opacity=0.88,
                text=[f"{v:,.0f}" for v in thr],
                textposition="outside",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=lat,
                name="Average alert latency (ms)",
                mode="lines+markers",
                line=dict(color=palette["accent"], width=3),
                marker=dict(size=10),
            ),
            secondary_y=True,
        )
        fig.update_xaxes(title_text="Parallelism", dtick=1)
        fig.update_yaxes(title_text="Throughput (events/s)", secondary_y=False, range=[thr_lo, thr_hi])
        fig.update_yaxes(title_text="Average alert latency (ms)", secondary_y=True, range=[lat_lo, lat_hi])
        fig.update_layout(
            template=template,
            title=dict(text="Parallelism vs throughput and latency", font=dict(size=20)),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=480,
            margin=dict(l=56, r=56, t=80, b=56),
        )
        save(
            fig,
            "parallelism_vs_perf.html",
            "Parallelism vs throughput and latency",
            "Shows throughput and average alert latency for each parallelism case.",
        )

        macro_precision = [to_float(r, "macro_precision") for r in p_rows]
        macro_recall = [to_float(r, "macro_recall") for r in p_rows]
        macro_f1 = [to_float(r, "macro_f1") for r in p_rows]
        q_lo, q_hi = quality_axis_range(macro_precision + macro_recall + macro_f1)
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=x,
                y=macro_precision,
                name="Macro Precision",
                mode="lines+markers",
                line=dict(color=palette["primary"], width=3),
                marker=dict(size=10),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=macro_recall,
                name="Macro Recall",
                mode="lines+markers",
                line=dict(color=palette["accent"], width=3),
                marker=dict(size=10),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=macro_f1,
                name="Macro F1",
                mode="lines+markers",
                line=dict(color=palette["muted"], width=3),
                marker=dict(size=10),
            )
        )
        fig.update_xaxes(title_text="Parallelism", dtick=1)
        fig.update_yaxes(title_text="Case-level quality score", range=[q_lo, q_hi], tickformat=".0%")
        fig.update_layout(
            template=template,
            title=dict(text="Parallelism vs case-level quality metrics", font=dict(size=20)),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=470,
            margin=dict(l=56, r=56, t=80, b=56),
        )
        save(
            fig,
            "parallelism_vs_detection_rate.html",
            "Parallelism vs case-level quality metrics",
            "Uses scoped-alert TP/FP/FN aggregates and plots macro Precision / Recall / F1.",
        )

    s_rows = [r for r in read_csv(inp / "scalability.csv") if r.get("case", "").startswith("s")]
    if s_rows:
        s_rows.sort(key=lambda r: to_float(r, "events"))
        x = [to_float(r, "events") for r in s_rows]
        y = [to_float(r, "throughput") for r in s_rows]
        y0, y1 = throughput_axis_range(y) if len(y) > 1 and (max(y) - min(y)) / max(y) < 0.35 else y_axis_range(y, pad_ratio=0.10, floor_zero=True)
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="lines+markers",
                name="Throughput",
                line=dict(color=palette["primary"], width=3),
                marker=dict(size=11, color=palette["primary"]),
                fill="tozeroy",
                fillcolor=palette["fill"],
            )
        )
        fig.update_xaxes(title_text="事件规模（条）", tickformat=",")
        fig.update_yaxes(title_text="吞吐量（条/秒）", range=[y0, y1])
        fig.update_layout(template=template, title=dict(text="Scalability throughput", font=dict(size=20)), height=440, margin=dict(l=56, r=40, t=72, b=56), showlegend=False)
        save(fig, "scalability_throughput.html", "Scalability throughput",
             "Shows throughput as event volume increases.")

    w_rows = enrich_rows_for_charts([r for r in read_csv(inp / "perf_windows.csv") if r.get("case", "").startswith("w")])
    if w_rows:
        w_rows.sort(key=lambda r: to_float(r, "window"))
        labels = [f"{int(to_float(r, 'window') / 1000)}s" for r in w_rows]
        lat = [to_float(r, "avg_latency_ms") for r in w_rows]
        censored = [latency_is_censored(r) for r in w_rows]
        lat_text = [f"{v / 1000:.0f}s{'*' if c else ''}" for v, c in zip(lat, censored)]
        uncensored_lat = [v for v, c in zip(lat, censored) if not c]
        y0, y1 = y_axis_range(uncensored_lat or lat, pad_ratio=0.22, floor_zero=True)

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=labels,
                y=lat,
                name="Average latency",
                marker_color=palette["accent"],
                opacity=0.82,
                text=[f"{int(v):,}" for v in lat],
                textposition="outside",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=labels,
                y=lat,
                name="Trend",
                mode="lines+markers",
                line=dict(color=palette["primary"], width=3),
                marker=dict(size=12, color=palette["primary"], line=dict(width=1, color="white")),
            )
        )
        fig.update_xaxes(title_text="Window size", categoryorder="array", categoryarray=labels)
        fig.update_yaxes(title_text="Average alert latency (ms)", range=[y0, y1])
        fig.update_layout(
            template=template,
            title=dict(text="Window size vs average latency", font=dict(size=20)),
            height=440,
            margin=dict(l=56, r=40, t=72, b=56),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1),
            barmode="overlay",
        )
        save(
            fig,
            "window_vs_latency.html",
            "Window size vs average latency",
            "Shows how alert latency changes as the rule window grows.",
        )

        macro_precision = [to_float(r, "macro_precision") for r in w_rows]
        macro_recall = [to_float(r, "macro_recall") for r in w_rows]
        macro_f1 = [to_float(r, "macro_f1") for r in w_rows]
        q_lo, q_hi = clamp_unit_axis(macro_precision + macro_recall + macro_f1)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=labels, y=macro_precision, name="Macro Precision", mode="lines+markers", line=dict(color=palette["primary"], width=3), marker=dict(size=10)))
        fig.add_trace(go.Scatter(x=labels, y=macro_recall, name="Macro Recall", mode="lines+markers", line=dict(color=palette["accent"], width=3), marker=dict(size=10)))
        fig.add_trace(go.Scatter(x=labels, y=macro_f1, name="Macro F1", mode="lines+markers", line=dict(color=palette["muted"], width=3), marker=dict(size=10)))
        fig.update_xaxes(title_text="Window size", categoryorder="array", categoryarray=labels)
        fig.update_yaxes(title_text="Case-level quality score", range=[q_lo, q_hi], tickformat=".0%")
        fig.update_layout(
            template=template,
            title=dict(text="Window size vs case-level quality metrics", font=dict(size=20)),
            height=470,
            margin=dict(l=56, r=48, t=80, b=56),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        save(
            fig,
            "window_vs_detection_rate.html",
            "Window size vs case-level quality metrics",
            "Uses scoped-alert TP/FP/FN aggregates and plots macro Precision / Recall / F1.",
        )

    b_rows = [r for r in read_csv(inp / "perf_backend.csv") if r.get("case", "").startswith("b")]
    if b_rows:
        display_names = {"hashmap": "HashMap", "rocksdb": "RocksDB"}
        labels = [display_names.get(str(r.get("backend", "")).lower(), str(r.get("backend", ""))) for r in b_rows]
        mem = [to_float(r, "mem_mb") for r in b_rows]
        cpu = [to_float(r, "cpu_pct") for r in b_rows]
        c0, c1 = y_axis_range(cpu, pad_ratio=0.20, floor_zero=True)
        m0, m1 = y_axis_range(mem, pad_ratio=0.14, floor_zero=False)
        fig = make_subplots(rows=1, cols=2, subplot_titles=("CPU % (sample mean)", "Memory MiB (sample mean)"), horizontal_spacing=0.14)
        fig.add_trace(go.Bar(x=labels, y=cpu, name="CPU %", marker_color=palette["primary"], text=[f"{v:.1f}" for v in cpu], textposition="outside"), row=1, col=1)
        fig.add_trace(go.Bar(x=labels, y=mem, name="Memory MiB", marker_color=palette["muted"], text=[f"{v:.0f}" for v in mem], textposition="outside"), row=1, col=2)
        fig.update_yaxes(range=[c0, c1], row=1, col=1)
        fig.update_yaxes(range=[m0, m1], row=1, col=2)
        fig.update_layout(template=template, title=dict(text="Backend resource comparison", font=dict(size=18)), height=440, showlegend=False, margin=dict(l=56, r=40, t=88, b=56))
        save(fig, "backend_resource.html", "Backend resource comparison",
             "Compares TaskManager CPU and memory between HashMap and RocksDB backends.")

    rule_metric_rows = read_csv(inp / "rule_metrics.csv")
    if not rule_metric_rows:
        cases_dir = inp / "cases"
        if cases_dir.is_dir():
            for case_dir in sorted(cases_dir.iterdir()):
                candidate = case_dir / "rule_metrics.csv"
                if candidate.exists():
                    rule_metric_rows.extend(read_csv(candidate))
    reference_case = choose_reference_case(rule_metric_rows)
    if rule_metric_rows and reference_case:
        behavior_rows = [row for row in rule_metric_rows if str(row.get("case", "")).strip() == reference_case]
        types = [str(r.get("rule_label") or r.get("rule_type") or "").strip() for r in behavior_rows]
        prec = [to_float(r, "precision") for r in behavior_rows]
        rec = [to_float(r, "recall") for r in behavior_rows]
        f1 = [to_float(r, "f1") for r in behavior_rows]
        score_lo, score_hi = clamp_unit_axis(prec + rec + f1)
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Precision", x=types, y=prec, marker_color=palette["primary"], text=ratio_text(prec), textposition="outside"))
        fig.add_trace(go.Bar(name="Recall", x=types, y=rec, marker_color=palette["accent"], text=ratio_text(rec), textposition="outside"))
        fig.add_trace(go.Bar(name="F1", x=types, y=f1, marker_color=palette["muted"], text=ratio_text(f1), textposition="outside"))
        fig.update_yaxes(title_text="Per-rule quality score", range=[score_lo, score_hi], tickformat=".0%")
        fig.update_layout(template=template, title=dict(text=f"Per-rule Precision / Recall / F1 ({reference_case})", font=dict(size=18)), barmode="group", bargap=0.18, bargroupgap=0.08, height=460, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), margin=dict(l=56, r=40, t=88, b=72))
        save(fig, "detection_accuracy.html", "Per-rule quality metrics",
             f"Uses true per-rule TP/FP/FN metrics from rule_metrics.csv for reference case {reference_case}.",
             span12=True)
    else:
        functional_rows = patch_functional_metric_rows(read_csv(inp / "functional_metrics.csv"))
        if functional_rows:
            types = [str(r.get("异常类型", "") or "").strip() for r in functional_rows]
            prec = [to_float(r, "Precision") for r in functional_rows]
            rec = [to_float(r, "Recall") for r in functional_rows]
            f1 = [to_float(r, "F1") for r in functional_rows]
            score_lo, score_hi = clamp_unit_axis(prec + rec + f1)
            fig = go.Figure()
            fig.add_trace(go.Bar(name="精确率", x=types, y=prec, marker_color=palette["primary"], text=ratio_text(prec), textposition="outside"))
            fig.add_trace(go.Bar(name="召回率", x=types, y=rec, marker_color=palette["accent"], text=ratio_text(rec), textposition="outside"))
            fig.add_trace(go.Bar(name="F1 分数", x=types, y=f1, marker_color=palette["muted"], text=ratio_text(f1), textposition="outside"))
            fig.update_yaxes(title_text="各规则检测质量分数", range=[score_lo, score_hi], tickformat=".0%")
            fig.update_layout(template=template, title=dict(text="各异常类型检测精确率、召回率与 F1 分数", font=dict(size=18)), barmode="group", bargap=0.18, bargroupgap=0.08, height=460, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), margin=dict(l=56, r=40, t=88, b=72))
            save(fig, "detection_accuracy.html", "各异常类型检测质量指标",
                 "三类异常规则的逐规则精确率、召回率与 F1 分数，反映系统在不同行为模式下的检测能力差异。",
                 span12=True)

    (out / "index.html").write_text(build_index_html(manifest, cards), encoding="utf-8")
    print(f"wrote figures to {out.resolve()}")


if __name__ == "__main__":
    main()
