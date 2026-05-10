"""
Build interactive Plotly HTML charts from experiment CSVs under .data/experiment.

Usage:
  pip install -r samples/requirements-thesis.txt
  python samples/render_thesis_figures.py --input-dir .data/experiment --out-dir .data/experiment/figures

Optional: serve the output directory with docker-compose.thesis-viz.yml (nginx).
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Dict, List


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default=".data/experiment")
    parser.add_argument("--out-dir", default=".data/experiment/figures")
    args = parser.parse_args()
    inp = Path(args.input_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        print("plotly not installed; skip HTML figures. Run: pip install -r samples/requirements-thesis.txt")
        return

    palette = {"primary": "#2563eb", "accent": "#f97316", "muted": "#64748b", "fill": "rgba(37,99,235,0.12)"}
    template = "plotly_white"

    def save(fig: go.Figure, name: str):
        fig.write_html(out / name, include_plotlyjs="cdn", full_html=True)

    p_rows = [r for r in read_csv(inp / "perf_parallelism.csv") if r.get("case", "").startswith("p")]
    if p_rows:
        p_rows.sort(key=lambda r: to_float(r, "parallelism"))
        x = [to_float(r, "parallelism") for r in p_rows]
        thr = [to_float(r, "throughput") for r in p_rows]
        lat = [to_float(r, "avg_latency_ms") for r in p_rows]

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(
            go.Bar(x=x, y=thr, name="吞吐 (events/s)", marker_color=palette["primary"], opacity=0.88),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=lat,
                name="平均检测延迟 (ms)",
                mode="lines+markers",
                line=dict(color=palette["accent"], width=3),
                marker=dict(size=10),
            ),
            secondary_y=True,
        )
        lat_lo, lat_hi = y_axis_range(lat, pad_ratio=0.18, floor_zero=True)
        thr_lo, thr_hi = y_axis_range(thr, pad_ratio=0.12, floor_zero=True)
        fig.update_xaxes(title_text="并行度", dtick=1)
        fig.update_yaxes(title_text="吞吐 (events/s)", secondary_y=False, range=[thr_lo, thr_hi])
        fig.update_yaxes(title_text="平均检测延迟 (ms)", secondary_y=True, range=[lat_lo, lat_hi])
        fig.update_layout(
            template=template,
            title=dict(text="并行度与吞吐 / 检测延迟", font=dict(size=20)),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=480,
            margin=dict(l=56, r=56, t=80, b=56),
        )
        save(fig, "parallelism_vs_perf.html")

    s_rows = [r for r in read_csv(inp / "scalability.csv") if r.get("case", "").startswith("s")]
    if s_rows:
        s_rows.sort(key=lambda r: to_float(r, "events"))
        x = [to_float(r, "events") for r in s_rows]
        y = [to_float(r, "throughput") for r in s_rows]
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="lines+markers",
                name="吞吐",
                line=dict(color=palette["primary"], width=3),
                marker=dict(size=11, color=palette["primary"]),
                fill="tozeroy",
                fillcolor=palette["fill"],
            )
        )
        fig.update_xaxes(title_text="事件规模 (条)", tickformat=",")
        y0, y1 = y_axis_range(y, pad_ratio=0.10, floor_zero=True)
        fig.update_yaxes(title_text="吞吐 (events/s)", range=[y0, y1])
        fig.update_layout(
            template=template,
            title=dict(text="数据规模与吞吐", font=dict(size=20)),
            height=440,
            margin=dict(l=56, r=40, t=72, b=56),
            showlegend=False,
        )
        save(fig, "scalability_throughput.html")

    w_rows = [r for r in read_csv(inp / "perf_windows.csv") if r.get("case", "").startswith("w")]
    if w_rows:
        w_rows.sort(key=lambda r: to_float(r, "window"))
        x_sec = [to_float(r, "window") / 1000 for r in w_rows]
        labels = [f"{int(v)}s" for v in x_sec]
        lat = [to_float(r, "avg_latency_ms") for r in w_rows]
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=labels,
                y=lat,
                name="平均延迟",
                marker_color=palette["accent"],
                opacity=0.78,
                text=[f"{v:.0f}" for v in lat],
                textposition="outside",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=labels,
                y=lat,
                name="趋势线",
                mode="lines+markers",
                line=dict(color=palette["primary"], width=3),
                marker=dict(size=12, color=palette["primary"], line=dict(width=1, color="white")),
                yaxis="y",
            )
        )
        y0, y1 = y_axis_range(lat, pad_ratio=0.22, floor_zero=True)
        fig.update_xaxes(title_text="规则时间窗口（滚动窗口长度）", categoryorder="array", categoryarray=labels)
        fig.update_yaxes(title_text="平均检测延迟 (ms)", range=[y0, y1])
        fig.update_layout(
            template=template,
            title=dict(text="时间窗口与平均检测延迟", font=dict(size=20)),
            height=440,
            margin=dict(l=56, r=40, t=72, b=56),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1),
            barmode="overlay",
        )
        save(fig, "window_vs_latency.html")

    b_rows = [r for r in read_csv(inp / "perf_backend.csv") if r.get("case", "").startswith("b")]
    if b_rows:
        disp = {"hashmap": "HashMap", "rocksdb": "RocksDB"}
        labels = [disp.get(str(r.get("backend", "")).lower(), str(r.get("backend", ""))) for r in b_rows]
        mem = [to_float(r, "mem_mb") for r in b_rows]
        cpu = [to_float(r, "cpu_pct") for r in b_rows]
        fig = make_subplots(
            rows=1,
            cols=2,
            subplot_titles=("CPU 占用（%，采样均值）", "内存占用（MiB，采样均值）"),
            horizontal_spacing=0.14,
        )
        fig.add_trace(
            go.Bar(x=labels, y=cpu, name="CPU %", marker_color=palette["primary"], text=[f"{v:.1f}" for v in cpu], textposition="outside"),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Bar(x=labels, y=mem, name="内存 MiB", marker_color=palette["muted"], text=[f"{v:.0f}" for v in mem], textposition="outside"),
            row=1,
            col=2,
        )
        c0, c1 = y_axis_range(cpu, pad_ratio=0.20, floor_zero=True)
        m0, m1 = y_axis_range(mem, pad_ratio=0.14, floor_zero=False)
        fig.update_yaxes(range=[c0, c1], row=1, col=1)
        fig.update_yaxes(range=[m0, m1], row=1, col=2)
        fig.update_layout(
            template=template,
            title=dict(text="状态后端：TaskManager 资源占用对比", font=dict(size=18)),
            height=440,
            showlegend=False,
            margin=dict(l=56, r=40, t=88, b=56),
        )
        save(fig, "backend_resource.html")

    acc_rows = read_csv(inp / "functional_metrics.csv")
    if acc_rows:
        types = [str(r.get("异常类型", "") or "").strip() for r in acc_rows]
        prec = [to_float(r, "Precision") for r in acc_rows]
        rec = [to_float(r, "Recall") for r in acc_rows]
        f1 = [to_float(r, "F1") for r in acc_rows]
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Precision", x=types, y=prec, marker_color=palette["primary"], text=[f"{v:.3f}" for v in prec], textposition="outside"))
        fig.add_trace(go.Bar(name="Recall", x=types, y=rec, marker_color=palette["accent"], text=[f"{v:.3f}" for v in rec], textposition="outside"))
        fig.add_trace(go.Bar(name="F1", x=types, y=f1, marker_color=palette["muted"], text=[f"{v:.3f}" for v in f1], textposition="outside"))
        score_lo = min(prec + rec + f1)
        score_hi = max(prec + rec + f1)
        pad = max(0.02, (score_hi - score_lo) * 0.35)
        fig.update_yaxes(title_text="得分", range=[max(0.0, score_lo - pad), min(1.0, score_hi + pad)], tickformat=".2f")
        fig.update_layout(
            template=template,
            title=dict(text="各异常类型检测效果对比（Precision / Recall / F1）", font=dict(size=18)),
            barmode="group",
            bargap=0.18,
            bargroupgap=0.08,
            height=460,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=56, r=40, t=88, b=72),
        )
        save(fig, "detection_accuracy.html")

    (out / "index.html").write_text(
        """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <title>论文实验图表</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {
      --bg: #0b1220;
      --panel: rgba(255,255,255,0.06);
      --panel2: rgba(255,255,255,0.08);
      --text: #e5e7eb;
      --muted: #94a3b8;
      --link: #60a5fa;
      --ring: rgba(96,165,250,0.35);
      --shadow: 0 10px 30px rgba(0,0,0,0.35);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      color: var(--text);
      background:
        radial-gradient(1200px 500px at 15% -10%, rgba(96,165,250,0.22), transparent 60%),
        radial-gradient(900px 450px at 90% 0%, rgba(249,115,22,0.18), transparent 55%),
        radial-gradient(1100px 520px at 40% 110%, rgba(34,197,94,0.12), transparent 60%),
        var(--bg);
    }
    a { color: var(--link); text-decoration: none; }
    a:hover { text-decoration: underline; }
    .container { max-width: 1080px; margin: 0 auto; padding: 28px 18px 44px; }
    header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
    h1 { font-size: 22px; margin: 0; letter-spacing: 0.2px; }
    .subtitle { color: var(--muted); margin-top: 6px; font-size: 13px; line-height: 1.45; }
    .chip {
      display: inline-flex; align-items: center; gap: 8px;
      padding: 8px 10px; border: 1px solid rgba(255,255,255,0.14);
      border-radius: 999px; background: rgba(255,255,255,0.05);
      color: var(--muted); font-size: 12px; white-space: nowrap;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 14px;
      margin-top: 16px;
    }
    .card {
      grid-column: span 6;
      border: 1px solid rgba(255,255,255,0.14);
      background: linear-gradient(180deg, var(--panel2), var(--panel));
      border-radius: 14px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .card.span12 { grid-column: span 12; }
    .card h2 { font-size: 15px; margin: 0; }
    .card .meta { color: var(--muted); font-size: 12px; margin-top: 6px; line-height: 1.45; }
    .card .top { padding: 14px 14px 10px; }
    .actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
    .btn {
      display: inline-flex; align-items: center; gap: 8px;
      padding: 8px 10px; border-radius: 10px;
      border: 1px solid rgba(255,255,255,0.14);
      background: rgba(15,23,42,0.15);
      color: var(--text);
    }
    .btn:hover { border-color: rgba(96,165,250,0.6); box-shadow: 0 0 0 3px var(--ring); text-decoration: none; }
    .preview {
      border-top: 1px solid rgba(255,255,255,0.12);
      background: rgba(2,6,23,0.35);
      height: 360px;
    }
    iframe { width: 100%; height: 100%; border: 0; }
    .footer { margin-top: 18px; color: var(--muted); font-size: 12px; }
    code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    @media (max-width: 960px) {
      .card { grid-column: span 12; }
      .preview { height: 420px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div>
        <h1>论文实验图表</h1>
        <div class="subtitle">
          由 <code>samples/render_thesis_figures.py</code> 从 <code>.data/experiment/*.csv</code> 生成。
          点击「打开交互图」可在新标签页查看；下方为内嵌预览。
        </div>
      </div>
      <div class="chip">Plotly · HTML</div>
    </header>

    <div class="grid">
      <section class="card">
        <div class="top">
          <h2>并行度 / 吞吐与延迟</h2>
          <div class="meta">展示并行度变化下的吞吐（events/s）与平均告警延迟（ms）。</div>
          <div class="actions">
            <a class="btn" href="parallelism_vs_perf.html" target="_blank" rel="noreferrer">打开交互图</a>
          </div>
        </div>
        <div class="preview"><iframe loading="lazy" src="parallelism_vs_perf.html"></iframe></div>
      </section>

      <section class="card">
        <div class="top">
          <h2>数据规模与吞吐</h2>
          <div class="meta">展示事件规模（条）增长时的吞吐变化趋势。</div>
          <div class="actions">
            <a class="btn" href="scalability_throughput.html" target="_blank" rel="noreferrer">打开交互图</a>
          </div>
        </div>
        <div class="preview"><iframe loading="lazy" src="scalability_throughput.html"></iframe></div>
      </section>

      <section class="card">
        <div class="top">
          <h2>时间窗口与平均检测延迟</h2>
          <div class="meta">展示规则窗口（秒）变化对平均告警延迟（ms）的影响。</div>
          <div class="actions">
            <a class="btn" href="window_vs_latency.html" target="_blank" rel="noreferrer">打开交互图</a>
          </div>
        </div>
        <div class="preview"><iframe loading="lazy" src="window_vs_latency.html"></iframe></div>
      </section>

      <section class="card">
        <div class="top">
          <h2>状态后端资源占用</h2>
          <div class="meta">对比 HashMap / RocksDB 状态后端下 TaskManager CPU 与内存采样均值。</div>
          <div class="actions">
            <a class="btn" href="backend_resource.html" target="_blank" rel="noreferrer">打开交互图</a>
          </div>
        </div>
        <div class="preview"><iframe loading="lazy" src="backend_resource.html"></iframe></div>
      </section>

      <section class="card span12">
        <div class="top">
          <h2>检测准确率对比</h2>
          <div class="meta">按异常类型对比 Precision、Recall 与 F1（来源：<code>functional_metrics.csv</code>）。</div>
          <div class="actions">
            <a class="btn" href="detection_accuracy.html" target="_blank" rel="noreferrer">打开交互图</a>
          </div>
        </div>
        <div class="preview"><iframe loading="lazy" src="detection_accuracy.html"></iframe></div>
      </section>
    </div>

    <div class="footer">
      提示：如果用 nginx 挂载 <code>.data/experiment/figures</code>，本页可直接作为入口页。
    </div>
  </div>
</body>
</html>""",
        encoding="utf-8",
    )
    print(f"wrote figures to {out.resolve()}")


if __name__ == "__main__":
    main()
