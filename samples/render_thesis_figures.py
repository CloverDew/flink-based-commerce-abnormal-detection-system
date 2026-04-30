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
        fig.add_hrect(
            y0=60_000,
            y1=1_800_000,
            line_width=0,
            fillcolor="rgba(100,116,139,0.15)",
            secondary_y=True,
            annotation_text="典型离线/T+1 审计常见量级 (分钟级，示意参考带)",
            annotation_position="top left",
        )
        fig.update_xaxes(title_text="并行度", dtick=1)
        fig.update_yaxes(title_text="吞吐 (events/s)", secondary_y=False, rangemode="tozero")
        fig.update_yaxes(title_text="平均告警延迟 (ms)", secondary_y=True, rangemode="tozero")
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
        fig.update_yaxes(title_text="吞吐 (events/s)", rangemode="tozero")
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
        x = [to_float(r, "window") / 1000 for r in w_rows]
        lat = [to_float(r, "avg_latency_ms") for r in w_rows]
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=[f"{int(v)}s" for v in x],
                y=lat,
                marker_color=palette["accent"],
                text=[f"{v:.1f}" for v in lat],
                textposition="outside",
            )
        )
        fig.update_xaxes(title_text="规则时间窗口")
        fig.update_yaxes(title_text="平均告警延迟 (ms)", rangemode="tozero")
        fig.update_layout(
            template=template,
            title=dict(text="时间窗口与平均检测延迟", font=dict(size=20)),
            height=420,
            margin=dict(l=56, r=40, t=72, b=56),
            showlegend=False,
        )
        save(fig, "window_vs_latency.html")

    b_rows = [r for r in read_csv(inp / "perf_backend.csv") if r.get("case", "").startswith("b")]
    if b_rows:
        labels = [r.get("backend", "") for r in b_rows]
        mem = [to_float(r, "mem_mb") for r in b_rows]
        cpu = [to_float(r, "cpu_pct") for r in b_rows]
        fig = go.Figure()
        fig.add_trace(go.Bar(name="CPU %", x=labels, y=cpu, marker_color=palette["primary"]))
        fig.add_trace(go.Bar(name="内存 MiB", x=labels, y=mem, marker_color=palette["muted"], yaxis="y2"))
        fig.update_layout(
            template=template,
            title=dict(text="状态后端：TaskManager 资源占用（采样均值）", font=dict(size=18)),
            barmode="group",
            height=420,
            yaxis=dict(title="CPU %"),
            yaxis2=dict(title="内存 MiB", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", y=1.08),
            margin=dict(l=56, r=56, t=80, b=56),
        )
        save(fig, "backend_resource.html")

    (out / "index.html").write_text(
        """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <title>论文实验图表</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto; color: #0f172a; }
    a { color: #2563eb; }
    li { margin: 0.5rem 0; }
  </style>
</head>
<body>
  <h1>论文实验图表 (Plotly)</h1>
  <p>由 <code>samples/render_thesis_figures.py</code> 生成；可用 Docker nginx 挂载本目录浏览。</p>
  <ul>
    <li><a href="parallelism_vs_perf.html">并行度 / 吞吐与延迟</a></li>
    <li><a href="scalability_throughput.html">规模与吞吐</a></li>
    <li><a href="window_vs_latency.html">时间窗口与延迟</a></li>
    <li><a href="backend_resource.html">状态后端资源</a></li>
  </ul>
</body>
</html>""",
        encoding="utf-8",
    )
    print(f"wrote figures to {out.resolve()}")


if __name__ == "__main__":
    main()
