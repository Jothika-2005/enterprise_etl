"""
Enterprise ETL Platform - Visualization / Chart Library

All Plotly figures used across dashboard pages are built here.
Each function returns a go.Figure ready to pass to st.plotly_chart().
"""
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Shared theme ──────────────────────────────────────────────────────────────
DARK_BG    = "#0f1117"
CARD_BG    = "#1a1f2e"
ACCENT     = "#4f8ef7"
GREEN      = "#22c55e"
RED        = "#ef4444"
ORANGE     = "#f59e0b"
PURPLE     = "#a855f7"
FONT_COLOR = "#e2e8f0"
GRID_COLOR = "#1e293b"

LAYOUT_BASE = dict(
    plot_bgcolor=DARK_BG,
    paper_bgcolor=DARK_BG,
    font=dict(color=FONT_COLOR, family="Inter, sans-serif", size=12),
    margin=dict(l=10, r=10, t=36, b=10),
    xaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR),
    yaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
)


def _apply_base(fig: go.Figure, title: str = "", height: int = 340) -> go.Figure:
    fig.update_layout(**LAYOUT_BASE, title=dict(text=title, font=dict(size=14)), height=height)
    return fig


# ── Quality gauge ─────────────────────────────────────────────────────────────
def quality_gauge(score: float, title: str = "Data Quality Score") -> go.Figure:
    """Animated gauge chart for the 0–100 quality score."""
    color = GREEN if score >= 75 else (ORANGE if score >= 50 else RED)
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        delta={"reference": 75, "increasing": {"color": GREEN}, "decreasing": {"color": RED}},
        title={"text": title, "font": {"size": 16, "color": FONT_COLOR}},
        number={"suffix": " / 100", "font": {"size": 28, "color": color}},
        gauge={
            "axis": {"range": [0, 100], "tickcolor": FONT_COLOR, "tickfont": {"size": 10}},
            "bar": {"color": color, "thickness": 0.26},
            "bgcolor": CARD_BG,
            "borderwidth": 0,
            "steps": [
                {"range": [0, 40],  "color": "#2d1a1a"},
                {"range": [40, 75], "color": "#2d2510"},
                {"range": [75, 100],"color": "#1a2d1a"},
            ],
            "threshold": {
                "line": {"color": "white", "width": 2},
                "thickness": 0.75,
                "value": 75,
            },
        },
    ))
    fig.update_layout(
        paper_bgcolor=DARK_BG,
        font=dict(color=FONT_COLOR),
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    return fig


# ── Dimension radar / spider chart ────────────────────────────────────────────
def quality_radar(dimension_scores: Dict[str, float]) -> go.Figure:
    """Spider/radar chart for 5 quality dimensions."""
    dims  = list(dimension_scores.keys())
    vals  = list(dimension_scores.values())
    dims += [dims[0]]   # close polygon
    vals += [vals[0]]

    fig = go.Figure(go.Scatterpolar(
        r=vals, theta=dims,
        fill="toself",
        fillcolor="rgba(79, 142, 247, 0.2)",
        line=dict(color=ACCENT, width=2),
        marker=dict(size=6, color=ACCENT),
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(color=FONT_COLOR, size=9),
                            gridcolor=GRID_COLOR, linecolor=GRID_COLOR),
            angularaxis=dict(tickfont=dict(color=FONT_COLOR, size=10)),
            bgcolor=DARK_BG,
        ),
        paper_bgcolor=DARK_BG,
        showlegend=False,
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
    )
    return fig


# ── Null heatmap ──────────────────────────────────────────────────────────────
def null_heatmap(df: pd.DataFrame, max_cols: int = 30) -> go.Figure:
    """Heatmap showing missing values per column."""
    cols  = df.columns[:max_cols]
    nulls = df[cols].isnull().astype(int)

    # Sample rows for display performance
    sample = nulls.sample(min(200, len(nulls)), random_state=42) if len(nulls) > 200 else nulls

    fig = go.Figure(go.Heatmap(
        z=sample.values,
        x=list(sample.columns),
        y=[f"row {i}" for i in sample.index],
        colorscale=[[0, CARD_BG], [1, RED]],
        showscale=True,
        colorbar=dict(
            title="Missing", tickvals=[0, 1], ticktext=["Present", "Missing"],
            titlefont=dict(color=FONT_COLOR), tickfont=dict(color=FONT_COLOR),
        ),
    ))
    return _apply_base(fig, "🔍 Missing Values Heatmap", height=320)


# ── Column null bar chart ─────────────────────────────────────────────────────
def null_bar_chart(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar — null % per column, sorted descending."""
    null_pct = (df.isnull().mean() * 100).sort_values(ascending=True)
    null_pct = null_pct[null_pct > 0]  # only cols with nulls

    if null_pct.empty:
        fig = go.Figure()
        fig.add_annotation(text="✅ No missing values!", showarrow=False,
                           font=dict(size=16, color=GREEN), xref="paper", yref="paper",
                           x=0.5, y=0.5)
        return _apply_base(fig, "Missing Values per Column", 200)

    colors = [RED if v > 20 else ORANGE if v > 5 else ACCENT for v in null_pct.values]
    fig = go.Figure(go.Bar(
        x=null_pct.values,
        y=null_pct.index,
        orientation="h",
        marker_color=colors,
        text=[f"{v:.1f}%" for v in null_pct.values],
        textposition="outside",
        textfont=dict(color=FONT_COLOR, size=10),
    ))
    fig.update_layout(**LAYOUT_BASE,
                      title="Missing Values per Column (%)",
                      height=max(200, 30 * len(null_pct) + 80),
                      xaxis=dict(title="Null %", range=[0, 105]),
                      margin=dict(l=10, r=60, t=36, b=10))
    return fig


# ── Duplicate bar ─────────────────────────────────────────────────────────────
def duplicate_summary_chart(total: int, duplicates: int) -> go.Figure:
    """Donut showing unique vs duplicate rows."""
    unique = total - duplicates
    labels = ["Unique Rows", "Duplicate Rows"]
    values = [unique, duplicates]
    colors = [GREEN, RED]
    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        marker_colors=colors,
        hole=0.6,
        textinfo="label+percent",
        textfont=dict(color=FONT_COLOR, size=11),
    ))
    fig.update_layout(
        paper_bgcolor=DARK_BG,
        showlegend=True,
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=FONT_COLOR)),
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
        annotations=[dict(text=f"{total:,}<br>rows", x=0.5, y=0.5,
                          font=dict(size=14, color=FONT_COLOR), showarrow=False)],
    )
    return fig


# ── Column data types distribution ────────────────────────────────────────────
def dtype_distribution_chart(df: pd.DataFrame) -> go.Figure:
    """Pie of column data types."""
    dtype_counts: Dict[str, int] = {}
    for dtype in df.dtypes:
        key = str(dtype)
        if "int" in key or "float" in key:
            group = "Numeric"
        elif "object" in key or "string" in key:
            group = "Text"
        elif "datetime" in key:
            group = "DateTime"
        elif "bool" in key:
            group = "Boolean"
        else:
            group = "Other"
        dtype_counts[group] = dtype_counts.get(group, 0) + 1

    colors_map = {"Numeric": ACCENT, "Text": PURPLE, "DateTime": GREEN,
                  "Boolean": ORANGE, "Other": "#64748b"}
    labels = list(dtype_counts.keys())
    values = list(dtype_counts.values())
    colors_list = [colors_map.get(l, ACCENT) for l in labels]

    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        marker_colors=colors_list,
        hole=0.5,
        textinfo="label+value",
        textfont=dict(color=FONT_COLOR, size=11),
    ))
    fig.update_layout(
        paper_bgcolor=DARK_BG,
        showlegend=False,
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    return fig


# ── Numeric column distribution ───────────────────────────────────────────────
def column_histogram(df: pd.DataFrame, column: str) -> go.Figure:
    """Histogram for a single numeric column."""
    data = df[column].dropna()
    fig = go.Figure(go.Histogram(
        x=data,
        nbinsx=40,
        marker_color=ACCENT,
        marker_line=dict(width=0.5, color=DARK_BG),
        opacity=0.85,
    ))
    # Add mean/median lines
    fig.add_vline(x=float(data.mean()), line_dash="dash", line_color=GREEN,
                  annotation_text=f"Mean: {data.mean():.2f}", annotation_font_color=GREEN)
    fig.add_vline(x=float(data.median()), line_dash="dot", line_color=ORANGE,
                  annotation_text=f"Median: {data.median():.2f}", annotation_font_color=ORANGE)
    return _apply_base(fig, f"Distribution: {column}", 300)


# ── Pipeline run history ──────────────────────────────────────────────────────
def pipeline_run_chart(runs_df: pd.DataFrame) -> go.Figure:
    """Stacked bar — success/failed runs per day."""
    if runs_df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No pipeline run data yet.", showarrow=False,
                           font=dict(size=14, color=FONT_COLOR), x=0.5, y=0.5,
                           xref="paper", yref="paper")
        return _apply_base(fig, "Pipeline Runs (Daily)", 280)

    df = runs_df.copy()
    if "started_at" in df.columns:
        df["date"] = pd.to_datetime(df["started_at"]).dt.date
        daily = df.groupby(["date", "status"]).size().unstack(fill_value=0).reset_index()
    else:
        return _apply_base(go.Figure(), "Pipeline Runs", 280)

    fig = go.Figure()
    if "success" in daily.columns:
        fig.add_trace(go.Bar(x=daily["date"], y=daily["success"],
                              name="Success", marker_color=GREEN))
    if "failed" in daily.columns:
        fig.add_trace(go.Bar(x=daily["date"], y=daily["failed"],
                              name="Failed", marker_color=RED))
    fig.update_layout(**LAYOUT_BASE, barmode="stack",
                      title="Daily Pipeline Runs", height=300)
    return fig


# ── ETL execution timeline (Gantt-style) ─────────────────────────────────────
def execution_timeline(runs_df: pd.DataFrame) -> go.Figure:
    """Horizontal Gantt-style timeline of recent pipeline runs."""
    if runs_df.empty or "started_at" not in runs_df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No run data for timeline.", showarrow=False,
                           font=dict(size=14, color=FONT_COLOR), x=0.5, y=0.5,
                           xref="paper", yref="paper")
        return _apply_base(fig, "ETL Execution Timeline", 300)

    df = runs_df.head(20).copy()
    df["started_at"] = pd.to_datetime(df["started_at"])
    df["finished_at"] = pd.to_datetime(df.get("finished_at", df["started_at"]))
    df["finished_at"] = df["finished_at"].fillna(df["started_at"] + pd.Timedelta(seconds=10))
    df["label"] = df["pipeline_name"] + " [" + df["status"] + "]"

    color_map = {"success": GREEN, "failed": RED, "running": ACCENT, "pending": ORANGE}
    fig = go.Figure()
    for _, row in df.iterrows():
        c = color_map.get(str(row.get("status", "")), "#64748b")
        fig.add_trace(go.Scatter(
            x=[row["started_at"], row["finished_at"]],
            y=[row["label"], row["label"]],
            mode="lines",
            line=dict(color=c, width=14),
            name=str(row.get("status", "")),
            showlegend=False,
            hovertemplate=(
                f"<b>{row.get('pipeline_name','')}</b><br>"
                f"Status: {row.get('status','')}<br>"
                f"Records: {row.get('records_loaded', 'N/A')}<extra></extra>"
            ),
        ))
    return _apply_base(fig, "ETL Execution Timeline (Latest 20 Runs)", 400)


# ── Cleaning before/after summary ────────────────────────────────────────────
def cleaning_before_after(original_rows: int, cleaned_rows: int,
                            nulls_filled: int, dups_removed: int) -> go.Figure:
    """4-bar grouped comparison: original vs cleaned metrics."""
    categories = ["Total Rows", "Rows After Clean", "Nulls Filled", "Duplicates Removed"]
    original_vals = [original_rows, original_rows, 0, 0]
    cleaned_vals  = [original_rows, cleaned_rows, nulls_filled, dups_removed]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Before", x=categories, y=original_vals, marker_color="#334155",
        text=original_vals, textposition="outside", textfont=dict(color=FONT_COLOR, size=10)
    ))
    fig.add_trace(go.Bar(
        name="After", x=categories, y=cleaned_vals, marker_color=ACCENT,
        text=cleaned_vals, textposition="outside", textfont=dict(color=FONT_COLOR, size=10)
    ))
    fig.update_layout(**LAYOUT_BASE, barmode="group",
                      title="Cleaning Impact Summary", height=320)
    return fig
