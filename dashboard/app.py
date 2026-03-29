"""
Enterprise ETL Platform - Streamlit Dashboard

Multi-page dashboard providing real-time visibility into:
- Platform health and KPIs
- Pipeline run history and status
- Data quality metrics and trends
- Execution timeline charts
"""
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Configuration ─────────────────────────────────────────────────────────────
API_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="ETL Platform Dashboard",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #0f2540 100%);
        border-radius: 12px;
        padding: 20px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    .metric-value { font-size: 2.2rem; font-weight: 700; margin: 4px 0; }
    .metric-label { font-size: 0.85rem; opacity: 0.75; letter-spacing: 0.05em; text-transform: uppercase; }
    .status-success { color: #4CAF50; font-weight: 600; }
    .status-failed  { color: #f44336; font-weight: 600; }
    .status-running { color: #2196F3; font-weight: 600; }
    .status-pending { color: #FF9800; font-weight: 600; }
    .stMetric > div { background: #1a1a2e; border-radius: 8px; padding: 10px; }
    h1, h2, h3 { color: #e0e6f0; }
    .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)


# ── API Client ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def fetch_summary() -> Dict[str, Any]:
    try:
        r = httpx.get(f"{API_URL}/api/v1/metrics/summary", timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


@st.cache_data(ttl=30)
def fetch_runs(pipeline_name: Optional[str] = None, limit: int = 100) -> List[Dict]:
    try:
        params = {"limit": limit}
        if pipeline_name:
            params["pipeline_name"] = pipeline_name
        r = httpx.get(f"{API_URL}/api/v1/pipelines/runs", params=params, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


@st.cache_data(ttl=30)
def fetch_pipeline_metrics() -> List[Dict]:
    try:
        r = httpx.get(f"{API_URL}/api/v1/metrics/by-pipeline", timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


@st.cache_data(ttl=60)
def fetch_trend(days: int = 30) -> List[Dict]:
    try:
        r = httpx.get(f"{API_URL}/api/v1/metrics/trend", params={"days": days}, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


@st.cache_data(ttl=30)
def fetch_quality_metrics(pipeline_name: Optional[str] = None) -> List[Dict]:
    try:
        params = {"limit": 200}
        if pipeline_name:
            params["pipeline_name"] = pipeline_name
        r = httpx.get(f"{API_URL}/api/v1/metrics/quality", params=params, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


@st.cache_data(ttl=60)
def fetch_pipelines() -> List[str]:
    try:
        r = httpx.get(f"{API_URL}/api/v1/pipelines/list", timeout=8)
        r.raise_for_status()
        return r.json().get("pipelines", [])
    except Exception:
        return []


def trigger_pipeline(pipeline_name: str) -> Dict[str, Any]:
    try:
        r = httpx.post(
            f"{API_URL}/api/v1/pipelines/trigger",
            json={"pipeline_name": pipeline_name, "triggered_by": "dashboard"},
            timeout=300,
        )
        r.raise_for_status()
        return {"success": True, "data": r.json()}
    except httpx.HTTPStatusError as e:
        return {"success": False, "error": e.response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def status_badge(status: str) -> str:
    css_class = f"status-{status}"
    icons = {
        "success": "✅", "failed": "❌", "running": "🔄",
        "pending": "⏳", "retrying": "🔁",
    }
    icon = icons.get(status, "❓")
    return f'<span class="{css_class}">{icon} {status.title()}</span>'


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/pipeline.png", width=64)
    st.title("ETL Platform")
    st.caption("v1.0.0 | Enterprise Edition")
    st.divider()

    page = st.radio(
        "Navigation",
        ["🏠 Overview", "📋 Pipeline Runs", "🔬 Data Quality", "🚀 Trigger Pipeline"],
        label_visibility="collapsed",
    )

    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")


# ── Page: Overview ────────────────────────────────────────────────────────────
if page == "🏠 Overview":
    st.title("⚙️ ETL Platform Overview")

    summary = fetch_summary()
    trend_data = fetch_trend(days=30)

    # KPI Row
    col1, col2, col3, col4, col5 = st.columns(5)
    metrics = [
        (col1, "Total Runs", summary.get("total_runs", 0), None),
        (col2, "Successful", summary.get("successful_runs", 0), None),
        (col3, "Failed", summary.get("failed_runs", 0), None),
        (col4, "Success Rate", f"{summary.get('success_rate_pct', 0):.1f}%", None),
        (col5, "Records Loaded", f"{summary.get('total_records_loaded', 0):,}", None),
    ]
    for col, label, value, delta in metrics:
        col.metric(label=label, value=value, delta=delta)

    col_a, col_b = st.columns(2)
    col_a.metric("Avg Quality Score", f"{summary.get('avg_quality_score', 0):.1f}%")
    col_b.metric("Avg Duration", f"{summary.get('avg_duration_seconds', 0):.1f}s")

    st.divider()

    # Pipeline performance table
    st.subheader("📊 Pipeline Performance")
    pipeline_metrics = fetch_pipeline_metrics()
    if pipeline_metrics:
        df_pm = pd.DataFrame(pipeline_metrics)
        if not df_pm.empty:
            df_display = df_pm[[
                "pipeline_name", "total_runs", "successful_runs", "failed_runs",
                "success_rate_pct", "avg_quality_score", "total_records_loaded", "last_run",
            ]].rename(columns={
                "pipeline_name": "Pipeline",
                "total_runs": "Total Runs",
                "successful_runs": "✅ Success",
                "failed_runs": "❌ Failed",
                "success_rate_pct": "Success %",
                "avg_quality_score": "Avg Quality",
                "total_records_loaded": "Records Loaded",
                "last_run": "Last Run",
            })
            st.dataframe(df_display, use_container_width=True, hide_index=True)

    st.divider()

    # Trend Charts
    col_trend1, col_trend2 = st.columns(2)

    with col_trend1:
        st.subheader("📈 Daily Run Trend (30 days)")
        if trend_data:
            df_trend = pd.DataFrame(trend_data)
            df_trend["date"] = pd.to_datetime(df_trend["date"])
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_trend["date"], y=df_trend["successful"],
                name="Successful", marker_color="#4CAF50"
            ))
            fig.add_trace(go.Bar(
                x=df_trend["date"], y=df_trend["failed"],
                name="Failed", marker_color="#f44336"
            ))
            fig.update_layout(
                barmode="stack",
                plot_bgcolor="#0f1117",
                paper_bgcolor="#0f1117",
                font_color="#e0e0e0",
                legend=dict(orientation="h"),
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data available yet.")

    with col_trend2:
        st.subheader("🎯 Quality Score Trend")
        if trend_data:
            df_trend = pd.DataFrame(trend_data)
            df_trend["date"] = pd.to_datetime(df_trend["date"])
            df_trend = df_trend[df_trend["avg_quality"] > 0]
            if not df_trend.empty:
                fig2 = px.line(
                    df_trend, x="date", y="avg_quality",
                    labels={"avg_quality": "Avg Quality Score", "date": "Date"},
                    color_discrete_sequence=["#2196F3"],
                )
                fig2.add_hline(
                    y=80, line_dash="dash", line_color="orange",
                    annotation_text="Threshold (80%)"
                )
                fig2.update_layout(
                    plot_bgcolor="#0f1117",
                    paper_bgcolor="#0f1117",
                    font_color="#e0e0e0",
                    height=300,
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No quality data available yet.")


# ── Page: Pipeline Runs ───────────────────────────────────────────────────────
elif page == "📋 Pipeline Runs":
    st.title("📋 Pipeline Run History")

    pipelines = fetch_pipelines()
    col_f1, col_f2 = st.columns([2, 1])

    with col_f1:
        filter_pipeline = st.selectbox(
            "Filter by Pipeline", options=["All"] + pipelines
        )
    with col_f2:
        filter_limit = st.selectbox("Show", options=[25, 50, 100], index=1)

    pipeline_filter = None if filter_pipeline == "All" else filter_pipeline
    runs = fetch_runs(pipeline_name=pipeline_filter, limit=filter_limit)

    if runs:
        df_runs = pd.DataFrame(runs)

        # Status distribution
        status_counts = df_runs["status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        color_map = {
            "success": "#4CAF50", "failed": "#f44336",
            "running": "#2196F3", "pending": "#FF9800",
        }

        col_pie, col_stats = st.columns([1, 2])
        with col_pie:
            fig_pie = px.pie(
                status_counts, values="Count", names="Status",
                color="Status",
                color_discrete_map=color_map,
                hole=0.45,
            )
            fig_pie.update_layout(
                plot_bgcolor="#0f1117", paper_bgcolor="#0f1117",
                font_color="#e0e0e0", height=260,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(orientation="h"),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_stats:
            st.metric("Showing", f"{len(df_runs)} runs")
            if "duration_seconds" in df_runs:
                valid = df_runs[df_runs["duration_seconds"].notna()]
                if not valid.empty:
                    st.metric(
                        "Avg Duration",
                        f"{valid['duration_seconds'].mean():.1f}s"
                    )
            if "records_loaded" in df_runs:
                st.metric("Total Records", f"{df_runs['records_loaded'].sum():,.0f}")

        st.divider()
        st.subheader("Run Details")

        display_cols = [
            "pipeline_name", "status", "records_loaded",
            "quality_score", "duration_seconds", "started_at", "triggered_by",
        ]
        available_cols = [c for c in display_cols if c in df_runs.columns]
        df_display = df_runs[available_cols].copy()

        if "started_at" in df_display:
            df_display["started_at"] = pd.to_datetime(
                df_display["started_at"]
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

        st.dataframe(df_display, use_container_width=True, hide_index=True)
    else:
        st.info("No pipeline runs found. Trigger a pipeline to get started.")


# ── Page: Data Quality ────────────────────────────────────────────────────────
elif page == "🔬 Data Quality":
    st.title("🔬 Data Quality Analytics")

    pipelines = fetch_pipelines()
    filter_pipeline = st.selectbox(
        "Select Pipeline", options=["All"] + pipelines
    )
    pipeline_filter = None if filter_pipeline == "All" else filter_pipeline

    quality_data = fetch_quality_metrics(pipeline_name=pipeline_filter)

    if quality_data:
        df_q = pd.DataFrame(quality_data)

        # Pass/Fail summary
        pass_count = df_q["passed"].sum()
        fail_count = (~df_q["passed"]).sum()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Checks", len(df_q))
        col2.metric("✅ Passed", int(pass_count))
        col3.metric("❌ Failed", int(fail_count))

        st.divider()

        # Category breakdown
        col_cat, col_trend = st.columns(2)
        with col_cat:
            st.subheader("Quality by Category")
            if "metric_category" in df_q.columns:
                cat_summary = df_q.groupby("metric_category")["passed"].agg(
                    ["sum", "count"]
                ).reset_index()
                cat_summary.columns = ["Category", "Passed", "Total"]
                cat_summary["Pass Rate %"] = (
                    cat_summary["Passed"] / cat_summary["Total"] * 100
                ).round(1)

                fig_cat = px.bar(
                    cat_summary, x="Category", y="Pass Rate %",
                    color="Pass Rate %",
                    color_continuous_scale=["#f44336", "#FF9800", "#4CAF50"],
                    range_color=[0, 100],
                )
                fig_cat.update_layout(
                    plot_bgcolor="#0f1117", paper_bgcolor="#0f1117",
                    font_color="#e0e0e0", height=300,
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig_cat, use_container_width=True)

        with col_trend:
            st.subheader("Failed Checks by Type")
            failed = df_q[~df_q["passed"]]
            if not failed.empty:
                fail_counts = failed["metric_name"].value_counts().head(10)
                fig_fails = px.bar(
                    x=fail_counts.values,
                    y=fail_counts.index,
                    orientation="h",
                    color=fail_counts.values,
                    color_continuous_scale=["#FF9800", "#f44336"],
                    labels={"x": "Failures", "y": "Check Name"},
                )
                fig_fails.update_layout(
                    plot_bgcolor="#0f1117", paper_bgcolor="#0f1117",
                    font_color="#e0e0e0", height=300,
                    margin=dict(l=0, r=0, t=10, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig_fails, use_container_width=True)
            else:
                st.success("🎉 All quality checks passed!")

        st.divider()
        st.subheader("Quality Check Details")
        display = df_q[["pipeline_name", "metric_name", "metric_category", "passed",
                         "actual_value", "threshold", "evaluated_at"]].copy()
        st.dataframe(display, use_container_width=True, hide_index=True)

    else:
        st.info("No quality metrics available yet. Run a pipeline to generate data.")


# ── Page: Trigger Pipeline ────────────────────────────────────────────────────
elif page == "🚀 Trigger Pipeline":
    st.title("🚀 Trigger Pipeline")
    st.caption("Manually trigger a pipeline run directly from the dashboard.")

    pipelines = fetch_pipelines()

    if not pipelines:
        st.warning("No pipeline configurations found. Add YAML files to configs/pipelines/")
    else:
        with st.form("trigger_form"):
            selected = st.selectbox("Select Pipeline", options=pipelines)
            triggered_by = st.text_input("Triggered By", value="dashboard-user")
            st.caption("⚠️ This will execute the pipeline synchronously. Large pipelines may take minutes.")
            submitted = st.form_submit_button("▶️ Run Pipeline", type="primary")

        if submitted:
            with st.spinner(f"Running pipeline: {selected}..."):
                result = trigger_pipeline(selected)

            if result["success"]:
                data = result["data"]
                st.success(f"✅ Pipeline completed! Run ID: `{data.get('run_id')}`")
                st.json(data)
            else:
                st.error(f"❌ Pipeline failed: {result.get('error')}")
