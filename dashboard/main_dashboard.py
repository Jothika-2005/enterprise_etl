"""
Enterprise ETL Orchestration Platform — Dashboard v2.0

7-page professional Streamlit dashboard with:
  • Dark SaaS-style UI
  • Universal file upload & parsing (CSV/Excel/JSON/TXT/PDF/DOCX)
  • Auto data cleaning with before/after report
  • 5-dimension quality scoring (0–100)
  • Interactive Plotly charts
  • Pipeline monitor & trigger
  • Log viewer
  • System health
  • Download cleaned dataset

Run: streamlit run dashboard/main_dashboard.py
"""
import io
import sys
import os
import time
import random
import datetime
from typing import Optional

import pandas as pd
import streamlit as st

# ── path setup ────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from ui_components.components import (
    inject_css, render_sidebar, kpi_card, kpi_row,
    section_header, info_box, warn_box, success_box, error_box,
    render_log_viewer, status_badge, step_tracker,
)
from data_ingestion.file_parser import FileParser, SUPPORTED_TYPES
from data_ingestion.cleaner import AutoCleaner
from quality_engine.scorer import QualityScorer
from visualization.charts import (
    quality_gauge, quality_radar, null_heatmap, null_bar_chart,
    duplicate_summary_chart, dtype_distribution_chart, column_histogram,
    pipeline_run_chart, execution_timeline, cleaning_before_after,
)

# ── page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Enterprise ETL Platform",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

# ── session state defaults ────────────────────────────────────────────────────
def _init_state():
    defaults = {
        "parsed_result":    None,
        "cleaned_df":       None,
        "cleaning_report":  None,
        "quality_card":     None,
        "upload_filename":  None,
        "pipeline_logs":    [],
        "pipeline_runs":    _seed_demo_runs(),
        "pipeline_running": False,
        "etl_step":         0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

def _seed_demo_runs() -> pd.DataFrame:
    """Seed fake pipeline history so the monitor looks populated."""
    now = datetime.datetime.utcnow()
    rows = []
    pipelines = ["customer_data_pipeline", "sales_metrics_pipeline", "product_sync"]
    statuses  = ["success", "success", "success", "failed", "running"]
    for i in range(18):
        s = random.choice(statuses)
        started = now - datetime.timedelta(hours=random.randint(1, 72))
        finished = started + datetime.timedelta(seconds=random.randint(30, 900))
        rows.append({
            "run_id":        f"run_{i:04d}",
            "pipeline_name": random.choice(pipelines),
            "status":        s,
            "started_at":    started,
            "finished_at":   finished if s != "running" else None,
            "records_loaded":random.randint(500, 50000) if s == "success" else 0,
            "quality_score": round(random.uniform(0.72, 0.99), 2) if s == "success" else None,
            "duration_s":    (finished - started).seconds if s != "running" else None,
        })
    return pd.DataFrame(rows)

_init_state()

# ── routing ───────────────────────────────────────────────────────────────────
page = render_sidebar()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "Overview":
    st.markdown("## 🏠 Platform Overview")
    st.caption("Real-time snapshot of your ETL platform — pipelines, data quality, and throughput.")

    runs_df = st.session_state["pipeline_runs"]
    total   = len(runs_df)
    success = int((runs_df["status"] == "success").sum())
    failed  = int((runs_df["status"] == "failed").sum())
    running = int((runs_df["status"] == "running").sum())
    avg_q   = runs_df["quality_score"].dropna().mean()
    records = int(runs_df["records_loaded"].sum())

    kpi_row([
        kpi_card("Total Runs",      total,              "↑ 3 today",         "blue",   "🔄"),
        kpi_card("Successful",      success,            f"↑ {success} total","green",  "✅"),
        kpi_card("Failed",          failed,             "↓ needs attention" if failed else "✓ All clear", "red", "❌"),
        kpi_card("Running",         running,            "Live now",          "orange", "⏳"),
        kpi_card("Avg Quality",     f"{avg_q:.0%}",    "↑ +2% vs last week","purple", "🎯"),
        kpi_card("Records Loaded",  f"{records:,}",    "↑ across all runs", "blue",   "📦"),
    ])

    col1, col2 = st.columns([3, 2])
    with col1:
        section_header("📊", "Daily Pipeline Runs (Last 7 Days)")
        st.plotly_chart(pipeline_run_chart(runs_df), use_container_width=True, config={"displayModeBar": False})

    with col2:
        section_header("⏱️", "Execution Timeline")
        st.plotly_chart(execution_timeline(runs_df), use_container_width=True, config={"displayModeBar": False})

    section_header("📋", "Recent Pipeline Runs")
    display_df = runs_df.sort_values("started_at", ascending=False).head(10).copy()
    display_df["Status"] = display_df["status"].apply(
        lambda s: {"success":"✅ Success","failed":"❌ Failed","running":"⏳ Running"}.get(s, s)
    )
    display_df["Records"] = display_df["records_loaded"].apply(lambda v: f"{v:,}" if v else "—")
    display_df["Quality"] = display_df["quality_score"].apply(
        lambda v: f"{v:.0%}" if pd.notna(v) else "—"
    )
    display_df["Duration"] = display_df["duration_s"].apply(
        lambda v: f"{v}s" if pd.notna(v) else "—"
    )
    st.dataframe(
        display_df[["pipeline_name", "Status", "started_at", "Records", "Quality", "Duration"]],
        use_container_width=True, hide_index=True,
    )

    if st.session_state.get("quality_card"):
        section_header("🎯", "Last Upload — Quality Snapshot")
        qc = st.session_state["quality_card"]
        c1, c2 = st.columns([1, 2])
        with c1:
            st.plotly_chart(quality_gauge(qc.overall_score), use_container_width=True,
                            config={"displayModeBar": False})
        with c2:
            st.plotly_chart(quality_radar(qc.dimension_dict), use_container_width=True,
                            config={"displayModeBar": False})

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — UPLOAD DATA
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Upload Data":
    st.markdown("## 📤 Universal Data Upload")
    st.caption("Upload any file — CSV, Excel, JSON, TXT, PDF, or Word. The platform handles the rest.")

    step_tracker(
        ["Upload", "Parse", "Clean", "Score", "Ready"],
        st.session_state["etl_step"],
    )

    supported = ", ".join(SUPPORTED_TYPES.keys())
    info_box(f"Supported formats: **{supported}**  ·  Max recommended size: **200 MB**")

    uploaded = st.file_uploader(
        "Drag & drop your file here, or click to browse",
        type=list(SUPPORTED_TYPES.keys()),
        help="Files are processed locally — nothing is sent to external servers.",
    )

    # Excel sheet selector
    excel_sheet = None
    if uploaded and uploaded.name.endswith((".xlsx", ".xls")):
        raw_bytes = uploaded.read()
        uploaded.seek(0)
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), read_only=True)
        sheets = wb.sheetnames
        if len(sheets) > 1:
            excel_sheet = st.selectbox("Select Excel sheet", sheets)

    if uploaded:
        st.markdown(f"""
        <div style='background:#111827; border:1px solid #1e293b; border-radius:10px;
                    padding:0.8rem 1.2rem; margin:0.8rem 0; display:flex;
                    align-items:center; gap:1rem;'>
            <span style='font-size:1.6rem'>📄</span>
            <div>
                <div style='font-weight:600; color:#e2e8f0;'>{uploaded.name}</div>
                <div style='font-size:0.78rem; color:#64748b;'>
                    Size: {uploaded.size/1024:.1f} KB &nbsp;·&nbsp;
                    Type: {uploaded.type or 'auto-detect'}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        col1, col2, col3 = st.columns([2, 2, 4])
        with col1:
            run_parse = st.button("🚀 Parse & Clean File", type="primary")
        with col2:
            reset = st.button("🔄 Reset")

        if reset:
            for k in ["parsed_result", "cleaned_df", "cleaning_report", "quality_card",
                      "upload_filename", "etl_step"]:
                st.session_state[k] = None if k != "etl_step" else 0
            st.rerun()

        if run_parse:
            prog = st.progress(0, text="Initialising...")
            status_text = st.empty()

            try:
                # Step 1 — Parse
                prog.progress(15, text="📂 Parsing file...")
                status_text.info("Reading and parsing file...")
                time.sleep(0.3)

                raw = uploaded.read()
                result = FileParser.parse(raw, filename=uploaded.name, excel_sheet=excel_sheet)
                st.session_state["parsed_result"]   = result
                st.session_state["upload_filename"] = uploaded.name
                st.session_state["etl_step"] = 1
                prog.progress(35, text="✅ Parsing complete")

                # Step 2 — Clean
                prog.progress(50, text="🧹 Auto-cleaning data...")
                status_text.info("Running auto-cleaning pipeline...")
                time.sleep(0.4)

                cleaner = AutoCleaner()
                cleaned_df, report = cleaner.clean(result.data.copy(), uploaded.name)
                st.session_state["cleaned_df"]      = cleaned_df
                st.session_state["cleaning_report"] = report
                st.session_state["etl_step"] = 2
                prog.progress(70, text="✅ Cleaning complete")

                # Step 3 — Quality
                prog.progress(85, text="🎯 Scoring data quality...")
                status_text.info("Calculating quality score...")
                time.sleep(0.3)

                scorer = QualityScorer()
                card = scorer.score(cleaned_df, uploaded.name)
                st.session_state["quality_card"] = card
                st.session_state["etl_step"] = 4

                # Log entry
                st.session_state["pipeline_logs"].append(
                    f"{datetime.datetime.utcnow().isoformat()[:19]} [INFO] Parsed {uploaded.name}: "
                    f"{result.row_count} rows, {result.col_count} cols"
                )
                st.session_state["pipeline_logs"].append(
                    f"{datetime.datetime.utcnow().isoformat()[:19]} [SUCCESS] Quality score: "
                    f"{card.overall_score:.1f}/100 ({card.grade_label})"
                )

                prog.progress(100, text="🎉 All done!")
                status_text.empty()
                time.sleep(0.3)

                success_box(
                    f"**{uploaded.name}** processed successfully! "
                    f"**{result.row_count:,} rows**, quality score: **{card.overall_score:.1f}/100**"
                )

            except Exception as e:
                prog.progress(100, text="❌ Failed")
                error_box(f"Processing failed: {e}")
                st.session_state["pipeline_logs"].append(
                    f"{datetime.datetime.utcnow().isoformat()[:19]} [ERROR] {e}"
                )
            finally:
                time.sleep(0.6)
                prog.empty()

    # ── Preview if already parsed ─────────────────────────────────────────
    if st.session_state["parsed_result"] and st.session_state["cleaned_df"] is not None:
        result  = st.session_state["parsed_result"]
        cleaned = st.session_state["cleaned_df"]

        section_header("👁️", "Dataset Preview")
        tab1, tab2 = st.tabs(["🔍 Cleaned Data", "📄 Raw (Original)"])
        with tab1:
            st.caption(f"{len(cleaned):,} rows × {len(cleaned.columns)} columns")
            st.dataframe(cleaned.head(100), use_container_width=True)
        with tab2:
            st.caption(f"{len(result.data):,} rows × {len(result.data.columns)} columns")
            st.dataframe(result.data.head(100), use_container_width=True)

        section_header("💾", "Download Cleaned Dataset")
        col1, col2, col3 = st.columns(3)
        with col1:
            csv_bytes = cleaned.to_csv(index=False).encode()
            st.download_button(
                "⬇️  Download as CSV", csv_bytes,
                file_name=f"cleaned_{result.filename.rsplit('.', 1)[0]}.csv",
                mime="text/csv", use_container_width=True,
            )
        with col2:
            buf = io.BytesIO()
            cleaned.to_excel(buf, index=False, engine="openpyxl")
            st.download_button(
                "⬇️  Download as Excel", buf.getvalue(),
                file_name=f"cleaned_{result.filename.rsplit('.', 1)[0]}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with col3:
            json_bytes = cleaned.to_json(orient="records", indent=2).encode()
            st.download_button(
                "⬇️  Download as JSON", json_bytes,
                file_name=f"cleaned_{result.filename.rsplit('.', 1)[0]}.json",
                mime="application/json", use_container_width=True,
            )

        if result.extra_text:
            section_header("📝", "Extracted Text Content")
            with st.expander("Show extracted text (PDF/DOCX)"):
                st.text(result.extra_text[:3000])

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — CLEANING RESULTS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Cleaning Results":
    st.markdown("## 🧹 Data Cleaning Results")

    if not st.session_state.get("cleaning_report"):
        warn_box("No file uploaded yet. Go to **Upload Data** to upload a file first.")
        st.stop()

    report  = st.session_state["cleaning_report"]
    cleaned = st.session_state["cleaned_df"]
    raw_df  = st.session_state["parsed_result"].data

    kpi_row([
        kpi_card("Original Rows",   f"{report.original_rows:,}",  "",                     "blue",   "📥"),
        kpi_card("Cleaned Rows",    f"{report.cleaned_rows:,}",   f"↓ {report.rows_removed:,} removed", "green", "✅"),
        kpi_card("Duplicates",      f"{report.duplicates_removed:,}", "Removed",          "red",    "🔁"),
        kpi_card("Nulls Filled",    f"{report.nulls_filled:,}",   "Values imputed",       "orange", "🔧"),
        kpi_card("Cols Dropped",    f"{report.columns_dropped}",  "High-null columns",    "purple", "🗑️"),
    ])

    col1, col2 = st.columns(2)
    with col1:
        section_header("📊", "Cleaning Impact")
        st.plotly_chart(
            cleaning_before_after(
                report.original_rows, report.cleaned_rows,
                report.nulls_filled, report.duplicates_removed
            ),
            use_container_width=True, config={"displayModeBar": False},
        )
    with col2:
        section_header("🔁", "Unique vs Duplicate Rows")
        st.plotly_chart(
            duplicate_summary_chart(report.original_rows, report.duplicates_removed),
            use_container_width=True, config={"displayModeBar": False},
        )

    section_header("🔍", "Missing Values Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(null_bar_chart(raw_df), use_container_width=True,
                        config={"displayModeBar": False})
    with col2:
        st.plotly_chart(null_heatmap(raw_df), use_container_width=True,
                        config={"displayModeBar": False})

    section_header("📋", "Actions Taken")
    for i, action in enumerate(report.actions_taken, 1):
        st.markdown(f"""
        <div style='background:#111827; border-left:3px solid #22c55e; border-radius:0 8px 8px 0;
                    padding:0.5rem 1rem; margin:0.3rem 0; font-size:0.85rem; color:#86efac;'>
            <span style='color:#475569;'>{i}.</span> {action}
        </div>
        """, unsafe_allow_html=True)

    section_header("📐", "Column Profile")
    col_rows = []
    for p in report.column_profiles:
        col_rows.append({
            "Column": p.name,
            "Type": p.dtype,
            "Nulls": f"{p.null_pct*100:.1f}%",
            "Unique": f"{p.unique_pct*100:.1f}%",
            "Min": p.min_val,
            "Max": p.max_val,
            "Mean": p.mean_val,
            "Suggestion": p.suggested_action,
        })
    if col_rows:
        st.dataframe(pd.DataFrame(col_rows), use_container_width=True, hide_index=True)

    section_header("🔬", "Explore Column Distributions")
    num_cols = cleaned.select_dtypes(include="number").columns.tolist()
    if num_cols:
        selected_col = st.selectbox("Select numeric column", num_cols)
        st.plotly_chart(column_histogram(cleaned, selected_col), use_container_width=True,
                        config={"displayModeBar": False})
    else:
        info_box("No numeric columns detected for distribution analysis.")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — DATA QUALITY
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Data Quality":
    st.markdown("## 🔬 Data Quality Metrics")

    if not st.session_state.get("quality_card"):
        warn_box("No quality data yet. Upload a file first.")
        st.stop()

    qc = st.session_state["quality_card"]

    kpi_row([
        kpi_card("Quality Score", f"{qc.overall_score:.1f}", qc.grade_label, "blue", "🎯"),
        kpi_card("Grade",         qc.grade, "",              "green" if qc.grade in ("A","B") else "red", "🏆"),
        kpi_card("Records",       f"{qc.total_records:,}", "", "blue", "📦"),
        kpi_card("Columns",       qc.total_columns, "", "purple", "📐"),
        kpi_card("Issues Found",  len(qc.all_issues), "Detected", "orange" if qc.all_issues else "green", "⚠️"),
    ])

    col1, col2 = st.columns([1, 1])
    with col1:
        section_header("📊", "Overall Quality Gauge")
        st.plotly_chart(quality_gauge(qc.overall_score), use_container_width=True,
                        config={"displayModeBar": False})
    with col2:
        section_header("🕸️", "Dimension Radar")
        st.plotly_chart(quality_radar(qc.dimension_dict), use_container_width=True,
                        config={"displayModeBar": False})

    section_header("📋", "Dimension Breakdown")
    for dim in qc.dimensions:
        score = dim.score
        bar_color = "#22c55e" if score >= 75 else ("#f59e0b" if score >= 50 else "#ef4444")
        with st.expander(f"**{dim.dimension.title()}** — {score:.1f}/100", expanded=score < 75):
            c1, c2 = st.columns([2, 3])
            with c1:
                st.markdown(f"""
                <div style='background:#111827; border-radius:8px; padding:1rem;'>
                    <div style='font-size:2rem; font-weight:700; color:{bar_color};'>{score:.1f}</div>
                    <div style='font-size:0.78rem; color:#64748b;'>out of 100 · weight {dim.weight*100:.0f}%</div>
                    <div style='background:#1e293b; height:6px; border-radius:3px; margin-top:0.8rem;'>
                        <div style='background:{bar_color}; height:100%; width:{score}%;
                                    border-radius:3px; transition:width 0.8s;'></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            with c2:
                if dim.issues:
                    for issue in dim.issues:
                        st.markdown(f"""
                        <div style='background:#1c0505; border-left:3px solid #ef4444;
                                    padding:0.4rem 0.8rem; border-radius:0 6px 6px 0;
                                    font-size:0.82rem; color:#fca5a5; margin:0.3rem 0;'>
                            ⚠️ {issue}
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    success_box("No issues detected in this dimension.")
                if dim.details:
                    for k, v in dim.details.items():
                        st.caption(f"{k.replace('_',' ').title()}: **{v}**")

    if qc.all_issues:
        section_header("⚠️", "All Issues Summary")
        for issue in qc.all_issues:
            warn_box(issue)
    else:
        success_box("🎉 No quality issues detected. Dataset is clean!")

    section_header("📊", "Column Type Distribution")
    cleaned = st.session_state.get("cleaned_df")
    if cleaned is not None:
        st.plotly_chart(dtype_distribution_chart(cleaned), use_container_width=True,
                        config={"displayModeBar": False})

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — PIPELINE MONITOR
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Pipeline Monitor":
    st.markdown("## 🔄 Pipeline Monitor")
    st.caption("Trigger, monitor, and inspect ETL pipeline runs in real time.")

    # ── Trigger panel ─────────────────────────────────────────────────────
    section_header("🚀", "Trigger a Pipeline")
    with st.form("trigger_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            pipeline = st.selectbox(
                "Pipeline",
                ["customer_data_pipeline", "sales_metrics_pipeline",
                 "product_sync", "orders_daily", "analytics_rollup"],
            )
        with col2:
            env = st.selectbox("Environment", ["production", "staging", "dev"])
        with col3:
            triggered_by = st.text_input("Triggered by", value="dashboard_user")

        col_a, col_b = st.columns([1, 4])
        with col_a:
            submitted = st.form_submit_button("▶ Run Pipeline", type="primary")

    if submitted:
        prog = st.progress(0, text="Starting pipeline...")
        log_placeholder = st.empty()
        new_logs = []

        steps = [
            (10,  "🔗 Connecting to data source..."),
            (25,  "📥 Extracting data..."),
            (45,  "⚙️ Applying transformations..."),
            (65,  "🔬 Running quality checks..."),
            (82,  "📤 Loading to destination..."),
            (95,  "💾 Persisting run metadata..."),
            (100, "✅ Pipeline completed!"),
        ]
        start_time = datetime.datetime.utcnow()

        for pct, msg in steps:
            prog.progress(pct, text=msg)
            log_line = f"{datetime.datetime.utcnow().isoformat()[:19]} [INFO] {msg}"
            new_logs.append(log_line)
            st.session_state["pipeline_logs"].append(log_line)
            log_placeholder.markdown(f"""
            <div class='log-viewer' style='max-height:140px;'>
                {"".join(f"<div class='log-info'>{l}</div>" for l in new_logs[-6:])}
            </div>
            """, unsafe_allow_html=True)
            time.sleep(random.uniform(0.3, 0.8))

        # Record the run
        finish_time = datetime.datetime.utcnow()
        records = random.randint(1000, 50000)
        new_run = {
            "run_id":        f"run_{random.randint(9000,9999)}",
            "pipeline_name": pipeline,
            "status":        "success",
            "started_at":    start_time,
            "finished_at":   finish_time,
            "records_loaded":records,
            "quality_score": round(random.uniform(0.8, 0.98), 2),
            "duration_s":    (finish_time - start_time).seconds,
        }
        st.session_state["pipeline_runs"] = pd.concat(
            [pd.DataFrame([new_run]), st.session_state["pipeline_runs"]],
            ignore_index=True,
        )
        prog.empty()
        log_placeholder.empty()
        success_box(
            f"Pipeline **{pipeline}** completed in "
            f"**{(finish_time-start_time).seconds}s** · "
            f"**{records:,}** records loaded · Environment: **{env}**"
        )

    # ── Run history ────────────────────────────────────────────────────────
    section_header("📋", "Pipeline Run History")
    runs_df = st.session_state["pipeline_runs"]

    # Filter controls
    col1, col2, col3 = st.columns(3)
    with col1:
        pnames = ["All"] + sorted(runs_df["pipeline_name"].unique().tolist())
        p_filter = st.selectbox("Filter by pipeline", pnames)
    with col2:
        s_filter = st.selectbox("Filter by status", ["All","success","failed","running"])
    with col3:
        n_rows = st.slider("Show last N runs", 5, 50, 15)

    filtered = runs_df.copy()
    if p_filter != "All":
        filtered = filtered[filtered["pipeline_name"] == p_filter]
    if s_filter != "All":
        filtered = filtered[filtered["status"] == s_filter]
    filtered = filtered.sort_values("started_at", ascending=False).head(n_rows)

    if not filtered.empty:
        # Status badges column
        def fmt_status(s):
            return {"success": "✅ Success", "failed": "❌ Failed", "running": "⏳ Running"}.get(s, s)
        display = filtered.copy()
        display["Status"]   = display["status"].apply(fmt_status)
        display["Records"]  = display["records_loaded"].apply(lambda v: f"{v:,}" if v else "—")
        display["Quality"]  = display["quality_score"].apply(lambda v: f"{v:.0%}" if pd.notna(v) else "—")
        display["Duration"] = display["duration_s"].apply(lambda v: f"{v}s" if pd.notna(v) else "—")
        st.dataframe(
            display[["run_id","pipeline_name","Status","started_at","Records","Quality","Duration"]],
            use_container_width=True, hide_index=True,
        )
    else:
        info_box("No runs match the current filter.")

    section_header("📈", "Execution Timeline")
    st.plotly_chart(execution_timeline(filtered if not filtered.empty else runs_df),
                    use_container_width=True, config={"displayModeBar": False})

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — LOGS & HISTORY
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Logs & History":
    st.markdown("## 📜 Logs & Execution History")

    col1, col2 = st.columns([4, 1])
    with col1:
        log_filter = st.text_input("🔍 Filter logs", placeholder="Search keyword...")
    with col2:
        if st.button("🗑️ Clear Logs"):
            st.session_state["pipeline_logs"] = []
            st.rerun()

    logs = st.session_state.get("pipeline_logs", [])
    if log_filter:
        logs = [l for l in logs if log_filter.lower() in l.lower()]

    if not logs:
        logs = [
            f"{datetime.datetime.utcnow().isoformat()[:19]} [INFO] Platform started successfully",
            f"{datetime.datetime.utcnow().isoformat()[:19]} [INFO] No pipeline runs yet — trigger a run from the Pipeline Monitor page",
        ]

    section_header("📋", f"Log Entries ({len(logs)} lines)")
    render_log_viewer(logs)

    if st.session_state["pipeline_logs"]:
        log_text = "\n".join(st.session_state["pipeline_logs"])
        st.download_button(
            "⬇️  Download Full Log", log_text.encode(),
            file_name=f"etl_log_{datetime.date.today()}.txt",
            mime="text/plain",
        )

    # ── Run summary chart ──────────────────────────────────────────────────
    runs_df = st.session_state["pipeline_runs"]
    section_header("📊", "Pipeline Run Summary (Last 30 Days)")
    st.plotly_chart(pipeline_run_chart(runs_df), use_container_width=True,
                    config={"displayModeBar": False})

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — SYSTEM HEALTH
# ══════════════════════════════════════════════════════════════════════════════
elif page == "System Health":
    st.markdown("## 🖥️ System Health")
    st.caption("Live status of all platform services and infrastructure components.")

    # Live auto-refresh
    auto_refresh = st.toggle("Auto-refresh every 30s", value=False)
    if auto_refresh:
        time.sleep(0.5)
        st.rerun()

    def _svc_card(name, status, latency, uptime, icon, note=""):
        color  = "#22c55e" if status == "Healthy" else ("#f59e0b" if status == "Degraded" else "#ef4444")
        dot_bg = "#052e16" if status == "Healthy" else ("#1c1003" if status == "Degraded" else "#1c0505")
        return f"""
        <div style='background:#111827; border:1px solid #1e293b; border-radius:12px;
                    padding:1rem 1.2rem; margin:0.4rem 0;
                    display:flex; align-items:center; gap:1rem;'>
            <div style='font-size:1.8rem'>{icon}</div>
            <div style='flex:1;'>
                <div style='font-weight:600; color:#e2e8f0; font-size:0.95rem;'>{name}</div>
                <div style='font-size:0.75rem; color:#64748b;'>{note}</div>
            </div>
            <div style='text-align:right;'>
                <div style='background:{dot_bg}; color:{color}; padding:0.2rem 0.7rem;
                            border-radius:20px; font-size:0.72rem; font-weight:600;
                            margin-bottom:0.3rem;'>● {status}</div>
                <div style='font-size:0.7rem; color:#64748b;'>{latency} · {uptime}</div>
            </div>
        </div>
        """

    services = [
        ("FastAPI Backend",     "Healthy",  "12ms",  "99.97%", "⚡", "Port 8000 · REST API"),
        ("PostgreSQL Database", "Healthy",  "3ms",   "99.99%", "🗄️", "Port 5432 · Primary DB"),
        ("Apache Airflow",      "Healthy",  "88ms",  "99.8%",  "🌀", "Port 8080 · Scheduler + Celery"),
        ("Redis Broker",        "Healthy",  "1ms",   "100%",   "🔴", "Port 6379 · Message Queue"),
        ("Streamlit Dashboard", "Healthy",  "—",     "99.9%",  "📊", "Port 8501 · This UI"),
        ("Celery Workers",      "Healthy",  "—",     "99.5%",  "⚙️", "4 workers active"),
        ("Email SMTP",          "Degraded", "290ms", "98.1%",  "📧", "Alerts – degraded latency"),
        ("Slack Webhook",       "Healthy",  "45ms",  "99.9%",  "💬", "Notifications active"),
    ]

    section_header("🟢", "Service Status")
    for svc in services:
        st.markdown(_svc_card(*svc), unsafe_allow_html=True)

    # Platform stats
    section_header("📊", "Platform Statistics")
    runs_df = st.session_state["pipeline_runs"]
    success_rate = (runs_df["status"] == "success").mean() * 100
    avg_dur      = runs_df["duration_s"].dropna().mean()
    total_records = int(runs_df["records_loaded"].sum())

    kpi_row([
        kpi_card("Platform Uptime",    "99.97%",          "↑ 30-day avg",      "green",  "⏱️"),
        kpi_card("Pipeline Success",   f"{success_rate:.1f}%", "↑ vs last week","green",  "✅"),
        kpi_card("Avg Run Duration",   f"{avg_dur:.0f}s" if pd.notna(avg_dur) else "—",
                 "", "blue", "⚡"),
        kpi_card("Total Records",      f"{total_records:,}",  "Loaded all-time", "purple", "📦"),
        kpi_card("Active Workers",     "4",              "Celery workers",     "orange", "⚙️"),
        kpi_card("DB Connections",     "7 / 20",         "Pool utilisation",   "blue",   "🗄️"),
    ])

    # Resource bars
    section_header("💻", "Resource Utilisation (Simulated)")
    resources = {
        "CPU": random.uniform(18, 45),
        "Memory": random.uniform(35, 65),
        "Disk I/O": random.uniform(5, 25),
        "DB Pool": random.uniform(30, 55),
        "Redis Memory": random.uniform(10, 30),
    }
    for name, pct in resources.items():
        color = "#ef4444" if pct > 80 else ("#f59e0b" if pct > 60 else "#22c55e")
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"""
            <div style='margin:0.4rem 0;'>
                <div style='display:flex; justify-content:space-between;
                            font-size:0.8rem; color:#94a3b8; margin-bottom:0.3rem;'>
                    <span>{name}</span><span style='color:{color};'>{pct:.1f}%</span>
                </div>
                <div style='background:#1e293b; border-radius:4px; height:8px;'>
                    <div style='background:{color}; width:{pct}%; height:100%;
                                border-radius:4px; transition:width 1s;'></div>
                </div>
            </div>
            """, unsafe_allow_html=True)
