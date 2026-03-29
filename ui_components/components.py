"""
Enterprise ETL Platform – UI Component Library

All custom CSS injection, styled metric cards, tables, and
reusable Streamlit widgets live here.
"""
import streamlit as st


# ── GLOBAL CSS ────────────────────────────────────────────────────────────────
GLOBAL_CSS = """
<style>
/* ── base ── */
[data-testid="stAppViewContainer"] {
    background: #0b0f1a;
    color: #e2e8f0;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f1724 0%, #111827 100%);
    border-right: 1px solid #1e293b;
}
[data-testid="stSidebar"] * { color: #cbd5e1 !important; }
[data-testid="stSidebar"] .sidebar-logo {
    font-size: 1.3rem; font-weight: 700; color: #4f8ef7 !important;
    padding: 1rem 0 0.5rem;
}
header[data-testid="stHeader"] { background: transparent; }

/* ── metric cards ── */
.kpi-row { display: flex; gap: 1rem; margin-bottom: 1.2rem; flex-wrap: wrap; }
.kpi-card {
    background: linear-gradient(135deg, #1a1f2e 0%, #131926 100%);
    border: 1px solid #1e293b;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    flex: 1 1 180px;
    min-width: 160px;
    position: relative;
    overflow: hidden;
    transition: transform 0.2s, box-shadow 0.2s;
}
.kpi-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 8px 24px rgba(79,142,247,0.15);
}
.kpi-card::before {
    content: '';
    position: absolute; top: 0; left: 0;
    width: 4px; height: 100%;
    border-radius: 4px 0 0 4px;
}
.kpi-card.blue::before  { background: #4f8ef7; }
.kpi-card.green::before { background: #22c55e; }
.kpi-card.red::before   { background: #ef4444; }
.kpi-card.orange::before{ background: #f59e0b; }
.kpi-card.purple::before{ background: #a855f7; }

.kpi-label { font-size: 0.72rem; color: #64748b; text-transform: uppercase;
              letter-spacing: 0.08em; margin-bottom: 0.4rem; }
.kpi-value { font-size: 2rem; font-weight: 700; color: #f1f5f9;
              line-height: 1.1; margin-bottom: 0.2rem; }
.kpi-delta { font-size: 0.78rem; }
.kpi-delta.up   { color: #22c55e; }
.kpi-delta.down { color: #ef4444; }
.kpi-delta.neutral { color: #94a3b8; }
.kpi-icon { font-size: 1.6rem; position: absolute; right: 1rem; top: 1rem;
            opacity: 0.25; }

/* ── section headers ── */
.section-header {
    display: flex; align-items: center; gap: 0.6rem;
    font-size: 1.05rem; font-weight: 600; color: #e2e8f0;
    border-bottom: 1px solid #1e293b;
    padding-bottom: 0.5rem; margin: 1.5rem 0 1rem;
}

/* ── badge pills ── */
.badge {
    display: inline-block; padding: 0.2rem 0.6rem;
    border-radius: 20px; font-size: 0.72rem; font-weight: 600;
}
.badge-green  { background: #14532d; color: #22c55e; }
.badge-red    { background: #450a0a; color: #f87171; }
.badge-orange { background: #451a03; color: #fb923c; }
.badge-blue   { background: #0c1a40; color: #60a5fa; }
.badge-purple { background: #2e1065; color: #c084fc; }

/* ── action buttons ── */
.stButton > button {
    background: linear-gradient(135deg, #1d4ed8, #4f8ef7);
    color: white; border: none; border-radius: 8px;
    padding: 0.5rem 1.2rem; font-weight: 600;
    transition: all 0.2s;
}
.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 14px rgba(79,142,247,0.4);
}
.btn-success > button { background: linear-gradient(135deg, #15803d, #22c55e) !important; }
.btn-danger  > button { background: linear-gradient(135deg, #b91c1c, #ef4444) !important; }
.btn-orange  > button { background: linear-gradient(135deg, #b45309, #f59e0b) !important; }

/* ── upload zone ── */
[data-testid="stFileUploader"] {
    background: #111827; border: 2px dashed #1e293b;
    border-radius: 12px; padding: 1rem;
    transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #4f8ef7; }

/* ── dataframe ── */
[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }
.dataframe thead th {
    background: #1e293b !important; color: #94a3b8 !important;
    font-size: 0.78rem !important; text-transform: uppercase; letter-spacing: 0.05em;
}
.dataframe tbody tr:hover td { background: #1e293b !important; }

/* ── log viewer ── */
.log-viewer {
    background: #060a12; border: 1px solid #1e293b;
    border-radius: 10px; padding: 1rem;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    font-size: 0.78rem; max-height: 340px; overflow-y: auto;
    line-height: 1.7;
}
.log-info    { color: #60a5fa; }
.log-success { color: #34d399; }
.log-warning { color: #fbbf24; }
.log-error   { color: #f87171; }
.log-ts      { color: #475569; margin-right: 0.5rem; }

/* ── progress bar override ── */
.stProgress > div > div { background: linear-gradient(90deg, #1d4ed8, #4f8ef7); }

/* ── tabs ── */
button[data-baseweb="tab"] {
    background: transparent; color: #64748b;
    border-bottom: 2px solid transparent;
    font-weight: 500;
}
button[data-baseweb="tab"][aria-selected="true"] {
    color: #4f8ef7; border-bottom-color: #4f8ef7;
}

/* ── selectbox ── */
[data-baseweb="select"] { border-radius: 8px; }
[data-baseweb="select"] > div { background: #1a1f2e; border-color: #1e293b; }

/* ── info boxes ── */
.info-box {
    background: #0c1a40; border-left: 3px solid #4f8ef7;
    border-radius: 0 8px 8px 0; padding: 0.8rem 1rem;
    font-size: 0.88rem; color: #93c5fd; margin: 0.5rem 0;
}
.warn-box {
    background: #1c1003; border-left: 3px solid #f59e0b;
    border-radius: 0 8px 8px 0; padding: 0.8rem 1rem;
    font-size: 0.88rem; color: #fbbf24; margin: 0.5rem 0;
}
.success-box {
    background: #052e16; border-left: 3px solid #22c55e;
    border-radius: 0 8px 8px 0; padding: 0.8rem 1rem;
    font-size: 0.88rem; color: #86efac; margin: 0.5rem 0;
}
.error-box {
    background: #1c0505; border-left: 3px solid #ef4444;
    border-radius: 0 8px 8px 0; padding: 0.8rem 1rem;
    font-size: 0.88rem; color: #fca5a5; margin: 0.5rem 0;
}
</style>
"""


def inject_css():
    """Inject global dashboard CSS — call once at top of app."""
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


# ── SIDEBAR ───────────────────────────────────────────────────────────────────
def render_sidebar() -> str:
    """
    Render the sidebar navigation and return the selected page name.
    """
    with st.sidebar:
        st.markdown("""
        <div style='text-align:center; padding: 1.2rem 0 0.8rem;'>
            <div style='font-size:1.8rem'>⚙️</div>
            <div style='font-size:1.1rem; font-weight:700; color:#4f8ef7;'>
                ETL Platform
            </div>
            <div style='font-size:0.7rem; color:#475569; margin-top:0.2rem;'>
                Enterprise Data Orchestration
            </div>
        </div>
        <hr style='border-color:#1e293b; margin:0.5rem 0 1rem;'>
        """, unsafe_allow_html=True)

        pages = {
            "🏠  Overview":              "Overview",
            "📤  Upload Data":           "Upload Data",
            "🧹  Cleaning Results":      "Cleaning Results",
            "🔬  Data Quality":          "Data Quality",
            "🔄  Pipeline Monitor":      "Pipeline Monitor",
            "📜  Logs & History":        "Logs & History",
            "🖥️  System Health":         "System Health",
        }

        selected = st.radio(
            label="nav",
            options=list(pages.keys()),
            label_visibility="collapsed",
        )

        st.markdown("<hr style='border-color:#1e293b; margin:1.5rem 0 1rem;'>",
                    unsafe_allow_html=True)
        st.markdown("""
        <div style='font-size:0.68rem; color:#334155; text-align:center;'>
            Enterprise ETL Platform<br>v2.0 · Production Ready
        </div>
        """, unsafe_allow_html=True)

    return pages[selected]


# ── KPI CARDS ─────────────────────────────────────────────────────────────────
def kpi_card(label: str, value, delta: str = "", color: str = "blue", icon: str = "📊"):
    """Render a single animated KPI card."""
    delta_class = "up" if "↑" in delta or "+" in delta else (
        "down" if "↓" in delta or "-" in delta else "neutral"
    )
    delta_html = f'<div class="kpi-delta {delta_class}">{delta}</div>' if delta else ""
    return f"""
    <div class="kpi-card {color}">
        <div class="kpi-icon">{icon}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """


def kpi_row(cards: list):
    """Render a row of KPI cards from a list of kpi_card() html strings."""
    html = '<div class="kpi-row">' + "".join(cards) + "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ── SECTION HEADERS ───────────────────────────────────────────────────────────
def section_header(icon: str, title: str):
    st.markdown(
        f'<div class="section-header"><span>{icon}</span><span>{title}</span></div>',
        unsafe_allow_html=True,
    )


# ── INFO BOXES ────────────────────────────────────────────────────────────────
def info_box(text: str):
    st.markdown(f'<div class="info-box">ℹ️ {text}</div>', unsafe_allow_html=True)

def warn_box(text: str):
    st.markdown(f'<div class="warn-box">⚠️ {text}</div>', unsafe_allow_html=True)

def success_box(text: str):
    st.markdown(f'<div class="success-box">✅ {text}</div>', unsafe_allow_html=True)

def error_box(text: str):
    st.markdown(f'<div class="error-box">❌ {text}</div>', unsafe_allow_html=True)


# ── LOG VIEWER ────────────────────────────────────────────────────────────────
def render_log_viewer(log_lines: list):
    """Render a styled monospace log viewer."""
    def _class(line: str) -> str:
        lo = line.lower()
        if "error" in lo or "fail" in lo or "exception" in lo:
            return "log-error"
        if "warn" in lo:
            return "log-warning"
        if "success" in lo or "complete" in lo or "✓" in lo:
            return "log-success"
        return "log-info"

    html_lines = []
    for line in log_lines[-200:]:  # last 200 entries
        parts = line.split(" ", 1)
        ts = parts[0] if len(parts) > 1 else ""
        msg = parts[1] if len(parts) > 1 else line
        cls = _class(line)
        html_lines.append(
            f'<div><span class="log-ts">{ts}</span>'
            f'<span class="{cls}">{msg}</span></div>'
        )

    inner = "\n".join(html_lines) or "<div class='log-info'>No log entries yet.</div>"
    st.markdown(f'<div class="log-viewer">{inner}</div>', unsafe_allow_html=True)


# ── BADGE ─────────────────────────────────────────────────────────────────────
def badge(text: str, color: str = "blue") -> str:
    return f'<span class="badge badge-{color}">{text}</span>'


def status_badge(status: str) -> str:
    m = {"success": "green", "failed": "red", "running": "blue",
         "pending": "orange", "skipped": "purple"}
    return badge(status.upper(), m.get(status.lower(), "blue"))


# ── PROGRESS STEP TRACKER ─────────────────────────────────────────────────────
def step_tracker(steps: list, current: int):
    """
    Render a horizontal step tracker.
    steps: list of step labels
    current: 0-based index of the active step
    """
    html = '<div style="display:flex; align-items:center; margin:1rem 0;">'
    for i, step in enumerate(steps):
        if i < current:
            dot_style = "background:#22c55e; color:white;"
            text_color = "#22c55e"
        elif i == current:
            dot_style = "background:#4f8ef7; color:white;"
            text_color = "#4f8ef7"
        else:
            dot_style = "background:#1e293b; color:#475569;"
            text_color = "#475569"

        html += f"""
        <div style="display:flex; flex-direction:column; align-items:center; flex:1;">
            <div style="{dot_style} width:28px; height:28px; border-radius:50%;
                        display:flex; align-items:center; justify-content:center;
                        font-size:0.8rem; font-weight:700;">
                {"✓" if i < current else str(i+1)}
            </div>
            <div style="font-size:0.68rem; margin-top:0.3rem; color:{text_color};
                        text-align:center; max-width:80px;">{step}</div>
        </div>
        """
        if i < len(steps) - 1:
            line_color = "#22c55e" if i < current else "#1e293b"
            html += f'<div style="flex:1; height:2px; background:{line_color}; margin-bottom:1rem;"></div>'

    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)
