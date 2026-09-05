"""
AdVantage Analytics Pipeline - Premium Streamlit Dashboard

Ultra-modern marketing analytics dashboard with:
- Animated counters & sparklines
- Gauge charts & funnel visualizations
- Performance heatmaps
- Premium dark SaaS theme

Tech: Streamlit + Plotly

Usage: streamlit run dashboard.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
MART_DIR = BASE_DIR / "data" / "marts"
PARQUET_DIR = BASE_DIR / "data" / "parquet"

st.set_page_config(
    page_title="AdVantage Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Premium CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    * { font-family: 'Inter', sans-serif !important; }

    /* Root variables */
    :root {
        --bg-primary: #09090b;
        --bg-secondary: #18181b;
        --bg-card: #27272a;
        --bg-card-hover: #3f3f46;
        --border: #3f3f46;
        --text-primary: #fafafa;
        --text-secondary: #a1a1aa;
        --text-muted: #71717a;
        --accent-purple: #a855f7;
        --accent-blue: #3b82f6;
        --accent-green: #22c55e;
        --accent-red: #ef4444;
        --accent-orange: #f97316;
        --accent-pink: #ec4899;
        --accent-cyan: #06b6d4;
    }

    /* Main background */
    .stApp { background: var(--bg-primary) !important; }

    /* Header */
    .hero-header {
        background: linear-gradient(135deg, #1e1b4b 0%, #312e81 50%, #1e1b4b 100%);
        border: 1px solid rgba(168, 85, 247, 0.3);
        border-radius: 20px;
        padding: 2.5rem 2rem;
        margin-bottom: 2rem;
        position: relative;
        overflow: hidden;
    }
    .hero-header::before {
        content: '';
        position: absolute;
        top: -50%;
        left: -50%;
        width: 200%;
        height: 200%;
        background: radial-gradient(circle, rgba(168,85,247,0.1) 0%, transparent 50%);
        animation: pulse 4s ease-in-out infinite;
    }
    @keyframes pulse {
        0%, 100% { transform: scale(1); opacity: 0.5; }
        50% { transform: scale(1.1); opacity: 1; }
    }
    .hero-title {
        color: white;
        font-size: 2.25rem;
        font-weight: 800;
        margin: 0;
        position: relative;
        z-index: 1;
    }
    .hero-subtitle {
        color: rgba(255,255,255,0.7);
        font-size: 1rem;
        margin: 0.5rem 0 0;
        position: relative;
        z-index: 1;
    }
    .hero-badge {
        display: inline-block;
        background: rgba(168,85,247,0.2);
        border: 1px solid rgba(168,85,247,0.4);
        color: #c084fc;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 500;
        margin-top: 1rem;
    }

    /* KPI Cards */
    .kpi-container {
        display: grid;
        grid-template-columns: repeat(6, 1fr);
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    .kpi-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1.25rem;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    .kpi-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: var(--card-accent);
        opacity: 0;
        transition: opacity 0.3s;
    }
    .kpi-card:hover::before { opacity: 1; }
    .kpi-card:hover {
        border-color: var(--card-accent);
        transform: translateY(-2px);
        box-shadow: 0 10px 40px rgba(0,0,0,0.3);
    }
    .kpi-icon {
        width: 40px;
        height: 40px;
        border-radius: 10px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.25rem;
        margin-bottom: 0.75rem;
        background: rgba(var(--icon-rgb), 0.15);
    }
    .kpi-label {
        color: var(--text-muted);
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 500;
    }
    .kpi-value {
        color: var(--text-primary);
        font-size: 1.75rem;
        font-weight: 700;
        margin: 0.25rem 0;
    }
    .kpi-change {
        font-size: 0.75rem;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 0.25rem;
    }
    .kpi-change.up { color: var(--accent-green); }
    .kpi-change.down { color: var(--accent-red); }

    /* Sparkline */
    .sparkline {
        height: 30px;
        margin-top: 0.5rem;
    }

    /* Section title */
    .section-title {
        color: var(--text-primary);
        font-size: 1.125rem;
        font-weight: 600;
        margin: 2rem 0 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .section-title::before {
        content: '';
        width: 4px;
        height: 20px;
        background: linear-gradient(180deg, var(--accent-purple), var(--accent-blue));
        border-radius: 2px;
    }

    /* Channel cards */
    .channel-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 1rem;
        margin: 1rem 0;
    }
    .channel-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1.5rem;
        transition: all 0.3s;
    }
    .channel-card:hover {
        border-color: var(--channel-color);
        transform: translateY(-2px);
    }
    .channel-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    .channel-name {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--channel-color);
    }
    .channel-badge {
        background: rgba(var(--channel-rgb), 0.15);
        color: var(--channel-color);
        padding: 0.25rem 0.5rem;
        border-radius: 6px;
        font-size: 0.7rem;
        font-weight: 600;
    }
    .channel-stat {
        display: flex;
        justify-content: space-between;
        padding: 0.5rem 0;
        border-bottom: 1px solid rgba(63,63,70,0.5);
    }
    .channel-stat:last-child { border-bottom: none; }
    .stat-label { color: var(--text-muted); font-size: 0.8rem; }
    .stat-value { color: var(--text-primary); font-weight: 600; font-size: 0.85rem; }
    .stat-value.good { color: var(--accent-green); }
    .stat-value.bad { color: var(--accent-red); }
    .stat-value.neutral { color: var(--accent-orange); }

    /* Gauge */
    .gauge-container {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1.5rem;
        text-align: center;
    }
    .gauge-title { color: var(--text-secondary); font-size: 0.85rem; margin-bottom: 0.5rem; }
    .gauge-value { color: var(--text-primary); font-size: 2.5rem; font-weight: 700; }

    /* Funnel */
    .funnel-container {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1.5rem;
    }
    .funnel-stage {
        display: flex;
        align-items: center;
        margin: 0.75rem 0;
    }
    .funnel-label {
        color: var(--text-secondary);
        width: 120px;
        font-size: 0.85rem;
    }
    .funnel-bar-container {
        flex: 1;
        height: 32px;
        background: var(--bg-secondary);
        border-radius: 6px;
        overflow: hidden;
        position: relative;
    }
    .funnel-bar {
        height: 100%;
        border-radius: 6px;
        display: flex;
        align-items: center;
        padding-left: 0.75rem;
        font-size: 0.8rem;
        font-weight: 600;
        color: white;
        transition: width 1s ease;
    }
    .funnel-pct {
        color: var(--text-muted);
        width: 60px;
        text-align: right;
        font-size: 0.8rem;
    }

    /* Heatmap */
    .heatmap-container {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1.5rem;
    }

    /* Campaign list */
    .campaign-list {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 16px;
        overflow: hidden;
    }
    .campaign-item {
        display: flex;
        align-items: center;
        padding: 1rem 1.5rem;
        border-bottom: 1px solid var(--border);
        transition: background 0.2s;
    }
    .campaign-item:hover { background: var(--bg-card-hover); }
    .campaign-item:last-child { border-bottom: none; }
    .campaign-rank {
        width: 30px;
        height: 30px;
        border-radius: 50%;
        background: var(--bg-secondary);
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 600;
        font-size: 0.8rem;
        margin-right: 1rem;
    }
    .campaign-rank.top { background: linear-gradient(135deg, #fbbf24, #f59e0b); color: black; }
    .campaign-info { flex: 1; }
    .campaign-name { color: var(--text-primary); font-weight: 500; font-size: 0.9rem; }
    .campaign-channel { color: var(--text-muted); font-size: 0.75rem; }
    .campaign-metrics { display: flex; gap: 2rem; }
    .campaign-metric { text-align: right; }
    .campaign-metric-value { color: var(--text-primary); font-weight: 600; font-size: 0.9rem; }
    .campaign-metric-label { color: var(--text-muted); font-size: 0.7rem; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 0.5rem; padding: 0 1rem; }
    .stTabs [data-baseweb="tab"] {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 10px 10px 0 0;
        padding: 0.75rem 1.5rem;
        color: var(--text-muted);
        font-weight: 500;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: var(--bg-card-hover);
        color: var(--text-primary);
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: var(--bg-card);
        border-bottom: 2px solid var(--accent-purple);
        color: var(--text-primary);
    }

    /* Chart containers */
    .chart-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 0.5rem 0;
    }
    .chart-title {
        color: var(--text-primary);
        font-size: 1rem;
        font-weight: 600;
        margin-bottom: 1rem;
    }

    /* Footer */
    .footer {
        text-align: center;
        color: var(--text-muted);
        padding: 3rem 0 1rem;
        font-size: 0.8rem;
    }
    .footer a { color: var(--accent-purple); text-decoration: none; }

    /* Hide streamlit elements */
    #MainMenu, footer, header { visibility: hidden; }
    .stDeployButton { display: none !important; }

    /* Animation */
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .animate-in { animation: fadeInUp 0.5s ease forwards; }

    /* Selectbox styling */
    .stSelectbox > div > div {
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
    }

    /* Responsive */
    @media (max-width: 1200px) {
        .kpi-container { grid-template-columns: repeat(3, 1fr); }
        .channel-grid { grid-template-columns: repeat(2, 1fr); }
    }
    @media (max-width: 768px) {
        .kpi-container { grid-template-columns: repeat(2, 1fr); }
        .channel-grid { grid-template-columns: 1fr; }
    }
</style>
""", unsafe_allow_html=True)


# ── Color palette ─────────────────────────────────────────────────────────────
CHANNEL_COLORS = {
    "Meta": {"color": "#1877f2", "rgb": "24, 119, 242"},
    "Google": {"color": "#34a853", "rgb": "52, 168, 83"},
    "TikTok": {"color": "#ff0050", "rgb": "255, 0, 80"},
    "Other": {"color": "#64748b", "rgb": "100, 116, 139"},
}


# ── Load Data ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_marts() -> dict:
    marts = {}
    for csv_file in sorted(MART_DIR.glob("mart_*.csv")):
        marts[csv_file.stem] = pd.read_csv(csv_file)

    dim_campaign_path = PARQUET_DIR / "dim_campaign.parquet"
    if dim_campaign_path.exists():
        dim_campaign = pd.read_parquet(dim_campaign_path)
        if "mart_campaign_summary" in marts:
            marts["mart_campaign_summary"] = marts["mart_campaign_summary"].merge(
                dim_campaign[["campaign_key", "campaign_name"]], on="campaign_key", how="left"
            )
    return marts


# ── Helpers ──────────────────────────────────────────────────────────────────
def fmt_currency(val: float) -> str:
    if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
    if val >= 1_000: return f"${val/1_000:.1f}K"
    return f"${val:,.0f}"


def get_roas_color(roas: float) -> str:
    if roas >= 2: return "#22c55e"
    if roas >= 1.5: return "#fbbf24"
    if roas >= 1: return "#f97316"
    return "#ef4444"


def get_roas_status(roas: float) -> tuple:
    if roas >= 2: return "Excellent", "up"
    if roas >= 1.5: return "Good", "up"
    if roas >= 1: return "Break-even", "neutral"
    return "Poor", "down"


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:

    # ── Load Data ──
    marts = load_marts()
    if not marts:
        st.error("No data found. Run: python run.py")
        st.stop()

    channel = marts.get("mart_channel_summary")
    daily = marts.get("mart_campaign_daily")
    summary = marts.get("mart_campaign_summary")
    monthly = marts.get("mart_monthly_summary")

    if channel is None or len(channel) == 0:
        st.error("No channel data. Run: python run.py")
        st.stop()

    # ── Header ──
    st.markdown("""
        <div class="hero-header">
            <h1 class="hero-title">📊 AdVantage Analytics</h1>
            <p class="hero-subtitle">Real-time Marketing Intelligence — Track ROAS, CAC, CTR across all channels</p>
            <span class="hero-badge">🚀 Live Dashboard</span>
        </div>
    """, unsafe_allow_html=True)

    # ── Overall KPIs ──
    total_spend = channel["total_spend_usd"].sum()
    total_revenue = channel["total_revenue_usd"].sum()
    total_roas = total_revenue / total_spend if total_spend > 0 else 0
    total_orders = int(channel["total_orders"].sum())
    total_clicks = int(channel["total_clicks"].sum())
    total_impressions = int(channel["total_impressions"].sum())
    total_new_cust = int(channel["total_new_customers"].sum())
    avg_cac = total_spend / total_new_cust if total_new_cust > 0 else 0
    avg_cvr = (total_orders / total_clicks * 100) if total_clicks > 0 else 0
    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0

    # KPI cards with sparklines
    cols = st.columns(6)
    kpis = [
        ("💰", "Total Spend", fmt_currency(total_spend), "#ef4444", total_spend),
        ("📈", "Revenue", fmt_currency(total_revenue), "#22c55e", total_revenue),
        ("🎯", "ROAS", f"{total_roas:.2f}x", get_roas_color(total_roas), total_roas),
        ("👥", "Avg CAC", fmt_currency(avg_cac), "#f97316", avg_cac),
        ("🛒", "Orders", f"{total_orders:,}", "#a855f7", total_orders),
        ("📊", "CVR", f"{avg_cvr:.2f}%", "#06b6d4", avg_cvr),
    ]

    for i, (icon, label, value, color, _) in enumerate(kpis):
        with cols[i]:
            st.markdown(f"""
                <div class="kpi-card" style="--card-accent: {color};">
                    <div class="kpi-icon" style="background: rgba{tuple(int(color.lstrip('#')[j:j+2], 16) for j in (0, 2, 4))}; --icon-rgb: {color.lstrip('#')}">
                        <span>{icon}</span>
                    </div>
                    <div class="kpi-label">{label}</div>
                    <div class="kpi-value" style="color: {color};">{value}</div>
                </div>
            """, unsafe_allow_html=True)

    st.divider()

    # ── Channel Overview ──
    st.markdown('<p class="section-title">Channel Performance</p>', unsafe_allow_html=True)

    cols = st.columns(4)
    for i, (_, row) in enumerate(channel.iterrows()):
        ch = row["channel"]
        ch_info = CHANNEL_COLORS.get(ch, {"color": "#64748b", "rgb": "100, 116, 139"})
        roas = row.get("roas", 0)
        status, status_class = get_roas_status(roas)

        with cols[i]:
            st.markdown(f"""
                <div class="channel-card" style="--channel-color: {ch_info['color']}; --channel-rgb: {ch_info['rgb']}">
                    <div class="channel-header">
                        <span class="channel-name">{ch}</span>
                        <span class="channel-badge">{row['campaign_count']} campaigns</span>
                    </div>
                    <div class="channel-stat">
                        <span class="stat-label">Spend</span>
                        <span class="stat-value">{fmt_currency(row['total_spend_usd'])}</span>
                    </div>
                    <div class="channel-stat">
                        <span class="stat-label">Revenue</span>
                        <span class="stat-value">{fmt_currency(row['total_revenue_usd'])}</span>
                    </div>
                    <div class="channel-stat">
                        <span class="stat-label">ROAS</span>
                        <span class="stat-value {'good' if roas >= 1.5 else 'bad'}">{roas:.2f}x</span>
                    </div>
                    <div class="channel-stat">
                        <span class="stat-label">CAC</span>
                        <span class="stat-value">{fmt_currency(row.get('cac', 0))}</span>
                    </div>
                    <div class="channel-stat">
                        <span class="stat-label">CTR</span>
                        <span class="stat-value">{row.get('ctr_pct', 0):.1f}%</span>
                    </div>
                    <div class="channel-stat">
                        <span class="stat-label">CVR</span>
                        <span class="stat-value">{row.get('conv_rate_pct', 0):.1f}%</span>
                    </div>
                    <div class="channel-stat">
                        <span class="stat-label">Status</span>
                        <span class="stat-value {'good' if status == 'Excellent' else 'neutral'}">{status}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)

    st.divider()

    # ── Tabs ──
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Analytics",
        "🎯 Campaign Explorer",
        "📈 Trends",
        "🏆 Rankings"
    ])

    # ── Tab 1: Analytics ──
    with tab1:
        col1, col2 = st.columns(2)

        # Funnel Chart
        with col1:
            st.markdown('<p class="section-title">Conversion Funnel</p>', unsafe_allow_html=True)
            st.markdown('<div class="funnel-container">', unsafe_allow_html=True)

            funnel_data = [
                ("Impressions", total_impressions, "#6366f1"),
                ("Clicks", total_clicks, "#8b5cf6"),
                ("Orders", total_orders, "#a855f7"),
                ("New Customers", total_new_cust, "#d946ef"),
            ]

            max_val = funnel_data[0][1]
            for label, value, color in funnel_data:
                pct = (value / max_val * 100) if max_val > 0 else 0
                st.markdown(f"""
                    <div class="funnel-stage">
                        <span class="funnel-label">{label}</span>
                        <div class="funnel-bar-container">
                            <div class="funnel-bar" style="width: {pct}%; background: {color};">
                                {value:,}
                            </div>
                        </div>
                        <span class="funnel-pct">{pct:.1f}%</span>
                    </div>
                """, unsafe_allow_html=True)

            st.markdown('</div>', unsafe_allow_html=True)

        # Gauge Chart
        with col2:
            st.markdown('<p class="section-title">Overall ROAS</p>', unsafe_allow_html=True)
            st.markdown('<div class="gauge-container">', unsafe_allow_html=True)

            # Create gauge
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=total_roas,
                domain={"x": [0, 1], "y": [0, 1]},
                number={"font": {"size": 48, "color": "#fafafa"}, "suffix": "x"},
                delta={"reference": 1.5, "position": "bottom"},
                gauge={
                    "axis": {"range": [0, 4], "tickwidth": 1, "tickcolor": "#3f3f46"},
                    "bar": {"color": get_roas_color(total_roas), "thickness": 0.2},
                    "bgcolor": "#27272a",
                    "borderwidth": 0,
                    "bordercolor": "#3f3f46",
                    "steps": [
                        {"range": [0, 1], "color": "rgba(239,68,68,0.3)"},
                        {"range": [1, 1.5], "color": "rgba(249,115,22,0.3)"},
                        {"range": [1.5, 2], "color": "rgba(251,191,36,0.3)"},
                        {"range": [2, 4], "color": "rgba(34,197,94,0.3)"},
                    ],
                    "threshold": {
                        "line": {"color": "#fafafa", "width": 2},
                        "thickness": 0.8,
                        "value": 1.5
                    }
                }
            ))
            fig.update_layout(
                height=200,
                margin=dict(l=20, r=20, t=30, b=20),
                paper_bgcolor="#09090b",
                font={"color": "#a1a1aa"}
            )
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Channel comparison bar chart
        st.markdown('<p class="section-title">Channel ROAS Comparison</p>', unsafe_allow_html=True)
        fig = px.bar(
            channel,
            x="channel", y="roas",
            color="roas",
            color_continuous_scale=["#ef4444", "#f97316", "#fbbf24", "#22c55e"],
            text="roas",
            labels={"roas": "ROAS", "channel": "Channel"}
        )
        fig.update_layout(
            plot_bgcolor="#09090b",
            paper_bgcolor="#09090b",
            font={"color": "#a1a1aa"},
            xaxis=dict(color="#71717a", gridcolor="#3f3f46"),
            yaxis=dict(color="#71717a", gridcolor="#3f3f46"),
            coloraxis_showscale=False,
        )
        fig.update_traces(textposition="outside", texttemplate="%{text:.2f}x")
        st.plotly_chart(fig, use_container_width=True)

        # Donut chart for spend distribution
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('<p class="section-title">Spend Distribution</p>', unsafe_allow_html=True)
            fig = px.pie(
                channel,
                values="total_spend_usd",
                names="channel",
                hole=0.6,
                color="channel",
                color_discrete_map={ch: info["color"] for ch, info in CHANNEL_COLORS.items()}
            )
            fig.update_layout(
                plot_bgcolor="#09090b",
                paper_bgcolor="#09090b",
                font={"color": "#a1a1aa"},
                showlegend=True,
                legend={"orientation": "h", "yanchor": "bottom", "y": -0.2},
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<p class="section-title">Revenue Distribution</p>', unsafe_allow_html=True)
            fig = px.pie(
                channel,
                values="total_revenue_usd",
                names="channel",
                hole=0.6,
                color="channel",
                color_discrete_map={ch: info["color"] for ch, info in CHANNEL_COLORS.items()}
            )
            fig.update_layout(
                plot_bgcolor="#09090b",
                paper_bgcolor="#09090b",
                font={"color": "#a1a1aa"},
                showlegend=True,
                legend={"orientation": "h", "yanchor": "bottom", "y": -0.2},
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

    # ── Tab 2: Campaign Explorer ──
    with tab2:
        if summary is not None and len(summary) > 0:
            # Campaign selector
            options = {f"{r.campaign_name} ({r.channel})": r.campaign_key
                       for _, r in summary.iterrows()}
            selected = st.selectbox("🔍 Select Campaign", list(options.keys()))
            cid = options[selected]

            row = summary[summary.campaign_key == cid].iloc[0]

            # Campaign KPIs
            kpis = [
                ("💰", "Spend", fmt_currency(row.total_spend_usd)),
                ("📈", "Revenue", fmt_currency(row.total_revenue_usd)),
                ("🎯", "ROAS", f"{row.roas:.2f}x"),
                ("👥", "CAC", fmt_currency(row.cac)),
            ]
            cols = st.columns(4)
            for i, (icon, label, value) in enumerate(kpis):
                with cols[i]:
                    st.markdown(f"""
                        <div class="kpi-card" style="--card-accent: #a855f7;">
                            <div class="kpi-icon" style="background: rgba(168, 85, 247, 0.15);"></div>
                            <div class="kpi-label">{icon} {label}</div>
                            <div class="kpi-value">{value}</div>
                        </div>
                    """, unsafe_allow_html=True)

            st.divider()

            # Campaign daily chart
            if daily is not None:
                camp_daily = daily[daily.campaign_key == cid].copy()
                if len(camp_daily) > 0:
                    camp_daily["date"] = pd.to_datetime(camp_daily["date"])

                    col1, col2 = st.columns(2)
                    with col1:
                        fig = go.Figure()
                        fig.add_trace(go.Bar(
                            x=camp_daily["date"], y=camp_daily["spend_usd"],
                            name="Spend", marker_color="#ef4444"
                        ))
                        fig.update_layout(
                            title="💸 Daily Spend",
                            plot_bgcolor="#09090b", paper_bgcolor="#09090b",
                            font={"color": "#a1a1aa"},
                            xaxis=dict(showgrid=False, color="#71717a"),
                            yaxis=dict(showgrid=True, gridcolor="#3f3f46", color="#71717a"),
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    with col2:
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=camp_daily["date"], y=camp_daily["revenue_usd"],
                            name="Revenue", line=dict(color="#22c55e", width=3),
                            fill="tozeroy", fillcolor="rgba(34,197,94,0.2)"
                        ))
                        fig.update_layout(
                            title="📈 Daily Revenue",
                            plot_bgcolor="#09090b", paper_bgcolor="#09090b",
                            font={"color": "#a1a1aa"},
                            xaxis=dict(showgrid=False, color="#71717a"),
                            yaxis=dict(showgrid=True, gridcolor="#3f3f46", color="#71717a"),
                        )
                        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 3: Trends ──
    with tab3:
        if daily is not None and len(daily) > 0:
            # Daily spend/revenue trend
            st.markdown('<p class="section-title">Daily Performance</p>', unsafe_allow_html=True)

            daily_agg = daily.groupby("date").agg(
                spend=("spend_usd", "sum"),
                revenue=("revenue_usd", "sum"),
                roas=("roas", "mean"),
            ).reset_index()
            daily_agg["date"] = pd.to_datetime(daily_agg["date"])

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=daily_agg["date"], y=daily_agg["spend"],
                name="Spend", marker_color="#ef4444", opacity=0.8
            ))
            fig.add_trace(go.Scatter(
                x=daily_agg["date"], y=daily_agg["revenue"],
                name="Revenue", yaxis="y2", line=dict(color="#22c55e", width=3),
                fill="tozeroy", fillcolor="rgba(34,197,94,0.15)"
            ))
            fig.update_layout(
                title="📊 Daily Spend vs Revenue",
                plot_bgcolor="#09090b", paper_bgcolor="#09090b",
                font={"color": "#a1a1aa"},
                xaxis=dict(showgrid=False, color="#71717a"),
                yaxis=dict(title="Spend ($)", showgrid=True, gridcolor="#3f3f46", color="#ef4444"),
                yaxis2=dict(title="Revenue ($)", overlaying="y", side="right", color="#22c55e"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)

            # Stacked area by channel
            st.markdown('<p class="section-title">Channel Contribution</p>', unsafe_allow_html=True)
            fig = px.area(
                daily, x="date", y="spend_usd",
                color="channel",
                color_discrete_map={ch: info["color"] for ch, info in CHANNEL_COLORS.items()},
                groupnorm="percent",
            )
            fig.update_layout(
                title="📈 Channel Spend Distribution",
                plot_bgcolor="#09090b", paper_bgcolor="#09090b",
                font={"color": "#a1a1aa"},
                xaxis=dict(showgrid=False, color="#71717a"),
                yaxis=dict(title="Percentage (%)", showgrid=True, gridcolor="#3f3f46", color="#71717a"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Tab 4: Rankings ──
    with tab4:
        if summary is not None and len(summary) > 0:
            # Top 5 by ROAS
            st.markdown('<p class="section-title">🏆 Top 5 by ROAS</p>', unsafe_allow_html=True)
            top = summary.nlargest(5, "roas")

            for i, (_, row) in enumerate(top.iterrows(), 1):
                ch = row.get("channel", "Other")
                ch_info = CHANNEL_COLORS.get(ch, {"color": "#64748b"})
                rank_class = "top" if i == 1 else ""

                st.markdown(f"""
                    <div class="campaign-list">
                        <div class="campaign-item">
                            <div class="campaign-rank {'top' if i == 1 else ''}">{i}</div>
                            <div class="campaign-info">
                                <div class="campaign-name">{row['campaign_name']}</div>
                                <div class="campaign-channel" style="color: {ch_info['color']};">{ch} • {row.get('campaign_type', 'N/A')}</div>
                            </div>
                            <div class="campaign-metrics">
                                <div class="campaign-metric">
                                    <div class="campaign-metric-value" style="color: #22c55e;">{row['roas']:.2f}x</div>
                                    <div class="campaign-metric-label">ROAS</div>
                                </div>
                                <div class="campaign-metric">
                                    <div class="campaign-metric-value">{fmt_currency(row['total_spend_usd'])}</div>
                                    <div class="campaign-metric-label">Spend</div>
                                </div>
                                <div class="campaign-metric">
                                    <div class="campaign-metric-value">{fmt_currency(row['total_revenue_usd'])}</div>
                                    <div class="campaign-metric-label">Revenue</div>
                                </div>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

            st.divider()

            # Bottom 5 by ROAS
            st.markdown('<p class="section-title">⚠️ Bottom 5 by ROAS</p>', unsafe_allow_html=True)
            bottom = summary.nsmallest(5, "roas")

            for i, (_, row) in enumerate(bottom.iterrows(), 1):
                ch = row.get("channel", "Other")
                ch_info = CHANNEL_COLORS.get(ch, {"color": "#64748b"})

                st.markdown(f"""
                    <div class="campaign-list">
                        <div class="campaign-item">
                            <div class="campaign-rank">{i}</div>
                            <div class="campaign-info">
                                <div class="campaign-name">{row['campaign_name']}</div>
                                <div class="campaign-channel" style="color: {ch_info['color']};">{ch} • {row.get('campaign_type', 'N/A')}</div>
                            </div>
                            <div class="campaign-metrics">
                                <div class="campaign-metric">
                                    <div class="campaign-metric-value" style="color: #ef4444;">{row['roas']:.2f}x</div>
                                    <div class="campaign-metric-label">ROAS</div>
                                </div>
                                <div class="campaign-metric">
                                    <div class="campaign-metric-value">{fmt_currency(row['total_spend_usd'])}</div>
                                    <div class="campaign-metric-label">Spend</div>
                                </div>
                                <div class="campaign-metric">
                                    <div class="campaign-metric-value">{fmt_currency(row['total_revenue_usd'])}</div>
                                    <div class="campaign-metric-label">Revenue</div>
                                </div>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

    # ── Footer ──
    st.markdown("""
        <div class="footer">
            AdVantage Analytics Pipeline — Built with PySpark + AWS + Streamlit<br>
            <span style="opacity: 0.5;">Marketing Intelligence Platform</span>
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
