import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import psycopg2

st.set_page_config(
    page_title="RFM Customer Segmentation",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_URL = "postgresql://data_analyst:Visualize405@ep-sweet-king-aoq24ws3-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

SEGMENT_COLORS = {
    "best":     "#FFD700",
    "loyal":    "#4472C4",
    "at_risk":  "#FF9900",
    "churned":  "#808080",
}

SEGMENT_NAMES = {
    "best":    "Best Customers",
    "loyal":   "Loyal Customers",
    "at_risk": "At-Risk Customers",
    "churned": "Churned Customers",
}

SEGMENT_ORDER = ["best", "loyal", "at_risk", "churned"]


# ──────────────────────────── Data ────────────────────────────

@st.cache_data(ttl=3600)
def load_rfm_data():
    if not DB_URL:
        st.error("Chưa cấu hình DB_URL.")
        return None
    try:
        conn = psycopg2.connect(DB_URL)
        df = pd.read_sql(
            """
            SELECT customer_id, recency, frequency, monetary, cluster, segment_label
            FROM analytics.rfm_customers
            ORDER BY customer_id
            """,
            conn,
        )
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None


# ──────────────────────────── Helpers ────────────────────────────

def present_segments(rfm_df, candidates=None):
    """Trả về list segment có trong dữ liệu, theo thứ tự SEGMENT_ORDER."""
    candidates = candidates or SEGMENT_ORDER
    existing = set(rfm_df["segment_label"].unique())
    return [s for s in candidates if s in existing]


def make_box_plot(rfm_df, column, title, segments):
    fig = go.Figure()
    for seg in segments:
        seg_df = rfm_df[rfm_df["segment_label"] == seg]
        if seg_df.empty:
            continue
        fig.add_trace(
            go.Box(
                y=seg_df[column],
                name=SEGMENT_NAMES[seg],
                marker_color=SEGMENT_COLORS[seg],
            )
        )
    fig.update_layout(title=title, height=400, showlegend=False)
    return fig


# ──────────────────────────── Pages ────────────────────────────

def page_executive_overview(rfm_df):
    st.title("Executive Overview")

    total_customers   = len(rfm_df)
    total_revenue     = rfm_df["monetary"].sum()
    avg_ltv           = rfm_df["monetary"].mean()
    segments          = present_segments(rfm_df)
    segment_counts    = rfm_df["segment_label"].value_counts()
    segment_revenue   = rfm_df.groupby("segment_label")["monetary"].sum()

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Customers", f"{total_customers:,}")
    with col2:
        st.metric("Total Revenue", f"${total_revenue:,.0f}")
    with col3:
        st.metric("Avg Customer LTV", f"${avg_ltv:,.0f}")
    with col4:
        best_pct = segment_counts.get("best", 0) / total_customers * 100
        st.metric("Best Customers %", f"{best_pct:.1f}%")

    # Charts
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Customer Distribution by Segment")
        fig_pie = go.Figure(data=[go.Pie(
            labels=[SEGMENT_NAMES[s] for s in segments],
            values=[segment_counts.get(s, 0) for s in segments],
            marker=dict(colors=[SEGMENT_COLORS[s] for s in segments]),
            textinfo="label+percent+value",
            hovertemplate="<b>%{label}</b><br>Count: %{value:,}<br>%{percent}<extra></extra>",
        )])
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.subheader("Revenue Contribution by Segment")
        fig_revenue = go.Figure(data=[go.Bar(
            x=[SEGMENT_NAMES[s] for s in segments],
            y=[segment_revenue.get(s, 0) for s in segments],
            marker=dict(color=[SEGMENT_COLORS[s] for s in segments]),
            text=[f"${segment_revenue.get(s, 0):,.0f}" for s in segments],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>",
        )])
        fig_revenue.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig_revenue, use_container_width=True)

    # Segment Summary Table
    st.subheader("Segment Summary")
    rows = []
    for seg in segments:
        seg_df = rfm_df[rfm_df["segment_label"] == seg]
        rows.append({
            "Segment":           SEGMENT_NAMES[seg],
            "Count":             len(seg_df),
            "%":                 f"{len(seg_df) / total_customers * 100:.1f}%",
            "Avg Recency (days)": f"{seg_df['recency'].mean():.0f}",
            "Avg Frequency":     f"{seg_df['frequency'].mean():.2f}",
            "Avg Monetary":      f"${seg_df['monetary'].mean():,.0f}",
            "Total Revenue":     f"${seg_df['monetary'].sum():,.0f}",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def page_rfm_analysis(rfm_df):
    st.title("RFM Analysis")
    segments = present_segments(rfm_df)

    # 3D Scatter
    st.subheader("3D RFM Space — Recency · Frequency · Monetary")
    fig_3d = go.Figure()
    for seg in segments:
        seg_df = rfm_df[rfm_df["segment_label"] == seg]
        fig_3d.add_trace(go.Scatter3d(
            x=seg_df["recency"],
            y=seg_df["frequency"],
            z=seg_df["monetary"],
            mode="markers",
            name=SEGMENT_NAMES[seg],
            marker=dict(size=4, color=SEGMENT_COLORS[seg], opacity=0.7),
            hovertemplate=(
                f"<b>{SEGMENT_NAMES[seg]}</b><br>"
                "Recency: %{x:.0f}d<br>Frequency: %{y:.2f}<br>Monetary: $%{z:,.0f}<extra></extra>"
            ),
        ))
    fig_3d.update_layout(
        scene=dict(
            xaxis_title="Recency (days)",
            yaxis_title="Frequency",
            zaxis_title="Monetary ($)",
        ),
        height=600,
    )
    st.plotly_chart(fig_3d, use_container_width=True)

    # Box plots
    st.subheader("Distribution of RFM Metrics by Segment")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.plotly_chart(
            make_box_plot(rfm_df, "recency", "Recency (days)", segments),
            use_container_width=True,
        )
    with col2:
        st.plotly_chart(
            make_box_plot(rfm_df, "frequency", "Frequency (times)", segments),
            use_container_width=True,
        )
    with col3:
        st.plotly_chart(
            make_box_plot(rfm_df, "monetary", "Monetary ($)", segments),
            use_container_width=True,
        )


def page_high_value(rfm_df):
    st.title("Best & Loyal Customers")

    focus_segments = present_segments(rfm_df, candidates=["best", "loyal"])
    if not focus_segments:
        st.info("No 'best' or 'loyal' segments found in the data.")
        return

    # Frequency Distribution
    st.subheader("Purchase Frequency Distribution")
    fig_freq = go.Figure()
    for seg in focus_segments:
        seg_df = rfm_df[rfm_df["segment_label"] == seg]
        fig_freq.add_trace(go.Histogram(
            x=seg_df["frequency"],
            name=SEGMENT_NAMES[seg],
            marker_color=SEGMENT_COLORS[seg],
            opacity=0.7,
            nbinsx=20,
        ))
    fig_freq.update_layout(
        barmode="overlay",
        xaxis_title="Number of Purchases",
        yaxis_title="Customer Count",
        height=400,
    )
    st.plotly_chart(fig_freq, use_container_width=True)

    # AOV — Đã sửa đổi để chỉ hiển thị cho focus_segments nhằm tránh loãng thông tin
    st.subheader("Average Order Value (AOV) by Segment")
    aov_rows = []
    for seg in focus_segments:
        seg_df = rfm_df[rfm_df["segment_label"] == seg]
        freq_sum = seg_df["frequency"].sum()
        aov = seg_df["monetary"].sum() / freq_sum if freq_sum > 0 else 0
        aov_rows.append({"segment": seg, "label": SEGMENT_NAMES[seg], "aov": aov})

    fig_aov = go.Figure(data=[go.Bar(
        x=[r["label"] for r in aov_rows],
        y=[r["aov"]   for r in aov_rows],
        marker=dict(color=[SEGMENT_COLORS[r["segment"]] for r in aov_rows]),
        text=[f"${r['aov']:,.0f}" for r in aov_rows],
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>AOV: $%{y:,.0f}<extra></extra>",
    )])
    fig_aov.update_layout(height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig_aov, use_container_width=True)

    # Re-engagement Timing
    st.subheader("Re-engagement Timing")
    insights = []
    for seg in focus_segments:
        seg_df = rfm_df[rfm_df["segment_label"] == seg]
        freq_mean    = seg_df["frequency"].mean()
        recency_mean = seg_df["recency"].mean()
        avg_days_between = recency_mean / freq_mean if freq_mean > 0 else None
        insights.append({
            "Segment":                    SEGMENT_NAMES[seg],
            "Avg Recency (days)":         f"{recency_mean:.0f}",
            "Avg Frequency":              f"{freq_mean:.2f}x",
            "Est. Days Between Purchases": f"{avg_days_between:.0f}" if avg_days_between else "N/A",
            "Recommended Re-engagement":  (
                f"Every {max(7, int(avg_days_between * 0.8))} days"
                if avg_days_between else "N/A"
            ),
        })
    st.dataframe(pd.DataFrame(insights), use_container_width=True, hide_index=True)


def page_at_risk(rfm_df):
    st.title("At-Risk & Churned Customers")

    focus_segments = present_segments(rfm_df, candidates=["at_risk", "churned"])
    if not focus_segments:
        st.info("No 'at_risk' or 'churned' segments found in the data.")
        return

    # Recency distribution
    st.subheader("Days Since Last Purchase")
    fig_recency = go.Figure()
    for seg in focus_segments:
        seg_df = rfm_df[rfm_df["segment_label"] == seg]
        fig_recency.add_trace(go.Box(
            y=seg_df["recency"],
            name=SEGMENT_NAMES[seg],
            marker_color=SEGMENT_COLORS[seg],
            boxmean="sd",
        ))
    fig_recency.update_layout(
        yaxis_title="Days Since Last Purchase",
        height=400,
    )
    st.plotly_chart(fig_recency, use_container_width=True)

    # Churn Risk & Action Plan — Đã sửa đổi để chỉ lặp qua focus_segments thay vì toàn bộ rfm_df
    st.subheader("Churn Risk & Action Plan")
    max_recency = rfm_df["recency"].max()
    action_map = {
        "at_risk": '"We miss you" promotion',
        "churned": "Low-cost remarketing",
    }
    risk_rows = []
    for seg in focus_segments:
        seg_df     = rfm_df[rfm_df["segment_label"] == seg]
        avg_recency = seg_df["recency"].mean()
        risk_score  = (avg_recency / max_recency * 100) if max_recency > 0 else 0
        risk_rows.append({
            "Segment":                 SEGMENT_NAMES[seg],
            "Avg Days Since Purchase": f"{avg_recency:.0f}",
            "Churn Risk":              "Low" if risk_score < 20 else "Medium" if risk_score < 60 else "High",
            "Recommended Action":      action_map.get(seg, ""),
        })
    st.dataframe(pd.DataFrame(risk_rows), use_container_width=True, hide_index=True)


# ──────────────────────────── Main ────────────────────────────

def main():
    st.sidebar.title("RFM Dashboard")
    st.sidebar.caption("Data Source: PostgreSQL (Neon)")
    st.sidebar.markdown("---")
    
    rfm_df = load_rfm_data()

    # Kiểm tra dữ liệu tập trung ở main, dừng toàn bộ app nếu không có data.
    if rfm_df is None or rfm_df.empty:
        st.sidebar.error("Data Status: Disconnected")
        st.warning("No data available. Please check the database connection.")
        st.stop()
        
    st.sidebar.success("Data Status: Connected")
    
    page = st.sidebar.radio(
        "Navigation",
        ["Executive Overview", "RFM Analysis", "Best & Loyal", "At-Risk & Churned"],
    )

    try:
        if page == "Executive Overview":
            page_executive_overview(rfm_df)
        elif page == "RFM Analysis":
            page_rfm_analysis(rfm_df)
        elif page == "Best & Loyal":
            page_high_value(rfm_df)
        elif page == "At-Risk & Churned":
            page_at_risk(rfm_df)
    except Exception as e:
        st.error(f"Error rendering dashboard: {e}")


if __name__ == "__main__":
    main()