import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(page_title="RFM Customer Segmentation", layout="wide", initial_sidebar_state="expanded")

# Load RFM data from CSV
@st.cache_data(ttl=3600)
def load_rfm_data():
    import os
    csv_path = os.path.join(os.path.dirname(__file__), 'customer_segments.csv')
    df = pd.read_csv(csv_path)
    # Rename column if needed
    if 'user_id' in df.columns:
        df.rename(columns={'user_id': 'customer_id'}, inplace=True)
    return df

# Color mapping for segments
SEGMENT_COLORS = {
    0: '#FFD700',      # Champions - Gold
    1: '#4472C4',      # Loyal - Blue
    2: '#808080',      # Lost - Gray
    3: '#FF9900'       # At-Risk - Orange
}

SEGMENT_NAMES = {
    0: 'Champions (VIP)',
    1: 'Loyal Customers',
    2: 'Lost/Churned',
    3: 'At-Risk/Sleepers'
}

def page_executive_overview():
    st.title("Executive Overview - RFM Segmentation")
    
    try:
        rfm_df = load_rfm_data()
        
        # Calculate key metrics
        total_customers = len(rfm_df)
        total_revenue = rfm_df['monetary'].sum()
        avg_ltv = rfm_df['monetary'].mean()
        
        # Segment breakdown
        segment_counts = rfm_df['cluster'].value_counts().sort_index()
        segment_percentages = (segment_counts / total_customers * 100).round(2)
        segment_revenue = rfm_df.groupby('cluster')['monetary'].sum()
        
        # KPI Cards
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Customers", f"{total_customers:,}")
        with col2:
            st.metric("Total Revenue", f"${total_revenue:,.0f}")
        with col3:
            st.metric("Avg Customer LTV", f"${avg_ltv:,.0f}")
        with col4:
            champions_pct = segment_percentages.get(0, 0)
            st.metric("Champions %", f"{champions_pct:.1f}%")
        
        # Row 1: Segment Distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Customer Distribution by Segment")
            segment_data = pd.DataFrame({
                'Segment': [SEGMENT_NAMES[i] for i in segment_counts.index],
                'Count': segment_counts.values,
                'Percentage': segment_percentages.values
            })
            
            fig_pie = go.Figure(data=[go.Pie(
                labels=segment_data['Segment'],
                values=segment_data['Count'],
                marker=dict(colors=[SEGMENT_COLORS[i] for i in segment_counts.index]),
                textinfo='label+percent+value',
                hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<extra></extra>'
            )])
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            st.subheader("Revenue Contribution by Segment")
            revenue_data = pd.DataFrame({
                'Segment': [SEGMENT_NAMES[i] for i in segment_revenue.index],
                'Revenue': segment_revenue.values
            })
            
            fig_revenue = go.Figure(data=[go.Bar(
                x=revenue_data['Segment'],
                y=revenue_data['Revenue'],
                marker=dict(color=[SEGMENT_COLORS[i] for i in segment_revenue.index]),
                text=[f'${v:,.0f}' for v in revenue_data['Revenue']],
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>'
            )])
            fig_revenue.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig_revenue, use_container_width=True)
        
        # Row 2: Key Metrics by Segment
        st.subheader("Segment Characteristics")
        
        metrics_data = []
        for cluster in sorted(rfm_df['cluster'].unique()):
            cluster_df = rfm_df[rfm_df['cluster'] == cluster]
            metrics_data.append({
                'Segment': SEGMENT_NAMES[cluster],
                'Count': len(cluster_df),
                'Percentage': f"{len(cluster_df)/total_customers*100:.1f}%",
                'Avg Recency (days)': f"{cluster_df['recency'].mean():.0f}",
                'Avg Frequency': f"{cluster_df['frequency'].mean():.2f}",
                'Avg Monetary': f"${cluster_df['monetary'].mean():,.0f}",
                'Total Revenue': f"${cluster_df['monetary'].sum():,.0f}"
            })
        
        metrics_table = pd.DataFrame(metrics_data)
        st.dataframe(metrics_table, use_container_width=True, hide_index=True)
        
        # Row 3: Segment Health Status
        st.subheader("Segment Health Status")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Champions", f"{len(rfm_df[rfm_df['cluster']==0]):,}")
            st.caption("Status: Stable - Maintain VIP experience")
        
        with col2:
            st.metric("Loyal", f"{len(rfm_df[rfm_df['cluster']==1]):,}")
            st.caption("Status: Growing - Upsell opportunity")
        
        with col3:
            st.metric("At-Risk", f"{len(rfm_df[rfm_df['cluster']==3]):,}")
            st.caption("Status: Action needed - 1-year inactive")
        
        with col4:
            st.metric("Lost", f"{len(rfm_df[rfm_df['cluster']==2]):,}")
            st.caption("Status: Low priority - 4+ years")
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Make sure the 'rfm_customers' table exists in the database.")

def page_rfm_analysis():
    st.title("RFM Segmentation Analysis")
    
    try:
        rfm_df = load_rfm_data()
        
        # 3D Scatter Plot
        st.subheader("3D RFM Space - Recency vs Frequency vs Monetary")
        
        fig_3d = go.Figure(data=[go.Scatter3d(
            x=rfm_df['recency'],
            y=rfm_df['frequency'],
            z=rfm_df['monetary'],
            mode='markers',
            marker=dict(
                size=4,
                color=rfm_df['cluster'],
                colorscale=[[0, SEGMENT_COLORS[0]], [0.33, SEGMENT_COLORS[1]], 
                           [0.66, SEGMENT_COLORS[2]], [1, SEGMENT_COLORS[3]]],
                showscale=False,
                opacity=0.7
            ),
            text=[SEGMENT_NAMES[c] for c in rfm_df['cluster']],
            hovertemplate='<b>%{text}</b><br>Recency: %{x:.0f}d<br>Frequency: %{y:.2f}<br>Monetary: $%{z:,.0f}<extra></extra>'
        )])
        
        fig_3d.update_layout(
            scene=dict(
                xaxis_title="Recency (days)",
                yaxis_title="Frequency (times)",
                zaxis_title="Monetary ($)"
            ),
            height=600
        )
        st.plotly_chart(fig_3d, use_container_width=True)
        
        # Box Plots for RFM Metrics
        st.subheader("Distribution of RFM Metrics by Segment")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            fig_recency = go.Figure()
            for cluster in sorted(rfm_df['cluster'].unique()):
                cluster_data = rfm_df[rfm_df['cluster'] == cluster]['recency']
                fig_recency.add_trace(go.Box(
                    y=cluster_data,
                    name=SEGMENT_NAMES[cluster],
                    marker_color=SEGMENT_COLORS[cluster]
                ))
            fig_recency.update_layout(title="Recency Distribution", height=400, showlegend=False)
            st.plotly_chart(fig_recency, use_container_width=True)
        
        with col2:
            fig_frequency = go.Figure()
            for cluster in sorted(rfm_df['cluster'].unique()):
                cluster_data = rfm_df[rfm_df['cluster'] == cluster]['frequency']
                fig_frequency.add_trace(go.Box(
                    y=cluster_data,
                    name=SEGMENT_NAMES[cluster],
                    marker_color=SEGMENT_COLORS[cluster]
                ))
            fig_frequency.update_layout(title="Frequency Distribution", height=400, showlegend=False)
            st.plotly_chart(fig_frequency, use_container_width=True)
        
        with col3:
            fig_monetary = go.Figure()
            for cluster in sorted(rfm_df['cluster'].unique()):
                cluster_data = rfm_df[rfm_df['cluster'] == cluster]['monetary']
                fig_monetary.add_trace(go.Box(
                    y=cluster_data,
                    name=SEGMENT_NAMES[cluster],
                    marker_color=SEGMENT_COLORS[cluster]
                ))
            fig_monetary.update_layout(title="Monetary Distribution", height=400, showlegend=False)
            st.plotly_chart(fig_monetary, use_container_width=True)
        
        # RFM Heatmap
        st.subheader("RFM Segmentation Logic")
        
        rfm_summary = []
        for cluster in sorted(rfm_df['cluster'].unique()):
            cluster_df = rfm_df[rfm_df['cluster'] == cluster]
            rfm_summary.append({
                'Segment': SEGMENT_NAMES[cluster],
                'Recency': f"{cluster_df['recency'].mean():.0f}d",
                'Frequency': f"{cluster_df['frequency'].mean():.2f}x",
                'Monetary': f"${cluster_df['monetary'].mean():,.0f}",
                'Count': len(cluster_df)
            })
        
        rfm_table = pd.DataFrame(rfm_summary)
        st.dataframe(rfm_table, use_container_width=True, hide_index=True)
    
    except Exception as e:
        st.error(f"Error loading data: {e}")

def page_high_value():
    st.title("High-Value Customers: Champions & Loyal")
    
    try:
        rfm_df = load_rfm_data()
        high_value = rfm_df[rfm_df['cluster'].isin([0, 1])]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Champions", f"{len(rfm_df[rfm_df['cluster']==0]):,}")
            st.metric("Loyal Customers", f"{len(rfm_df[rfm_df['cluster']==1]):,}")
        
        with col2:
            champions_revenue = rfm_df[rfm_df['cluster']==0]['monetary'].sum()
            loyal_revenue = rfm_df[rfm_df['cluster']==1]['monetary'].sum()
            st.metric("Champions Revenue", f"${champions_revenue:,.0f}")
            st.metric("Loyal Revenue", f"${loyal_revenue:,.0f}")
        
        # Purchase Frequency Distribution
        st.subheader("Purchase Frequency Distribution")
        
        fig_freq = go.Figure()
        for cluster in [0, 1]:
            cluster_data = rfm_df[rfm_df['cluster'] == cluster]
            fig_freq.add_trace(go.Histogram(
                x=cluster_data['frequency'],
                name=SEGMENT_NAMES[cluster],
                marker_color=SEGMENT_COLORS[cluster],
                opacity=0.7,
                nbinsx=20
            ))
        
        fig_freq.update_layout(
            barmode='overlay',
            title="Frequency Distribution: Champions vs Loyal",
            xaxis_title="Number of Purchases",
            yaxis_title="Customer Count",
            height=400
        )
        st.plotly_chart(fig_freq, use_container_width=True)
        
        # Average Order Value
        st.subheader("Average Order Value (AOV) by Segment")
        
        aov_data = []
        for cluster in sorted(rfm_df['cluster'].unique()):
            cluster_df = rfm_df[rfm_df['cluster'] == cluster]
            aov = cluster_df['monetary'].sum() / cluster_df['frequency'].sum()
            aov_data.append({
                'Segment': SEGMENT_NAMES[cluster],
                'AOV': aov
            })
        
        aov_df = pd.DataFrame(aov_data)
        
        fig_aov = go.Figure(data=[go.Bar(
            x=aov_df['Segment'],
            y=aov_df['AOV'],
            marker=dict(color=[SEGMENT_COLORS[i] for i in sorted(rfm_df['cluster'].unique())]),
            text=[f'${v:,.0f}' for v in aov_df['AOV']],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>AOV: $%{y:,.0f}<extra></extra>'
        )])
        fig_aov.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig_aov, use_container_width=True)
        
        # Time Between Purchases
        st.subheader("Purchase Frequency Insights")
        
        insights = []
        for cluster in [0, 1]:
            cluster_df = rfm_df[rfm_df['cluster'] == cluster]
            avg_days_between = cluster_df['recency'].mean() / cluster_df['frequency'].mean()
            insights.append({
                'Segment': SEGMENT_NAMES[cluster],
                'Avg Days Between Purchases': f"{avg_days_between:.0f}",
                'Avg Recency': f"{cluster_df['recency'].mean():.0f}d",
                'Avg Frequency': f"{cluster_df['frequency'].mean():.2f}x",
                'Recommended Re-engagement': f"Every {max(7, int(avg_days_between * 0.8))} days"
            })
        
        insights_df = pd.DataFrame(insights)
        st.dataframe(insights_df, use_container_width=True, hide_index=True)
    
    except Exception as e:
        st.error(f"Error loading data: {e}")

def page_at_risk():
    st.title("At-Risk & Lost Customer Analysis")
    
    try:
        rfm_df = load_rfm_data()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("At-Risk Customers", f"{len(rfm_df[rfm_df['cluster']==3]):,}")
            st.metric("Lost Customers", f"{len(rfm_df[rfm_df['cluster']==2]):,}")
        
        with col2:
            at_risk_revenue = rfm_df[rfm_df['cluster']==3]['monetary'].sum()
            lost_revenue = rfm_df[rfm_df['cluster']==2]['monetary'].sum()
            st.metric("At-Risk Revenue", f"${at_risk_revenue:,.0f}")
            st.metric("Lost Revenue", f"${lost_revenue:,.0f}")
        
        # Recency Timeline
        st.subheader("Days Since Last Purchase (Recency)")
        
        fig_recency = go.Figure()
        for cluster in [3, 2]:
            cluster_data = rfm_df[rfm_df['cluster'] == cluster]
            fig_recency.add_trace(go.Box(
                y=cluster_data['recency'],
                name=SEGMENT_NAMES[cluster],
                marker_color=SEGMENT_COLORS[cluster],
                boxmean='sd'
            ))
        
        fig_recency.update_layout(
            title="Recency Distribution: At-Risk vs Lost",
            yaxis_title="Days Since Last Purchase",
            height=400
        )
        st.plotly_chart(fig_recency, use_container_width=True)
        
        # Churn Risk Assessment
        st.subheader("Churn Risk Assessment")
        
        risk_assessment = []
        for cluster in [0, 1, 3, 2]:
            cluster_df = rfm_df[rfm_df['cluster'] == cluster]
            risk_score = (cluster_df['recency'].mean() / rfm_df['recency'].max()) * 100
            risk_assessment.append({
                'Segment': SEGMENT_NAMES[cluster],
                'Avg Days Since Purchase': f"{cluster_df['recency'].mean():.0f}",
                'Churn Risk': 'Low' if risk_score < 20 else 'Medium' if risk_score < 60 else 'High',
                'Recommended Action': 'Maintain VIP' if cluster == 0 else 'Upsell campaign' if cluster == 1 else '"We miss you" promotion' if cluster == 3 else 'Low-cost remarketing'
            })
        
        risk_df = pd.DataFrame(risk_assessment)
        st.dataframe(risk_df, use_container_width=True, hide_index=True)
        
        # Reactivation Potential
        st.subheader("Reactivation Potential by Segment")
        
        potential_data = []
        for cluster in [3, 2]:
            cluster_df = rfm_df[rfm_df['cluster'] == cluster]
            # Score based on historical frequency and monetary
            reactivation_score = (cluster_df['frequency'].mean() / rfm_df['frequency'].max()) * 100
            potential_data.append({
                'Segment': SEGMENT_NAMES[cluster],
                'Reactivation Score': reactivation_score
            })
        
        potential_df = pd.DataFrame(potential_data)
        
        fig_potential = go.Figure(data=[go.Bar(
            x=potential_df['Segment'],
            y=potential_df['Reactivation Score'],
            marker=dict(color=[SEGMENT_COLORS[i] for i in [3, 2]]),
            text=[f'{v:.1f}%' for v in potential_df['Reactivation Score']],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Score: %{y:.1f}%<extra></extra>'
        )])
        fig_potential.update_layout(height=400)
        st.plotly_chart(fig_potential, use_container_width=True)
    
    except Exception as e:
        st.error(f"Error loading data: {e}")

def page_marketing_plan():
    st.title("Marketing Action Plan")
    
    try:
        rfm_df = load_rfm_data()
        
        st.subheader("Segment Strategy & Recommendations")
        
        # Marketing Action Matrix
        marketing_actions = [
            {
                'Segment': 'Champions',
                'Recommended Actions': [
                    '✓ VIP Account Manager assigned',
                    '✓ Birthday gifts & exclusive events',
                    '✓ Early access to new products',
                    '✓ Premium customer support'
                ],
                'Est. ROI': 'Retention focused',
                'Priority': 'P1 - Ongoing',
                'Timeline': 'Quarterly check-ins'
            },
            {
                'Segment': 'Loyal',
                'Recommended Actions': [
                    '✓ Upsell to premium products',
                    '✓ Cross-sell related items',
                    '✓ Membership loyalty program',
                    '✓ Monthly personalized offers'
                ],
                'Est. ROI': '25-35% LTV increase',
                'Priority': 'P1 - High',
                'Timeline': 'Monthly campaigns'
            },
            {
                'Segment': 'At-Risk',
                'Recommended Actions': [
                    '✓ "We miss you" email campaign',
                    '✓ 25-30% discount codes',
                    '✓ SMS urgent offers',
                    '✓ Limited time (48-72hr) promotions'
                ],
                'Est. ROI': '15-25% recovery',
                'Priority': 'P2 - Medium',
                'Timeline': 'Every 90 days'
            },
            {
                'Segment': 'Lost/Churned',
                'Recommended Actions': [
                    '✓ Low-cost remarketing only',
                    '✓ Display ads (cheap placement)',
                    '✓ Automated email sequences',
                    '✓ Skip aggressive campaigns'
                ],
                'Est. ROI': '2-5% recovery',
                'Priority': 'P3 - Low',
                'Timeline': 'Automated/passive'
            }
        ]
        
        for i, action in enumerate(marketing_actions):
            with st.expander(f"{action['Segment']} - {action['Priority']}", expanded=(i==0)):
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Recommended Actions:**")
                    for act in action['Recommended Actions']:
                        st.write(act)
                
                with col2:
                    st.write("**Campaign Details:**")
                    st.metric("Est. ROI", action['Est. ROI'])
                    st.metric("Timeline", action['Timeline'])
        
        # Revenue Impact Projection
        st.subheader("Projected Revenue Impact")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            current_revenue = rfm_df['monetary'].sum()
            st.metric("Current Total Revenue", f"${current_revenue:,.0f}")
        
        with col2:
            # Project 20% of At-Risk moving to Loyal
            at_risk_revenue = rfm_df[rfm_df['cluster']==3]['monetary'].sum()
            recovery = at_risk_revenue * 0.20 * 2  # 20% recover + double spend as Loyal
            st.metric("Recovery Opportunity", f"${recovery:,.0f}")
        
        with col3:
            # Project 15% of Loyal moving to Champions
            loyal_revenue = rfm_df[rfm_df['cluster']==1]['monetary'].sum()
            upsell = loyal_revenue * 0.15 * 1.5  # 15% upsell + 50% more spend
            st.metric("Upsell Opportunity", f"${upsell:,.0f}")
        
        # Segment Performance Comparison
        st.subheader("Current vs Potential Revenue by Segment")
        
        segments_revenue = []
        for cluster in sorted(rfm_df['cluster'].unique()):
            cluster_df = rfm_df[rfm_df['cluster'] == cluster]
            current = cluster_df['monetary'].sum()
            
            # Calculate potential based on strategic initiatives
            if cluster == 0:
                potential = current * 1.05  # 5% growth through retention
            elif cluster == 1:
                potential = current * 1.35  # 35% growth through upsell
            elif cluster == 3:
                potential = current * 1.25  # 25% growth through reactivation
            else:  # Lost
                potential = current * 1.05  # 5% low-cost recovery
            
            segments_revenue.append({
                'Segment': SEGMENT_NAMES[cluster],
                'Current': current,
                'Potential': potential
            })
        
        rev_df = pd.DataFrame(segments_revenue)
        
        fig_revenue = go.Figure(data=[
            go.Bar(name='Current Revenue', x=rev_df['Segment'], y=rev_df['Current'], marker_color='lightblue'),
            go.Bar(name='Potential Revenue', x=rev_df['Segment'], y=rev_df['Potential'], marker_color='darkblue')
        ])
        
        fig_revenue.update_layout(
            barmode='group',
            title="Revenue: Current vs Strategic Potential",
            yaxis_title="Revenue ($)",
            height=400,
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig_revenue, use_container_width=True)
        
        # Campaign Timing
        st.subheader("Campaign Calendar & Timing")
        
        campaign_timing = pd.DataFrame({
            'Segment': ['Champions', 'Loyal', 'At-Risk', 'Lost'],
            'Campaign Frequency': ['Quarterly', 'Monthly', 'Every 90 days', 'Ongoing/Passive'],
            'Best Channel': ['Email + Phone', 'Email + Push', 'SMS + Email', 'Display Ads'],
            'Offer Type': ['Exclusive', 'Premium products', 'Urgent discount', 'Remarketing'],
            'Optimal Timing': ['Q1, Q3, Q4', 'Start of month', 'When 90d inactive', 'Always active']
        })
        
        st.dataframe(campaign_timing, use_container_width=True, hide_index=True)
    
    except Exception as e:
        st.error(f"Error loading data: {e}")

def main():
    st.sidebar.title("RFM Dashboard")
    
    page = st.sidebar.radio(
        "Select Dashboard:",
        [
            "Executive Overview",
            "RFM Analysis",
            "Champions & Loyal",
            "At-Risk & Lost",
            "Marketing Plan"
        ]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.info(
        "**Quick Facts:**\n"
        "• 4 customer segments\n"
        "• Champions: 11%, highest LTV\n"
        "• At-Risk: 44%, reactivation focus\n"
        "• Lost: 11%, low priority"
    )
    
    try:
        if page == "Executive Overview":
            page_executive_overview()
        elif page == "RFM Analysis":
            page_rfm_analysis()
        elif page == "Champions & Loyal":
            page_high_value()
        elif page == "At-Risk & Lost":
            page_at_risk()
        elif page == "Marketing Plan":
            page_marketing_plan()
    
    except Exception as e:
        st.error(f"Error loading dashboard: {e}")
        st.info("Please ensure all required tables exist in the database.")

if __name__ == "__main__":
    main()
