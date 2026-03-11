import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="E-Commerce Analytics", layout="wide")
st.title("E-Commerce Analytics Dashboard")
st.markdown("---")

daily_revenue = pd.read_csv("/content/daily_revenue.csv")
top_products  = pd.read_csv("/content/top_products.csv")
customers     = pd.read_csv("/content/customer_segments.csv")
daily_revenue["Date"] = pd.to_datetime(daily_revenue["Date"])

# KPI METRICS
st.header("Key Metrics")
col1, col2, col3, col4 = st.columns(4)
total_revenue   = daily_revenue["daily_revenue"].sum()
total_orders    = daily_revenue["total_orders"].sum()
total_customers = customers["CustomerID"].nunique()
avg_order_value = customers["avg_order_value"].mean()
col1.metric("Total Revenue",   f"GBP {total_revenue:,.0f}")
col2.metric("Total Orders",    f"{total_orders:,.0f}")
col3.metric("Total Customers", f"{total_customers:,.0f}")
col4.metric("Avg Order Value", f"GBP {avg_order_value:,.2f}")
st.markdown("---")

# DAILY REVENUE TREND
st.header("Daily Revenue Trend")
uk_revenue = daily_revenue[daily_revenue["Country"] == "United Kingdom"].groupby("Date")["daily_revenue"].sum().reset_index()
fig1 = px.line(uk_revenue, x="Date", y="daily_revenue",
    title="Daily Revenue United Kingdom",
    color_discrete_sequence=["#2196F3"])
st.plotly_chart(fig1, use_container_width=True)
st.markdown("---")

# TOP 10 PRODUCTS
st.header("Top 10 Products by Revenue")
top10 = top_products.head(10)
fig2 = px.bar(top10, x="total_revenue", y="Description",
    orientation="h",
    title="Top 10 Products by Revenue",
    color="total_revenue",
    color_continuous_scale="Blues")
fig2.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig2, use_container_width=True)
st.markdown("---")

# CUSTOMER SEGMENTATION
st.header("Customer Segmentation")
col1, col2 = st.columns(2)
segment_counts = customers["customer_segment"].value_counts().reset_index()
segment_counts.columns = ["segment", "count"]
fig3 = px.pie(segment_counts, values="count", names="segment",
    title="Customer Segments Distribution",
    color_discrete_sequence=["#4CAF50", "#2196F3", "#FF9800"])
col1.plotly_chart(fig3, use_container_width=True)
segment_revenue = customers.groupby("customer_segment")["total_spent"].sum().reset_index()
fig4 = px.bar(segment_revenue, x="customer_segment", y="total_spent",
    title="Revenue by Customer Segment",
    color="customer_segment",
    color_discrete_sequence=["#4CAF50", "#2196F3", "#FF9800"])
col2.plotly_chart(fig4, use_container_width=True)
st.markdown("---")

# REVENUE BY COUNTRY
st.header("Revenue by Country")
country_revenue = daily_revenue.groupby("Country")["daily_revenue"].sum().reset_index().sort_values("daily_revenue", ascending=False).head(10)
fig5 = px.bar(country_revenue, x="Country", y="daily_revenue",
    title="Top 10 Countries by Revenue",
    color="daily_revenue",
    color_continuous_scale="Blues")
st.plotly_chart(fig5, use_container_width=True)
st.markdown("---")
st.markdown("Built with PySpark + Delta Lake + Streamlit")
