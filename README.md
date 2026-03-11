# End-to-End E-Commerce Batch Analytics Pipeline

## Overview
End-to-end data engineering pipeline processing 
500K+ e-commerce transactions using Medallion 
Architecture (Bronze/Silver/Gold) with PySpark, 
Delta Lake, and Streamlit dashboard.

## Architecture
Raw CSV → Bronze (Delta) → Silver (Delta) → Gold (Delta) → Streamlit Dashboard

## Tech Stack
- PySpark — distributed data processing
- Delta Lake — ACID transactions, Time Travel, MERGE
- Apache Airflow — pipeline orchestration
- Streamlit — interactive dashboard
- Python — scripting

## Key Features
- Medallion Architecture (Bronze/Silver/Gold)
- Delta Lake MERGE for incremental upserts
- Time Travel to query historical versions
- SCD Type 2 customer dimension tracking
- Customer segmentation (VIP/Regular/Occasional)
- Interactive Streamlit dashboard

## Pipeline Results
| Layer  | Rows    | Description              |
|--------|---------|--------------------------|
| Bronze | 541,909 | Raw data, untouched      |
| Silver | 387,841 | Cleaned, validated       |
| Gold   | 3 tables| Business KPIs            |

## Dashboard
- Daily revenue trends
- Top 10 products by revenue
- Customer segmentation analysis
- Revenue by country

## How to Run
1. Install dependencies: pip install -r requirements.txt
2. Run notebook: ecommerce_pipeline.ipynb
3. Run dashboard: streamlit run dashboard.py

## Dataset
UCI E-Commerce Dataset from Kaggle
500K+ transactions from UK retail company (2010-2011)