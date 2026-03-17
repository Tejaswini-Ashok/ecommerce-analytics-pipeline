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

<img width="1366" height="768" alt="Screenshot (8)" src="https://github.com/user-attachments/assets/7211d8ec-0167-4df7-9c81-a14fa44b6d1c" />
<img width="1366" height="768" alt="Screenshot (9)" src="https://github.com/user-attachments/assets/069cc59e-48d8-4113-8464-e1a081dfcd47" />
<img width="1366" height="768" alt="Screenshot (10)" src="https://github.com/user-attachments/assets/9c5cf75a-4361-4fe5-b4d6-8bfa63d5cdc2" />
<img width="1366" height="768" alt="Screenshot (11)" src="https://github.com/user-attachments/assets/6bd6d0e6-d99f-4d82-adac-916bf0088e9c" />
<img width="1366" height="768" alt="Screenshot (12)" src="https://github.com/user-attachments/assets/442ba760-abfd-41a8-a724-c98cb73e44d1" />

<img width="1408" height="768" alt="image_ce3e4e6a" src="https://github.com/user-attachments/assets/e888fa1b-3f17-47ca-9e5f-785508bcf1da" />






