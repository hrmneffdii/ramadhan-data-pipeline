# 📊 Ramadhan Seasonal Data Pipeline (ETL)

End-to-end data engineering project to analyze seasonal commodity trends during the Ramadhan period using Google Trends data.  
This project simulates a real-world ETL pipeline built with a modular structure and reproducible environment.

## 🚀 Project Overview
This pipeline extracts search trend data (e.g., "takjil", "baju lebaran", "snack lebaran") to identify hype movement of commodities during the Ramadhan season in Indonesia.

The goal of this project:
- Practice real-world ETL pipeline design
- Build a data engineering portfolio project
- Analyze seasonal demand signals for commodities

## 🏗️ Project Structure
```

ramadhan-data-pipeline/
├── etl/            # Extract logic (Google Trends)
├── config/         # Configuration files
├── data/           # Raw & processed data (ignored in git)
├── logs/           # Pipeline logs
├── main.py         # Pipeline entry point
├── requirements.txt
├── .gitignore
└── README.md

````

## ⚙️ Tech Stack
- Python 3.12
- Pandas
- Pytrends (Google Trends API)
- Ubuntu (local development)
- Git & GitHub (version control)

## 🔄 Pipeline Architecture
ETL Flow:
1. Extract → Fetch Google Trends data (Indonesia, last 3 months)
2. Transform → Clean & structure time-series data
3. Load → Save into local storage (CSV / future warehouse)

## 📥 Data Source
- Google Trends (via Pytrends)
- Keywords example:
  - Takjil
  - Baju Lebaran
  - Snack Lebaran

## 🛠️ Setup & Installation
Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/ramadhan-data-pipeline.git
cd ramadhan-data-pipeline
````

Create virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## ▶️ Run the Pipeline

```bash
python etl/extract_trends.py
```

Output will be saved in:

```
data/trends_raw.csv
```

## 📌 Notes

* `venv/`, `data/`, and `logs/` are excluded from Git for best practices
* Designed to be scalable to cloud (AWS/GCP) in future iterations
* Built on a low-spec machine (Core i3, 12GB RAM) to simulate realistic constraints

## 🎯 Future Improvements

* Add data warehouse (SQLite / PostgreSQL)
* Orchestrate with Airflow
* Deploy pipeline to AWS (S3 + Lambda + Glue)
* Dashboard visualization (Power BI / Streamlit)
