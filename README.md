# ETL-WeatherApi-AirQuality

🌤️ ETL Projects – Weather API & Urban Air Quality Monitoring

This repository contains two end-to-end ETL pipelines implemented in Python:

Weather Data ETL Pipeline – Hourly weather forecasts for cities like Hyderabad

Urban Air Quality Monitoring ETL Pipeline – Hourly pollutant data for major Indian metro cities

Both pipelines implement Extract → Transform → Load → Analyze, with automated Supabase integration, feature engineering, and visual analytics.

1️⃣ Weather API ETL Pipeline – Multi-City Hourly Forecasts

Project Overview

End-to-end ETL for fetching, transforming, loading, and analyzing hourly weather data via Open-Meteo Weather API

Tracks Temperature etc

Generates derived features: temperature category (very_cold → hot), feels-like temperature

Loads processed data into Supabase and creates automated plots & summary metrics

What I implemented:

Extraction with retry logic & error handling

JSON flattening, cleaning, feature engineering in transformation

Supabase integration with batch inserts and NaN → NULL handling

Automated analysis & plots (histogram, daily avg temperature)

Full pipeline runner run_pipeline.py

Project Directory
```
weather_api_etl/
│
├── data/
│   ├── raw/                  # Raw JSON from API
│   ├── staged/               # Transformed CSV
│   └── processed/            # Analysis CSV + plots
├── extract.py                # Fetches weather data
├── transform.py              # Flatten & feature engineer
├── load.py                   # Load to Supabase
├── etl_analysis.py           # KPIs & visualizations
├── run_pipeline.py           # Full pipeline automation
└── .env
```

Example Outputs

Metric	Value
Average Temperature (°C)	28.4
Data Collected From	2025-12-10 00:00 → 23:00

Plots
```

temperature_hist.png → Temperature distribution

daily_avg_temp.png → Daily average temperature trends
```
<img width="800" height="400" alt="image" src="https://github.com/user-attachments/assets/ff25f258-8dca-429a-bb64-c62c76de7dc5" />


2️⃣ Urban Air Quality Monitoring – Multi-City ETL Pipeline

Project Overview

Monitors air quality in Delhi, Mumbai, Bengaluru, Hyderabad, and Kolkata

Fetches hourly pollutant data from Open-Meteo Air Quality API

Pollutants: PM2.5, PM10, CO, NO₂, SO₂, O₃, UV Index

Generates AQI category, severity score, risk classification

Loads into Supabase, performs automated analysis, and saves visualizations

What I implemented:

Extraction of hourly pollutant data for 5 cities with retry logic & logging

JSON flattening, cleaning, and feature engineering:

AQI category based on PM2.5

Weighted severity score

Risk flag (Low/Moderate/High)

Supabase table creation & batch inserts (size=200)

KPI metrics: city with highest PM2.5, severity score, risk distribution

Trend plots & CSV reports

Full pipeline automation via run_pipeline.py

Project Directory
```
air_quality_etl/
│
├── data/
│   ├── raw/                  # Raw JSON from API
│   ├── staged/               # Transformed CSV
│   └── processed/            # Analysis CSV + plots
├── extract.py                # Fetches pollutant data
├── transform.py              # Flatten & feature engineer
├── load.py                   # Load to Supabase
├── etl_analysis.py           # KPIs & visualizations
├── run_pipeline.py           # Full pipeline automation
└── .env
```

Example Outputs

KPI	Value
City with highest PM2.5	Delhi
City with highest severity	Mumbai
% High Risk Hours	12%
Hour of worst AQI	17:00

Plots

pm2_5_hist.png → PM2.5 distribution


<img width="800" height="400" alt="image" src="https://github.com/user-attachments/assets/77934742-61eb-4a67-9c0f-ff7de9efd503" />


risk_flags_per_city.png → Risk classification per city


<img width="640" height="480" alt="image" src="https://github.com/user-attachments/assets/ce1f6ca1-2847-4ee8-ac3b-e53c391f8800" />


hourly_pm2_5_trends.png → Hourly PM2.5 trends


<img width="640" height="480" alt="image" src="https://github.com/user-attachments/assets/e3ef2b0c-6eca-4a08-8222-5e2c2c8ee12e" />


severity_vs_pm2_5.png → Severity vs PM2.5 scatter


<img width="800" height="600" alt="image" src="https://github.com/user-attachments/assets/927e96da-3189-4f65-bc29-bb9deaeb76f5" />


# Setup environment variables (.env)
# Weather API
```
LAT=17.3850
LON=78.4867
FORECAST_DAYS=1

```
# Supabase
```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

Check data/processed/ for CSV outputs and plots. Inspect Supabase tables: weather_data & air_quality_data.

🛠️ Technologies
```
Python – ETL logic, analysis, plotting

Pandas / Matplotlib – Data processing & visualization

Supabase – Cloud database for storage

Open-Meteo APIs – Free weather & air quality data sources

dotenv – Environment variable management
```
