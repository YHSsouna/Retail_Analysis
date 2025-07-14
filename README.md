# 🛒 Retail Data Analysis Pipeline Project

## 🧾 Overview
This project aims to **collect daily data from four major supermarket websites** in order to analyze and compare product information, pricing trends, and availability.

## 📊 Pipeline Features

<img src="https://github.com/YHSsouna/Retail_Analysis/blob/24fab9295b67d4bbd451e5305b8fcfd20450e827/architecture.png" alt="Architecture Diagram" width="600"/>


- **🔍 Daily web scraping** from 4 retail websites (Python, Selenium)
- **🧠 LLM-based data transformation:** Automatically extract product quantity and unit from product names  
  > *Example:* `Boisson lait d'amande chocolat BIO, Bjorg (3 x 20 cl)` → `Weight: 0.6`, `Unit: L`
- **📦 LLM-based product categorization:** Determine the appropriate product category using product name  
  > *Example:* `Yaourt brassé fraise et rhubarbe BIO, Les 2 Vaches (4 x 115 g)` → `Category: Yaourt`
- **📐 Data transformation and modeling** using [dbt](https://www.getdbt.com/)
- **📅 Workflow orchestration** with [Apache Airflow](https://airflow.apache.org/)
- **🧠 Model training and tracking** using [MLflow](https://mlflow.org/)
- **💾 Data storage** in PostgreSQL
- **📊 Visualization** and KPI monitoring with Power BI
- **💬 Integrated AI Chatbot** for querying and interacting with the processed retail data using natural language
- **🐳 Dockerized Deployment**: The entire pipeline runs in isolated containers using Docker and Docker Compose

## 🏬 Targeted Supermarkets

- Auchan
- Carrefour
- Biocoop
- La Belle Vie

## ⚙️ Tech Stack

| Layer        | Tools Used                          |
|-------------|--------------------------------------|
| Orchestration | Apache Airflow                     |
| Data Collection | Python Web Scraping               |
| Data Modeling | dbt (Data Build Tool)              |
| Data Storage  | PostgreSQL, local files            |
| Machine Learning | MLflow (optional model training)|
| Containerization | Docker, Docker Compose         |

## 📁 Project Structure
## 📂 Project Structure

- `dags/`: Airflow DAGs
- `retail/`: dbt models and SQL transformations
- `web_scraping_scripts/`: Python scrapers for retail websites
- `docker-compose.yaml`: Runs the full pipeline in Docker
- `dockerfile`: build the airflow image
- `dockerfile.mlflow`: build the mlflow image

## 🚀 Getting Started

## 📈 Power BI Dashboard

Below is a sample of the interactive dashboard generated from the pipeline:

<img src="https://github.com/YHSsouna/Retail_Analysis/blob/f12f6e9f1f4386c177fb5fc4acfc9cfb99c57676/Screenshot%202025-07-14%20183159.png" alt="Power BI screenshot"/>


```bash
docker-compose up --build



