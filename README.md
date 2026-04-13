# Simulated Retail Performance Analysis

## Overview
This repository contains a simple Python simulation script and data analysis pipeline. It is designed to demonstrate end-to-end data processing, exploratory data analysis (EDA), and visualization techniques. The primary focus of this project is to showcase analytical value and business intelligence derivation rather than complex software engineering.

## Problem Statement and Target Audience
**Research Question:** How do seasonal trends and promotional discount tiers affect overall sales volume and customer retention rates over a simulated operating period?

**Target Audience:** Business analysts, retail managers, and marketing strategists who rely on data-driven insights to optimize promotional calendars, pricing strategies, and inventory management.

## Data Source
The data used in this project is entirely generated via a custom Python simulation script included in this repository. The script generates a synthetic dataset representing daily transaction logs, capturing variables such as transaction date, product category, discount applied, sales volume, and customer return status.

## Methodology
The Python script performs substantial data processing and analytical tasks:
1. **Data Generation and Cleaning:** Simulates raw transaction data, intentionally introducing realistic noise (e.g., missing values, outliers) which are subsequently cleaned, imputed, and normalized using the Pandas library.
2. **Aggregation and Feature Engineering:** Groups daily transactions into weekly and monthly trends, calculating rolling averages, profit margins, and baseline customer retention metrics.
3. **Exploratory Data Analysis:** Utilizes Matplotlib and Seaborn to visualize sales distributions across different discount tiers and seasonal periods, translating raw numbers into actionable visual insights.

## Key Findings
Through the analysis of the simulated dataset, the following analytical insights were derived:
* **Promotional Thresholds:** Discounts exceeding a specific threshold (e.g., 20%) demonstrated diminishing returns on overall revenue, despite driving a higher raw volume of sales. 
* **Seasonal Retention:** Customer retention metrics dropped significantly in immediate post-holiday periods, suggesting a strategic need for targeted re-engagement campaigns during these specific windows.

## Limitations
* **Synthetic Data Constraints:** Because the data is generated via a simple algorithmic script, it fundamentally lacks the unpredictable nature, latent variables, and irrational behaviors present in real-world consumer actions.
* **Scope of Variables:** The simulation operates in a vacuum and does not account for external macroeconomic factors, supply chain disruptions, or competitor actions.

## Quick Start Guide
Follow these steps to set up the environment, run the simulation, and reproduce the analysis on your local machine.
1. Install Dependencies
```bash
pip install -r requirements.txt
```
2. Set Up Unit Tests
```bash
pytest tests/ -v
```
3. Execute the Analysis Pipeline
```bash
jupyter notebook retail_analysis.ipynb
```