# Nvidia Narrative and Financial Analysis Script

## Overview
This repository contains a simple Python data extraction and analysis script. It is designed to demonstrate end-to-end data processing, text sentiment analysis, and data visualization techniques using the Pandas and Matplotlib libraries. The primary focus of this project is to showcase analytical value by extracting business intelligence from both structured financial data and unstructured news text.

## Problem Statement and Target Audience
**Research Question:** How does the media narrative (specifically sentiment and the frequency of AI-related keywords) correlate with Nvidia's actual financial performance (revenue, R&D intensity) and stock price movements?

**Target Audience:** Business students, retail investors, and financial analysts who want to rely on data-driven insights rather than pure market hype to understand the relationship between corporate media presence and fundamental equity valuation in the semiconductor sector.

## Data Sources
The script aggregates data from two primary free sources:
1. **Yahoo Finance API:** Used to fetch structured historical OHLCV market data and annual financial fundamentals (Income Statement and Balance Sheet).
2. **Google News RSS Feed:** Used as a reliable, anti-bot-friendly source to scrape recent unstructured news headlines and descriptions related to Nvidia.

## Methodology
The Python script performs the following data processing and analytical tasks:
1. **Data Acquisition & Cleaning:** Fetches raw financial and text data, standardizing missing values and coercing data types using Pandas.
2. **Feature Engineering & NLP:** Transforms annual financial reports into broadcasted daily time-series data. Applies the TextBlob library to calculate polarity (sentiment) and subjectivity scores for each news article, and counts the frequency of strategic keywords (e.g., "AI", "Blackwell", "datacenter").
3. **Exploratory Data Analysis (EDA):** Uses Matplotlib and Seaborn to visualize the historical correlation between stock prices, R&D intensity, profit margins, and media sentiment scores.

## Analytical Value & Expected Findings
By merging heterogeneous datasets, this script extracts several layers of analytical value:
* **Keyword Dominance:** Identifies which specific terms dominate the corporate narrative and whether shifts in keywords align with revenue growth phases.
* **Sentiment vs. Valuation:** Provides a correlation matrix evaluating whether positive news sentiment acts as a leading indicator for short-term price appreciation or if it merely lags behind strong fundamental earnings reports.
* **Fundamental Trends:** Visualizes the structural uptrend in R&D intensity and profit margins, providing a quantitative justification for the company's market capitalization.

## Quick Start Guide
Follow these simple steps to set up the environment and run the analysis script on your local machine.

1. **Install Dependencies:**
Ensure you have a Python virtual environment activated, then install the required packages:
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