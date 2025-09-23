# VN Stock Screener

A comprehensive Vietnamese stock screening tool based on quantitative financial criteria.

## Features

- **Growth Analysis**: Revenue and Profit CAGR (3-year)
- **Profitability Metrics**: ROE, ROA analysis
- **Valuation Ratios**: P/E, P/B, PEG, EV/EBITDA
- **Liquidity & Ownership**: Free float, market cap, foreign ownership
- **Interactive UI**: Real-time filtering and analysis

## Criteria

- Min Revenue CAGR (3Y): 12%
- Min Profit CAGR (3Y): 15%
- Min ROE: 15%
- Min ROA: 5%
- Max P/B: 2.0
- Max PEG: 1.5
- Max EV/EBITDA: 10.0
- Min Free Float: 40%
- Min Avg Trading Value: 1.0 billion VND/day

## Data Sources

- **vnstock3**: Financial ratios, income statements, balance sheets
- **Real-time data**: Live stock information from Vietnamese exchanges

## Usage

1. Adjust criteria in the sidebar
2. Click "Scan now" to analyze stocks
3. View results in per-criterion tables
4. Check final pass list for stocks meeting all criteria

## Installation

```bash
pip install -r requirements.txt
streamlit run app/streamlit_app.py
```

## Live Demo

[Deploy on Streamlit Community Cloud](https://share.streamlit.io)
