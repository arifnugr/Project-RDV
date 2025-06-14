# Bitcoin Market Analysis Dashboard

## Overview

This project provides comprehensive visualization and dashboard tools for analyzing Bitcoin market data. The data is collected through a data engineering pipeline that combines Apache Kafka for streaming data, Apache Spark for processing, and Apache Airflow for workflow orchestration.

## Features

- **Interactive Dashboard**: Visualize Bitcoin market data with an interactive Streamlit dashboard
- **Data Storytelling**: Comprehensive analysis of Bitcoin market dynamics in markdown format
- **Batch and Stream Processing**: Analysis of both historical and real-time data
- **Key Visualizations**: Focus on the most important metrics and patterns in the Bitcoin market

## Key Visualizations

1. **Price Trends**: Analyze Bitcoin price movements over time with min/max range
2. **Volume Analysis**: Understand trading volume patterns and liquidity
3. **Price-Volume Relationship**: Explore the correlation between price and trading volume
4. **Bid-Ask Dynamics**: Analyze market liquidity through bid-ask spread
5. **Market Trends**: Visualize bullish vs bearish market sentiment
6. **Volatility Analysis**: Track Bitcoin price volatility over time

## Getting Started

### Prerequisites

- Python 3.7+
- Streamlit
- Plotly
- Pandas
- MongoDB (for data storage)

### Installation

1. Install required packages:
   ```
   pip install -r requirements.txt
   ```

2. Configure MongoDB connection in `.env` file or environment variables:
   ```
   MONGODB_HOST=localhost
   ```

### Running the Dashboard

#### Windows
```
run_enhanced_dashboard.bat
```

#### Linux/Mac
```
chmod +x run_enhanced_dashboard.sh
./run_enhanced_dashboard.sh
```

## Data Story

The `crypto_story_enhanced.md` file provides a comprehensive analysis of Bitcoin market dynamics, explaining the insights derived from each visualization. This document is designed for data storytelling and can be used for presentations or reports.

## Project Structure

- `crypto_analysis.py`: Core visualization functions
- `dashboard_enhanced.py`: Streamlit dashboard implementation
- `crypto_story_enhanced.md`: Data storytelling document
- `plots/`: Directory containing saved visualizations
- `stream_plots/`: Directory containing real-time stream visualizations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.