"""
Visualization Update DAG for Apache Airflow
This DAG updates cryptocurrency visualizations periodically and generates analysis
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import pandas as pd
import os
import sys

# Tambahkan path ke direktori visualization
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(project_dir, 'visualization'))

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'visualization_update',
    default_args=default_args,
    description='Update cryptocurrency market visualizations',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    start_date=datetime(2025, 6, 21),
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'visualization', 'bitcoin'],
)

def generate_story_markdown():
    """Generate a comprehensive story from the analysis"""
    try:
        # Import di dalam fungsi untuk menghindari circular import
        from crypto_analysis import get_stream_data_by_date
        
        # Path untuk menyimpan story
        viz_dir = os.path.join(project_dir, 'visualization')
        story_dir = os.path.join(viz_dir, 'story')
        os.makedirs(story_dir, exist_ok=True)
        
        # Ambil data hari ini
        df = get_stream_data_by_date()
        
        if df is None or df.empty:
            print("Tidak ada data untuk generate story")
            return "No data available for story generation"
        
        # Hitung metrics untuk story
        latest_price = df['avg_price'].iloc[-1] if 'avg_price' in df.columns else 0
        earliest_price = df['avg_price'].iloc[0] if 'avg_price' in df.columns else 0
        price_change = latest_price - earliest_price
        price_change_pct = (price_change / earliest_price * 100) if earliest_price > 0 else 0
        
        max_price = df['avg_price'].max() if 'avg_price' in df.columns else 0
        min_price = df['avg_price'].min() if 'avg_price' in df.columns else 0
        volatility = ((max_price - min_price) / latest_price * 100) if latest_price > 0 else 0
        
        total_volume = df['avg_volume'].sum() if 'avg_volume' in df.columns else 0
        avg_volume = df['avg_volume'].mean() if 'avg_volume' in df.columns else 0
        
        # Determine trend
        if price_change_pct > 1:
            trend_text = "BULLISH"
        elif price_change_pct < -1:
            trend_text = "BEARISH"
        else:
            trend_text = "SIDEWAYS"
        
        # Generate story content
        story_content = f"""# Bitcoin Real-time Analysis Report

**Generated on:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Executive Summary

Bitcoin is currently showing a **{trend_text}** trend with significant market activity. The latest analysis reveals key insights into price movements and trading volumes that indicate market sentiment.

## Key Metrics

| Metric | Value | Change |
|--------|-------|--------|
| **Current Price** | ${latest_price:,.2f} | {price_change:+.2f} ({price_change_pct:+.2f}%) |
| **24h High** | ${max_price:,.2f} | - |
| **24h Low** | ${min_price:,.2f} | - |
| **Volatility** | {volatility:.2f}% | - |
| **Total Volume** | ${total_volume:,.0f} | - |
| **Avg Volume/min** | ${avg_volume:,.0f} | - |
| **Data Points** | {len(df)} records | - |

## Market Analysis

### Price Movement
{"Bitcoin has gained momentum with an upward trajectory, indicating strong buying pressure from the market." if price_change_pct > 1 else "Bitcoin is experiencing downward pressure, suggesting increased selling activity." if price_change_pct < -1 else "Bitcoin is trading in a consolidation phase with minimal directional bias."}

### Volume Analysis
{"High trading volume confirms the price movement, indicating strong market participation." if avg_volume > 1000000 else "Moderate trading volume suggests steady but not overwhelming market interest." if avg_volume > 500000 else "Lower trading volumes may indicate market uncertainty or consolidation phase."}

### Volatility Assessment
{"High volatility suggests active price discovery and potential trading opportunities." if volatility > 5 else "Moderate volatility indicates a balanced market with controlled price movements." if volatility > 2 else "Low volatility suggests market stability and reduced trading risk."}

## Trading Insights

**Support & Resistance:**
- **Support Level:** ${min_price:,.2f} (24h Low)
- **Resistance Level:** ${max_price:,.2f} (24h High)
- **Current Position:** {((latest_price - min_price) / (max_price - min_price) * 100):.1f}% from support to resistance

**Market Sentiment:**
{f"BULLISH - Strong upward momentum with volume confirmation" if price_change_pct > 2 and avg_volume > 1000000 else f"BEARISH - Downward pressure with volume participation" if price_change_pct < -2 and avg_volume > 1000000 else f"NEUTRAL - Consolidation phase with mixed signals"}

## Technical Indicators

- **Trend Direction:** {trend_text}
- **Price Change:** {price_change_pct:+.2f}%
- **Volatility Index:** {volatility:.2f}%
- **Volume Trend:** {"Increasing" if len(df) > 10 and df['avg_volume'].iloc[-5:].mean() > df['avg_volume'].iloc[:-5].mean() else "Stable"}

## Risk Assessment

**Risk Level:** {"HIGH" if volatility > 5 else "MODERATE" if volatility > 2 else "LOW"}

**Key Risks:**
- Volatility: {volatility:.2f}% ({"High" if volatility > 5 else "Moderate" if volatility > 2 else "Low"} risk)
- Liquidity: {"Good" if avg_volume > 1000000 else "Moderate" if avg_volume > 500000 else "Limited"}
- Market Sentiment: {trend_text}

## Outlook

Based on current data analysis, Bitcoin shows {"strong bullish momentum with potential for continued upward movement" if price_change_pct > 2 else "bearish pressure that may continue in the short term" if price_change_pct < -2 else "consolidation behavior that may lead to a breakout in either direction"}.

**Next Key Levels to Watch:**
- **Upside Target:** ${max_price * 1.02:,.2f} (+2% from current high)
- **Downside Support:** ${min_price * 0.98:,.2f} (-2% from current low)

---

*This analysis is generated from real-time market data and should be used for informational purposes only. Always conduct your own research before making investment decisions.*

**Data Source:** Kafka Stream Processing | **Analysis Engine:** Apache Spark | **Visualization:** Plotly
        """
        
        # Simpan story ke file
        story_path = os.path.join(story_dir, 'bitcoin_analysis_story.md')
        with open(story_path, 'w', encoding='utf-8') as f:
            f.write(story_content)
        
        print(f"Story berhasil di-generate: {story_path}")
        return f"Story generated successfully with {len(df)} data points"
        
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        print(f"Error generating story: {e}\n{error_msg}")
        return f"Error generating story: {str(e)}"

def update_visualizations(**kwargs):
    """Generate visualizations from the latest data and save to MongoDB"""
    # Path to project directory
    viz_dir = os.path.join(project_dir, 'visualization')
    plots_dir = os.path.join(viz_dir, 'plots')
    stream_plots_dir = os.path.join(viz_dir, 'stream_plots')
    
    # Ensure plots directories exist
    os.makedirs(plots_dir, exist_ok=True)
    os.makedirs(stream_plots_dir, exist_ok=True)
    
    try:
        # Connect to MongoDB
        mongo_host = os.environ.get('MONGODB_HOST', 'mongodb')
        mongo_port = os.environ.get('MONGODB_PORT', '27017')
        mongo_db = os.environ.get('MONGODB_DATABASE', 'market_data')
        
        client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        db = client[mongo_db]
        
        # Ambil data untuk hari ini saja dengan filter tanggal
        today = datetime.now().date()
        start_of_today = datetime.combine(today, datetime.min.time())
        end_of_today = datetime.combine(today, datetime.max.time())
        
        stream_collection = db["market_analysis_stream"]
        stream_cursor = stream_collection.find({
            "window_start": {
                "$gte": start_of_today.strftime("%Y-%m-%d %H:%M:%S"),
                "$lte": end_of_today.strftime("%Y-%m-%d %H:%M:%S")
            }
        }, {'_id': 0}).sort("window_start", 1)
        
        df_stream = pd.DataFrame(list(stream_cursor))
        
        if not df_stream.empty:
            # Konversi kolom waktu
            df_stream['window_start'] = pd.to_datetime(df_stream['window_start'])
            if 'window_end' in df_stream.columns:
                df_stream['window_end'] = pd.to_datetime(df_stream['window_end'])
            
            # Import dan panggil fungsi visualisasi yang sudah diupdate
            sys.path.insert(0, viz_dir)
            from crypto_analysis import save_stream_visualizations
            
            # Generate visualisasi dengan rentang Y yang optimal
            save_stream_visualizations(df_stream)
            
            # Generate story markdown
            story_result = generate_story_markdown()
            print(f"Story generation: {story_result}")
            
            # Simpan visualisasi ke MongoDB untuk dashboard
            viz_collection = db["visualizations"]

            # Hapus visualisasi lama untuk hari ini
            delete_result = viz_collection.delete_many({
                "date": today.strftime("%Y-%m-%d")
            })
            print(f"Deleted {delete_result.deleted_count} old visualizations for {today}")
            
            # Simpan visualisasi baru ke MongoDB
            try:
                viz_data = []
                visualization_files = [
                    'price_trends',              # avg_price, min_price, max_price
                    'volume_analysis',           # avg_volume 
                    'price_volume_comparison',   # avg_price + avg_volume
                    'market_impact',             # avg_market_impact
                    'price_range_analysis',      # avg_price_range
                    'volume_price_ratio',        # avg_volume_price_ratio
                    'summary_stats'              # Summary semua metrics
                ]
                
                for viz_name in visualization_files:
                    html_path = os.path.join(stream_plots_dir, f"{viz_name}.html")
                    
                    if os.path.exists(html_path):
                        with open(html_path, 'r', encoding='utf-8') as f:
                            html_content = f.read()
                            
                        viz_data.append({
                            "name": viz_name,
                            "date": today.strftime("%Y-%m-%d"),
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "html_content": html_content,
                            "data_points": len(df_stream),
                            "generated_by": "airflow_dag",
                            "file_size": len(html_content)
                        })
                        
                        print(f"Prepared {viz_name} for MongoDB storage ({len(html_content):,} chars)")
                    else:
                        print(f"File not found: {html_path}")
                
                # Insert ke MongoDB
                if viz_data:
                    result = viz_collection.insert_many(viz_data)
                    print(f"Saved {len(viz_data)} visualizations to MongoDB")
                    print(f"   Inserted IDs: {[str(id) for id in result.inserted_ids]}")
                    
                    # Verify insertion
                    for viz in viz_data:
                        count = viz_collection.count_documents({
                            "name": viz["name"], 
                            "date": viz["date"]
                        })
                        print(f"   Verified {viz['name']}: {count} documents")
                else:
                    print("No visualization files found to save")
                    
            except Exception as mongo_error:
                print(f"Error saving visualizations to MongoDB: {mongo_error}")
                import traceback
                print(f"   Traceback: {traceback.format_exc()}")
            
            # Close MongoDB connection
            client.close()
            
            print(f"Generated visualizations for {today} with {len(df_stream)} data points")
            return f"Successfully generated optimized visualizations for {today} and saved to MongoDB"
            
        else:
            client.close()
            print(f"No stream data found for {today}")
            return f"No stream data found for {today}"
            
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        print(f"Error generating visualizations: {e}\n{error_msg}")
        return f"Error generating visualizations: {str(e)}"

def cleanup_old_visualizations(**kwargs):
    """Cleanup visualizations older than 7 days"""
    try:
        mongo_host = os.environ.get('MONGODB_HOST', 'mongodb')
        mongo_port = os.environ.get('MONGODB_PORT', '27017')
        mongo_db = os.environ.get('MONGODB_DATABASE', 'market_data')
        
        client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        db = client[mongo_db]
        viz_collection = db["visualizations"]
        
        # Hapus visualisasi lebih dari 7 hari
        cutoff_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        delete_result = viz_collection.delete_many({
            "date": {"$lt": cutoff_date}
        })
        
        client.close()
        
        print(f"Cleaned up {delete_result.deleted_count} old visualizations (older than {cutoff_date})")
        return f"Cleaned up {delete_result.deleted_count} old visualizations"
        
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return f"Error during cleanup: {str(e)}"

# Define tasks
update_viz_task = PythonOperator(
    task_id='update_visualizations',
    python_callable=update_visualizations,
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_visualizations',
    python_callable=cleanup_old_visualizations,
    dag=dag,
    execution_timeout=timedelta(minutes=5),
)

# Set task dependencies
update_viz_task >> cleanup_task