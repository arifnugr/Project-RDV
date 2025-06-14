# Simpan sebagai dags/run_spark_job.py
import subprocess
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

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
    'spark_mongodb_batch',
    default_args=default_args,
    description='Process MongoDB data with Spark (Batch)',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['spark', 'mongodb', 'batch'],
)

def run_spark_job(**kwargs):
    """Run Spark job by processing MongoDB data directly"""
    from pymongo import MongoClient
    import pandas as pd
    from datetime import datetime
    
    try:
        print("Processing MongoDB data directly")
        
        # Connect to MongoDB
        client = MongoClient("mongodb://mongodb:27017/")
        db = client["market_data"]
        collection = db["market_data"]
        
        # Get data from MongoDB
        cursor = collection.find({}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return "No data found in MongoDB"
        
        print(f"Successfully read {len(df)} records from MongoDB")
        
        # Perform transformations (3 transformations required)
        # 1. Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # 2. Calculate additional metrics
        df['price_range'] = df['high_24h'] - df['low_24h']
        df['volume_price_ratio'] = df['volume_24h'] / df['last_price']
        df['market_impact'] = df['bid_volume'] * df['ask_volume'] / df['volume_24h']
        
        # 3. Group by time window and aggregate
        df['time_window'] = df['timestamp'].dt.floor('1min')
        result = df.groupby('time_window').agg({
            'last_price': ['mean', 'max', 'min'],
            'volume_24h': 'mean',
            'price_range': 'mean',
            'volume_price_ratio': 'mean',
            'market_impact': 'mean'
        })
        
        # Flatten column names
        result.columns = ['_'.join(col).strip() for col in result.columns.values]
        result = result.reset_index()
        
        # Save to MongoDB
        analysis_collection = db["market_analysis_batch"]
        analysis_collection.delete_many({})  # Clear previous results
        
        # Convert to records for MongoDB
        records = []
        for _, row in result.iterrows():
            record = {
                "window_start": row["time_window"].strftime("%Y-%m-%d %H:%M:%S"),
                "avg_price": float(row["last_price_mean"]),
                "max_price": float(row["last_price_max"]),
                "min_price": float(row["last_price_min"]),
                "avg_volume": float(row["volume_24h_mean"]),
                "avg_price_range": float(row["price_range_mean"]),
                "avg_volume_price_ratio": float(row["volume_price_ratio_mean"]),
                "avg_market_impact": float(row["market_impact_mean"]),
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            records.append(record)
        
        if records:
            analysis_collection.insert_many(records)
            print(f"Inserted {len(records)} records into market_analysis_batch")
        
        return f"Processed {len(df)} records with pandas"
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        print(f"Error processing data: {e}\n{error_msg}")
        return f"Error processing data: {str(e)}"

# Define task to run Spark job
spark_task = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=dag,
)

# Task to create visualizations from processed data
def create_visualizations(**kwargs):
    """Create visualizations from processed data"""
    # Path to project directory
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    viz_dir = os.path.join(project_dir, 'visualization', 'batch_plots')
    os.makedirs(viz_dir, exist_ok=True)
    
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://mongodb:27017/")
        db = client["market_data"]
        
        # Get data from market_analysis_batch collection
        collection = db["market_analysis_batch"]
        cursor = collection.find({}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return "No data found in market_analysis_batch collection"
        
        # Convert window_start to datetime for sorting
        df['window_start'] = pd.to_datetime(df['window_start'])
        df = df.sort_values('window_start')
        
        # Create visualizations
        
        # 1. Price trends
        plt.figure(figsize=(12, 6))
        plt.plot(df['window_start'], df['avg_price'], label='Average Price')
        plt.plot(df['window_start'], df['max_price'], label='Max Price')
        plt.plot(df['window_start'], df['min_price'], label='Min Price')
        plt.title('Price Trends from Batch Processing')
        plt.xlabel('Time Window')
        plt.ylabel('Price (USD)')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'price_trends.png'))
        plt.close()
        
        # 2. Volume analysis
        plt.figure(figsize=(12, 6))
        plt.bar(df['window_start'], df['avg_volume'])
        plt.title('Average Volume from Batch Processing')
        plt.xlabel('Time Window')
        plt.ylabel('Volume')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'volume_analysis.png'))
        plt.close()
        
        # 3. Market impact visualization
        plt.figure(figsize=(12, 6))
        plt.scatter(df['avg_volume_price_ratio'], df['avg_market_impact'], 
                   alpha=0.7, s=50)
        plt.title('Market Impact vs Volume-Price Ratio')
        plt.xlabel('Volume-Price Ratio')
        plt.ylabel('Market Impact')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'market_impact.png'))
        plt.close()
        
        return f"Created visualizations from {len(df)} batch processing records"
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        print(f"Error creating visualizations: {e}\n{error_msg}")
        return f"Error creating visualizations: {str(e)}"

# Define visualization task
visualization_task = PythonOperator(
    task_id='create_visualizations',
    python_callable=create_visualizations,
    dag=dag,
)

# Define task dependencies
spark_task >> visualization_task
