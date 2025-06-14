"""
Visualization Update DAG for Apache Airflow
This DAG updates cryptocurrency visualizations periodically and generates analysis
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import pandas as pd
from pymongo import MongoClient
import shutil

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
    description='Update cryptocurrency visualizations periodically',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['visualization', 'crypto'],
)

def update_visualizations(**kwargs):
    """Generate visualizations from the latest data"""
    # Path to project directory
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
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
        
        # Ambil data dari koleksi market_analysis_stream
        stream_collection = db["market_analysis_stream"]
        stream_cursor = stream_collection.find({}, {'_id': 0})
        df_stream = pd.DataFrame(list(stream_cursor))
        
        if not df_stream.empty:
            # Konversi kolom waktu
            df_stream['window_start'] = pd.to_datetime(df_stream['window_start'])
            df_stream['window_end'] = pd.to_datetime(df_stream['window_end'])
            
            # Import fungsi dari crypto_analysis.py
            sys.path.insert(0, viz_dir)
            from crypto_analysis import save_stream_visualizations
            
            # Panggil fungsi untuk membuat visualisasi dari data stream
            save_stream_visualizations(df_stream)
            
            # Generate story markdown
            generate_story_markdown()
            
            # Menambahkan data hasil visualisasi ke MongoDB untuk dashboard
            viz_collection = db["visualizations"]

            # Hapus visualisasi lama
            viz_collection.delete_many({})
            
            # Simpan visualisasi baru
            try:
                viz_data = []
                for viz_name in ['price_trends', 'volume_analysis', 'price_volume_comparison', 'market_impact']:
                    html_path = os.path.join(stream_plots_dir, f"{viz_name}.html")
                    if os.path.exists(html_path):
                        with open(html_path, 'r', encoding='utf-8') as f:
                            html_content = f.read()
                            viz_data.append({
                                "name": viz_name,
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "html_content": html_content
                            })
                
                if viz_data:
                    viz_collection.insert_many(viz_data)
                    print(f"Saved {len(viz_data)} visualizations to MongoDB")
            except Exception as e:
                print(f"Error saving visualizations to MongoDB: {e}")
            
            return "Successfully generated visualizations and analysis from market_analysis_stream"
        else:
            return "No data found in market_analysis_stream collection"
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        print(f"Error generating visualizations: {e}\n{error_msg}")
        return f"Error generating visualizations: {str(e)}"

def generate_story_markdown():
    """Generate enhanced story markdown from visualization analysis"""
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    viz_dir = os.path.join(project_dir, 'visualization')
    stream_plots_dir = os.path.join(viz_dir, 'stream_plots')
    
    # Path to the enhanced story markdown
    story_path = os.path.join(viz_dir, 'crypto_story_enhanced.md')
    
    # Create story content
    story_content = """# Dinamika Pasar Bitcoin: Analisis Data dan Visualisasi

## Pendahuluan

Bitcoin, sebagai cryptocurrency terbesar di dunia, telah menjadi aset digital yang menarik perhatian investor, trader, dan analis pasar. Artikel ini menyajikan analisis komprehensif tentang dinamika pasar Bitcoin berdasarkan data yang dikumpulkan melalui pipeline data engineering yang menggabungkan Apache Kafka, Apache Spark, dan Apache Airflow.

"""
    
    # Add analysis from each visualization if available
    for viz_name in ['price_trends', 'volume_analysis', 'price_volume_comparison', 'market_impact']:
        analysis_file = os.path.join(stream_plots_dir, f'{viz_name}_analysis.md')
        image_file = os.path.join(stream_plots_dir, f'{viz_name}.png')
        
        if os.path.exists(analysis_file) and os.path.exists(image_file):
            # Add image reference
            story_content += f"\n![{viz_name.replace('_', ' ').title()}](./stream_plots/{viz_name}.png)\n\n"
            
            # Add analysis content
            with open(analysis_file, 'r') as f:
                analysis_content = f.read()
                # Skip the title (first line) as we'll create our own
                if analysis_content.startswith('#'):
                    analysis_content = '\n'.join(analysis_content.split('\n')[1:])
                story_content += analysis_content + "\n\n"
    
    # Add conclusion
    story_content += """## Kesimpulan

Analisis komprehensif pasar Bitcoin ini mengungkapkan beberapa insight penting:

1. **Volatilitas Sebagai Karakteristik Utama**: Volatilitas tinggi Bitcoin bukan hanya risiko, tetapi juga sumber peluang bagi trader yang memahami dinamika pasar.

2. **Pentingnya Volume dan Likuiditas**: Volume perdagangan dan likuiditas adalah indikator penting yang melengkapi analisis harga.

3. **Pola dan Tren yang Dapat Diidentifikasi**: Meskipun pasar cryptocurrency sering dianggap tidak dapat diprediksi, analisis data menunjukkan pola dan tren yang dapat diidentifikasi.

4. **Manajemen Risiko yang Efektif**: Memahami volatilitas dan distribusi perubahan harga memungkinkan pengembangan strategi manajemen risiko yang lebih efektif.

Dengan terus memantau dan menganalisis data pasar Bitcoin, investor dan trader dapat membuat keputusan yang lebih terinformasi dan potensial meningkatkan hasil investasi mereka dalam aset digital yang menarik namun menantang ini."""
    
    # Write the story to file
    with open(story_path, 'w') as f:
        f.write(story_content)
    
    return "Successfully generated enhanced story markdown"

# Define tasks
update_viz_task = PythonOperator(
    task_id='update_visualizations',
    python_callable=update_visualizations,
    dag=dag,
)