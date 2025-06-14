import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import sys

# Tambahkan path proyek ke sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
sys.path.insert(0, project_dir)

# Fungsi untuk mendapatkan data dari MongoDB
def get_data_from_db():
    try:
        # Gunakan nama host 'mongodb' untuk Docker atau 'localhost' untuk development
        mongo_host = os.environ.get('MONGODB_HOST', 'mongodb')
        from pymongo import MongoClient
        
        client = MongoClient(f"mongodb://{mongo_host}:27017/")
        db = client["market_data"]
        collection = db["market_data"]
        
        # Ambil semua data dari MongoDB
        cursor = collection.find({}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        
        if not df.empty:
            # Konversi timestamp ke datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            print(f"Berhasil memuat {len(df)} data dari MongoDB")
            return df
        else:
            print("Tidak ada data di MongoDB")
            return None
    except Exception as e:
        print(f"Error saat terhubung ke MongoDB: {e}")
        return None

# Fungsi untuk mendapatkan data dari CSV (fallback)
def get_data_from_csv():
    csv_path = os.path.join(project_dir, 'data', 'market_data.csv')
    
    if not os.path.exists(csv_path):
        print(f"File CSV tidak ditemukan di {csv_path}")
        return None
    
    try:
        df = pd.read_csv(csv_path)
        
        # Konversi timestamp ke datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
    except Exception as e:
        print(f"Error saat membaca CSV: {e}")
        return None

# Fungsi untuk mendapatkan data (mencoba DB dulu, lalu CSV)
def get_data():
    df = get_data_from_db()
    if df is None:
        df = get_data_from_csv()
    return df

# Fungsi untuk membersihkan dan mempersiapkan data
def prepare_data(df):
    if df is None:
        return None
    
    # Hapus duplikat
    df = df.drop_duplicates()
    
    # Urutkan berdasarkan timestamp
    df = df.sort_values('timestamp')
    
    # Reset index
    df = df.reset_index(drop=True)
    
    # Tambahkan kolom tanggal dan waktu untuk analisis
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    
    # Tambahkan kolom untuk analisis volatilitas
    df['price_change'] = df['last_price'].diff()
    df['price_change_pct'] = df['price_change'] / df['last_price'].shift(1) * 100
    
    return df

# Fungsi untuk visualisasi harga Bitcoin terhadap waktu
def plot_price_over_time(df):
    if df is None:
        return None
    
    fig = px.line(df, x='timestamp', y='last_price', 
                  title='Harga Bitcoin (BTC/USDT) Terhadap Waktu',
                  labels={'last_price': 'Harga (USDT)', 'timestamp': 'Waktu'},
                  template='plotly_white')
    
    fig.update_layout(
        xaxis_title='Waktu',
        yaxis_title='Harga (USDT)',
        hovermode='x unified'
    )
    
    return fig

# Fungsi untuk visualisasi volume perdagangan
def plot_volume_analysis(df):
    if df is None:
        return None
    
    fig = px.bar(df, x='timestamp', y='volume_24h', 
                 title='Volume Perdagangan Bitcoin dalam 24 Jam',
                 labels={'volume_24h': 'Volume (USDT)', 'timestamp': 'Waktu'},
                 template='plotly_white')
    
    fig.update_layout(
        xaxis_title='Waktu',
        yaxis_title='Volume (USDT)',
        hovermode='x unified'
    )
    
    return fig

# Fungsi untuk visualisasi perbandingan harga bid dan ask
def plot_bid_ask_comparison(df):
    if df is None:
        return None
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Tambahkan garis untuk bid dan ask price
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['bid_price'], name='Bid Price'),
        secondary_y=False,
    )
    
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['ask_price'], name='Ask Price'),
        secondary_y=False,
    )
    
    # Tambahkan area untuk spread
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['bid_ask_spread'], 
                   name='Bid-Ask Spread', fill='tozeroy'),
        secondary_y=True,
    )
    
    fig.update_layout(
        title_text='Perbandingan Bid Price dan Ask Price dengan Spread',
        hovermode='x unified'
    )
    
    fig.update_xaxes(title_text='Waktu')
    fig.update_yaxes(title_text='Harga (USDT)', secondary_y=False)
    fig.update_yaxes(title_text='Spread (USDT)', secondary_y=True)
    
    return fig

# Fungsi untuk visualisasi distribusi perubahan harga
def plot_price_change_distribution(df):
    if df is None:
        return None
    
    fig = px.histogram(df, x='price_change_pct', 
                       title='Distribusi Perubahan Harga Bitcoin (%)',
                       labels={'price_change_pct': 'Perubahan Harga (%)'},
                       template='plotly_white',
                       marginal='box')
    
    fig.update_layout(
        xaxis_title='Perubahan Harga (%)',
        yaxis_title='Frekuensi',
        hovermode='x unified'
    )
    
    return fig

# Fungsi untuk visualisasi tren bullish vs bearish
def plot_trend_analysis(df):
    if df is None:
        return None
    
    trend_counts = df['trend'].value_counts().reset_index()
    trend_counts.columns = ['Trend', 'Count']
    
    fig = px.pie(trend_counts, values='Count', names='Trend', 
                 title='Distribusi Tren Pasar Bitcoin',
                 color='Trend',
                 color_discrete_map={'bullish': 'green', 'bearish': 'red', 'unknown': 'gray'},
                 template='plotly_white')
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    
    return fig

# Fungsi untuk visualisasi pola harian
def plot_hourly_pattern(df):
    if df is None:
        return None
    
    hourly_avg = df.groupby('hour')['last_price'].mean().reset_index()
    
    fig = px.line(hourly_avg, x='hour', y='last_price', 
                  title='Pola Harga Bitcoin Berdasarkan Jam',
                  labels={'last_price': 'Rata-rata Harga (USDT)', 'hour': 'Jam'},
                  template='plotly_white',
                  markers=True)
    
    fig.update_layout(
        xaxis_title='Jam (0-23)',
        yaxis_title='Rata-rata Harga (USDT)',
        hovermode='x unified',
        xaxis=dict(tickmode='linear', tick0=0, dtick=1)
    )
    
    return fig

# Fungsi untuk visualisasi korelasi antar variabel
def plot_correlation_heatmap(df):
    if df is None:
        return None
    
    # Pilih kolom numerik saja
    numeric_cols = ['last_price', 'bid_price', 'ask_price', 'bid_volume', 'ask_volume', 
                    'volume_24h', 'high_24h', 'low_24h', 'change_24h', 'price_change_24h', 
                    'bid_ask_spread', 'change_24h_normalized', 'spread_percentage']
    
    # Hitung korelasi
    corr = df[numeric_cols].corr()
    
    # Buat heatmap
    fig = px.imshow(corr, text_auto=True, aspect='auto',
                    title='Korelasi Antar Variabel Pasar Bitcoin',
                    color_continuous_scale='RdBu_r',
                    template='plotly_white')
    
    fig.update_layout(
        height=700,
        width=700
    )
    
    return fig

# Fungsi untuk visualisasi volatilitas harga
def plot_volatility_analysis(df):
    if df is None:
        return None
    
    # Hitung volatilitas harian (standar deviasi perubahan harga)
    daily_volatility = df.groupby('date')['price_change_pct'].std().reset_index()
    daily_volatility.columns = ['date', 'volatility']
    
    fig = px.line(daily_volatility, x='date', y='volatility', 
                  title='Volatilitas Harian Bitcoin',
                  labels={'volatility': 'Volatilitas (%)', 'date': 'Tanggal'},
                  template='plotly_white')
    
    fig.update_layout(
        xaxis_title='Tanggal',
        yaxis_title='Volatilitas (%)',
        hovermode='x unified'
    )
    
    return fig

# Fungsi untuk visualisasi perbandingan volume dan harga
def plot_volume_price_comparison(df):
    if df is None:
        return None
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Tambahkan garis untuk harga
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['last_price'], name='Harga'),
        secondary_y=False,
    )
    
    # Tambahkan bar untuk volume
    fig.add_trace(
        go.Bar(x=df['timestamp'], y=df['volume_24h'], name='Volume 24h'),
        secondary_y=True,
    )
    
    fig.update_layout(
        title_text='Perbandingan Harga dan Volume Perdagangan Bitcoin',
        hovermode='x unified'
    )
    
    fig.update_xaxes(title_text='Waktu')
    fig.update_yaxes(title_text='Harga (USDT)', secondary_y=False)
    fig.update_yaxes(title_text='Volume (USDT)', secondary_y=True)
    
    return fig

# # Fungsi untuk menyimpan semua visualisasi
# def save_all_visualizations(df=None):
#     if df is None:
#         df = get_data()
#         df = prepare_data(df)
    
#     if df is None:
#         print("Tidak dapat memuat data. Pastikan database tersedia.")
#         return
    
#     # Buat direktori untuk menyimpan visualisasi jika belum ada
#     viz_dir = os.path.join(current_dir, 'plots')
#     try:
#         if not os.path.exists(viz_dir):
#             os.makedirs(viz_dir)
#     except Exception as e:
#         print(f"Warning: Could not create plots directory: {e}")
#         # Lanjutkan meskipun tidak bisa membuat direktori
    
#     # Buat dan simpan visualisasi yang penting saja
#     key_visualizations = {
#         'price_over_time': plot_price_over_time(df),
#         'volume_analysis': plot_volume_analysis(df),
#         'bid_ask_comparison': plot_bid_ask_comparison(df),
#         'trend_analysis': plot_trend_analysis(df),
#         'volatility_analysis': plot_volatility_analysis(df),
#         'volume_price_comparison': plot_volume_price_comparison(df)
#     }
    
#     # Simpan visualisasi dengan deskripsi analisis
#     for name, fig in key_visualizations.items():
#         if fig is not None:
#             try:
#                 fig.write_html(os.path.join(viz_dir, f'{name}.html'))
#                 fig.write_image(os.path.join(viz_dir, f'{name}.png'))
                
#                 # Tambahkan deskripsi untuk setiap visualisasi
#                 with open(os.path.join(viz_dir, f'{name}_analysis.md'), 'w') as f:
#                     if name == 'price_over_time':
#                         f.write("# Analisis Harga Bitcoin Terhadap Waktu\n\n")
#                         f.write("Grafik ini menunjukkan pergerakan harga Bitcoin selama periode pengamatan. ")
#                         f.write("Volatilitas tinggi terlihat dari fluktuasi harga yang signifikan dalam waktu singkat. ")
#                         f.write("Pola tren naik (bullish) dan turun (bearish) dapat diidentifikasi, ")
#                         f.write("serta level support dan resistance yang menjadi acuan pergerakan harga.\n")
#                     elif name == 'volume_analysis':
#                         f.write("# Analisis Volume Perdagangan Bitcoin\n\n")
#                         f.write("Volume perdagangan menunjukkan likuiditas dan minat pasar terhadap Bitcoin. ")
#                         f.write("Volume tinggi yang mengikuti pergerakan harga mengkonfirmasi kekuatan tren tersebut. ")
#                         f.write("Penurunan volume pada tren yang sedang berlangsung dapat menjadi sinyal awal pembalikan arah. ")
#                         f.write("Volume perdagangan yang tinggi juga menunjukkan likuiditas pasar yang baik.\n")
#                     elif name == 'bid_ask_comparison':
#                         f.write("# Analisis Bid-Ask Spread Bitcoin\n\n")
#                         f.write("Bid-ask spread adalah selisih antara harga tertinggi yang bersedia dibayar pembeli (bid) ")
#                         f.write("dan harga terendah yang bersedia diterima penjual (ask). ")
#                         f.write("Spread yang kecil menunjukkan likuiditas tinggi, sementara spread yang besar menunjukkan likuiditas rendah. ")
#                         f.write("Peningkatan spread sering terjadi selama periode volatilitas tinggi atau ketidakpastian pasar.\n")
#                     elif name == 'trend_analysis':
#                         f.write("# Analisis Tren Pasar Bitcoin\n\n")
#                         f.write("Grafik ini menunjukkan distribusi tren bullish (naik) dan bearish (turun) selama periode pengamatan. ")
#                         f.write("Dominasi tren tertentu mencerminkan sentimen pasar secara keseluruhan. ")
#                         f.write("Memahami tren pasar membantu investor menyelaraskan strategi dengan arah pergerakan harga yang dominan.\n")
#                     elif name == 'volatility_analysis':
#                         f.write("# Analisis Volatilitas Bitcoin\n\n")
#                         f.write("Volatilitas menunjukkan seberapa besar dan cepat harga Bitcoin berfluktuasi. ")
#                         f.write("Volatilitas cenderung mengelompok, dengan periode volatilitas tinggi diikuti oleh periode volatilitas tinggi lainnya. ")
#                         f.write("Lonjakan volatilitas sering bertepatan dengan peristiwa pasar signifikan. ")
#                         f.write("Volatilitas yang lebih tinggi menunjukkan risiko yang lebih tinggi, tetapi juga potensi keuntungan yang lebih tinggi.\n")
#                     elif name == 'volume_price_comparison':
#                         f.write("# Analisis Hubungan Harga dan Volume Bitcoin\n\n")
#                         f.write("Grafik ini menunjukkan hubungan antara harga dan volume perdagangan Bitcoin. ")
#                         f.write("Lonjakan volume perdagangan sering bersamaan dengan pergerakan harga yang signifikan. ")
#                         f.write("Perubahan harga yang didukung oleh volume tinggi cenderung lebih berkelanjutan. ")
#                         f.write("Hubungan ini membantu mengidentifikasi kekuatan tren dan potensi pembalikan arah.\n")
#             except Exception as e:
#                 print(f"Warning: Could not save visualization {name}: {e}")
    
#     print(f"Visualisasi telah disimpan di {viz_dir}")

# Fungsi untuk membuat visualisasi dari data stream
def save_stream_visualizations(df):
    if df is None or df.empty:
        print("Tidak ada data stream untuk visualisasi.")
        return
    
    # Buat direktori untuk menyimpan visualisasi
    viz_dir = os.path.join(current_dir, 'stream_plots')
    os.makedirs(viz_dir, exist_ok=True)
    
    try:
        # Visualisasi 1: Tren harga rata-rata dengan area shading
        if 'avg_price' in df.columns:
            fig_price = go.Figure()
            
            # Tambahkan garis untuk harga rata-rata
            fig_price.add_trace(
                go.Scatter(
                    x=df['window_start'], 
                    y=df['avg_price'], 
                    name='Rata-rata Harga',
                    line=dict(color='rgb(31, 119, 180)', width=2)
                )
            )
            
            # Tambahkan area untuk range harga (min-max)
            if 'max_price' in df.columns and 'min_price' in df.columns:
                fig_price.add_trace(
                    go.Scatter(
                        x=df['window_start'],
                        y=df['max_price'],
                        fill=None,
                        mode='lines',
                        line_color='rgba(31, 119, 180, 0.1)',
                        name='Harga Maksimum'
                    )
                )
                
                fig_price.add_trace(
                    go.Scatter(
                        x=df['window_start'],
                        y=df['min_price'],
                        fill='tonexty',
                        mode='lines',
                        line_color='rgba(31, 119, 180, 0.1)',
                        name='Harga Minimum'
                    )
                )
            
            fig_price.update_layout(
                title='Tren Harga Bitcoin per Menit',
                xaxis_title='Waktu',
                yaxis_title='Harga (USDT)',
                hovermode='x unified',
                legend_title='Metrik',
                template='plotly_white',
                height=500
            )
            
            # Simpan visualisasi
            fig_price.write_html(os.path.join(viz_dir, 'price_trends.html'))
            fig_price.write_image(os.path.join(viz_dir, 'price_trends.png'))
            
            # Tambahkan deskripsi analisis
            with open(os.path.join(viz_dir, 'price_trends_analysis.md'), 'w') as f:
                f.write("# Analisis Tren Harga Bitcoin per Menit\n\n")
                f.write("Grafik ini menunjukkan pergerakan harga rata-rata Bitcoin dalam interval satu menit, ")
                f.write("dengan area bayangan yang menunjukkan rentang antara harga minimum dan maksimum. ")
                f.write("Visualisasi ini memungkinkan kita mengidentifikasi volatilitas jangka pendek dan ")
                f.write("tren pergerakan harga secara real-time. Area bayangan yang lebih lebar menunjukkan ")
                f.write("volatilitas yang lebih tinggi pada interval waktu tersebut, sementara area yang lebih ")
                f.write("sempit menunjukkan stabilitas harga yang relatif.\n")
            
            print("Berhasil menyimpan visualisasi tren harga")
        
        # Visualisasi 2: Volume perdagangan dengan gradient color
        if 'avg_volume' in df.columns:
            fig_volume = px.bar(
                df, 
                x='window_start', 
                y='avg_volume',
                title='Volume Perdagangan Rata-rata per Menit',
                labels={'avg_volume': 'Volume (USDT)', 'window_start': 'Waktu'},
                template='plotly_white',
                color='avg_volume',
                color_continuous_scale='Viridis'
            )
            
            fig_volume.update_layout(
                xaxis_title='Waktu',
                yaxis_title='Volume (USDT)',
                hovermode='x unified',
                height=500,
                coloraxis_showscale=False
            )
            
            # Simpan visualisasi
            fig_volume.write_html(os.path.join(viz_dir, 'volume_analysis.html'))
            fig_volume.write_image(os.path.join(viz_dir, 'volume_analysis.png'))
            
            # Tambahkan deskripsi analisis
            with open(os.path.join(viz_dir, 'volume_analysis_analysis.md'), 'w') as f:
                f.write("# Analisis Volume Perdagangan Bitcoin per Menit\n\n")
                f.write("Grafik batang ini menunjukkan volume perdagangan rata-rata Bitcoin dalam interval satu menit. ")
                f.write("Warna gradien membantu mengidentifikasi periode dengan volume perdagangan tinggi (warna lebih terang) ")
                f.write("dan rendah (warna lebih gelap). Volume perdagangan adalah indikator penting yang menunjukkan ")
                f.write("likuiditas dan minat pasar. Periode dengan volume tinggi sering kali bertepatan dengan ")
                f.write("pergerakan harga yang signifikan, sementara volume rendah dapat mengindikasikan kurangnya ")
                f.write("minat atau konsolidasi pasar.\n")
            
            print("Berhasil menyimpan visualisasi volume")
        
        # Visualisasi 3: Perbandingan harga dan volume dengan annotations
        if 'avg_price' in df.columns and 'avg_volume' in df.columns:
            fig_price_volume = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig_price_volume.add_trace(
                go.Scatter(
                    x=df['window_start'], 
                    y=df['avg_price'], 
                    name='Rata-rata Harga',
                    line=dict(color='rgb(31, 119, 180)', width=2)
                ),
                secondary_y=False,
            )
            
            fig_price_volume.add_trace(
                go.Bar(
                    x=df['window_start'], 
                    y=df['avg_volume'], 
                    name='Rata-rata Volume',
                    marker_color='rgba(255, 127, 14, 0.7)'
                ),
                secondary_y=True,
            )
            
            # Tambahkan anotasi untuk nilai tertinggi dan terendah
            if len(df) > 0:
                max_price_idx = df['avg_price'].idxmax()
                min_price_idx = df['avg_price'].idxmin()
                max_volume_idx = df['avg_volume'].idxmax()
                
                fig_price_volume.add_annotation(
                    x=df.iloc[max_price_idx]['window_start'],
                    y=df.iloc[max_price_idx]['avg_price'],
                    text="Harga Tertinggi",
                    showarrow=True,
                    arrowhead=1,
                    ax=0,
                    ay=-40
                )
                
                fig_price_volume.add_annotation(
                    x=df.iloc[min_price_idx]['window_start'],
                    y=df.iloc[min_price_idx]['avg_price'],
                    text="Harga Terendah",
                    showarrow=True,
                    arrowhead=1,
                    ax=0,
                    ay=40
                )
                
                fig_price_volume.add_annotation(
                    x=df.iloc[max_volume_idx]['window_start'],
                    y=df.iloc[max_volume_idx]['avg_volume'],
                    text="Volume Tertinggi",
                    showarrow=True,
                    arrowhead=1,
                    ax=0,
                    ay=-40,
                    yref="y2"
                )
            
            fig_price_volume.update_layout(
                title_text='Perbandingan Harga dan Volume Perdagangan Bitcoin',
                hovermode='x unified',
                template='plotly_white',
                height=500,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            fig_price_volume.update_xaxes(title_text='Waktu')
            fig_price_volume.update_yaxes(title_text='Harga (USDT)', secondary_y=False)
            fig_price_volume.update_yaxes(title_text='Volume (USDT)', secondary_y=True)
            
            # Simpan visualisasi
            fig_price_volume.write_html(os.path.join(viz_dir, 'price_volume_comparison.html'))
            fig_price_volume.write_image(os.path.join(viz_dir, 'price_volume_comparison.png'))
            
            # Tambahkan deskripsi analisis
            with open(os.path.join(viz_dir, 'price_volume_comparison_analysis.md'), 'w') as f:
                f.write("# Analisis Hubungan Harga dan Volume Perdagangan Bitcoin\n\n")
                f.write("Grafik ini menunjukkan hubungan antara harga rata-rata (garis biru) dan volume perdagangan ")
                f.write("(batang oranye) Bitcoin dalam interval satu menit. Anotasi menunjukkan titik-titik penting: ")
                f.write("harga tertinggi, harga terendah, dan volume tertinggi. Hubungan antara harga dan volume ")
                f.write("memberikan wawasan tentang kekuatan tren pasar. Volume tinggi yang mengikuti pergerakan harga ")
                f.write("mengkonfirmasi kekuatan tren tersebut, sementara divergensi antara harga dan volume dapat ")
                f.write("mengindikasikan potensi pembalikan arah. Analisis ini membantu trader mengidentifikasi momentum ")
                f.write("pasar dan membuat keputusan trading yang lebih terinformasi.\n")
            
            print("Berhasil menyimpan visualisasi perbandingan harga-volume")
        
        # Visualisasi 4: Market Impact Analysis
        if 'avg_market_impact' in df.columns:
            fig_impact = px.line(
                df, 
                x='window_start', 
                y='avg_market_impact',
                title='Analisis Market Impact Bitcoin',
                labels={'avg_market_impact': 'Market Impact', 'window_start': 'Waktu'},
                template='plotly_white',
                markers=True
            )
            
            fig_impact.update_traces(
                line=dict(width=2),
                marker=dict(size=8)
            )
            
            fig_impact.update_layout(
                xaxis_title='Waktu',
                yaxis_title='Market Impact',
                hovermode='x unified',
                height=500
            )
            
            # Simpan visualisasi
            fig_impact.write_html(os.path.join(viz_dir, 'market_impact.html'))
            fig_impact.write_image(os.path.join(viz_dir, 'market_impact.png'))
            
            # Tambahkan deskripsi analisis
            with open(os.path.join(viz_dir, 'market_impact_analysis.md'), 'w') as f:
                f.write("# Analisis Market Impact Bitcoin\n\n")
                f.write("Grafik ini menunjukkan market impact Bitcoin dalam interval satu menit. Market impact ")
                f.write("mengukur seberapa besar pengaruh transaksi terhadap pergerakan harga pasar. Nilai yang ")
                f.write("lebih tinggi menunjukkan bahwa transaksi memiliki pengaruh yang lebih besar terhadap harga, ")
                f.write("sementara nilai yang lebih rendah menunjukkan pasar yang lebih likuid dan efisien. ")
                f.write("Analisis ini penting untuk trader institusional dan algoritma yang perlu memahami ")
                f.write("bagaimana order besar dapat mempengaruhi pasar dan meminimalkan slippage.\n")
            
            print("Berhasil menyimpan visualisasi market impact")
        
        print(f"Visualisasi stream telah disimpan di {viz_dir}")
    except Exception as e:
        print(f"Error saat membuat visualisasi: {e}")
        import traceback
        print(traceback.format_exc())
        raise


if __name__ == "__main__":
    save_all_visualizations()