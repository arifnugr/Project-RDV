import streamlit as st
from pymongo import MongoClient
from datetime import datetime
import os   

# Konfigurasi MongoDBMore actions
mongo_host = os.environ.get('MONGODB_HOST', 'mongodb')
mongo_port = os.environ.get('MONGODB_PORT', '27017')
mongo_db = os.environ.get('MONGODB_DATABASE', 'market_data')

def get_visualization_from_db(viz_name):
    try:
        client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        db = client[mongo_db]
        viz_collection = db["visualizations"]
        
        # Ambil visualisasi terbaru
        viz = viz_collection.find_one({"name": viz_name}, sort=[("timestamp", -1)])
        
        if viz and "html_content" in viz:
            return viz["html_content"]
        return None
    except Exception as e:
        st.error(f"Error mengambil visualisasi {viz_name}: {e}")
        return None

def main():
    st.set_page_config(page_title="Dashboard Bitcoin", layout="wide")
    st.title("📊 Dashboard Analisis Pasar Bitcoin")
    
    # Sidebar
    st.sidebar.title("Navigasi")
    
    # Tombol refresh
    if st.sidebar.button('Refresh Data'):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Tampilkan visualisasi
    st.header("Visualisasi Stream Data")
    
    # Tren Harga
    st.subheader("Tren Harga Bitcoin")
    price_viz = get_visualization_from_db("price_trends")
    if price_viz:
        st.components.v1.html(price_viz, height=500)
    else:
        st.warning("Visualisasi tren harga tidak tersedia")
    
    st.markdown("""
    **Analisis Tren Harga:**
    
    Grafik ini menunjukkan pergerakan harga Bitcoin dalam interval satu menit berdasarkan data stream. Area bayangan menunjukkan rentang antara harga minimum dan maksimum, memberikan gambaran tentang volatilitas jangka pendek. Visualisasi ini memungkinkan kita mengidentifikasi volatilitas dan tren pergerakan harga secara real-time.
    """)
    
    # Volume Perdagangan
    st.subheader("Volume Perdagangan Bitcoin")
    volume_viz = get_visualization_from_db("volume_analysis")
    if volume_viz:
        st.components.v1.html(volume_viz, height=500)
    else:
        st.warning("Visualisasi volume perdagangan tidak tersedia")
    
    st.markdown("""
    **Analisis Volume Perdagangan:**
    
    Grafik batang ini menunjukkan volume perdagangan Bitcoin dalam interval satu menit dari data stream. Warna gradien membantu mengidentifikasi periode dengan volume perdagangan tinggi dan rendah. Volume perdagangan real-time ini memberikan indikasi likuiditas dan minat pasar dalam jangka pendek.
    """)
    
    # Perbandingan Harga dan Volume
    st.subheader("Hubungan Harga dan Volume Perdagangan")
    price_volume_viz = get_visualization_from_db("price_volume_comparison")
    if price_volume_viz:
        st.components.v1.html(price_volume_viz, height=500)
    else:
        st.warning("Visualisasi hubungan harga dan volume tidak tersedia")
    
    st.markdown("""
    **Analisis Hubungan Harga dan Volume:**
    
    Grafik ini menunjukkan hubungan antara harga rata-rata dan volume perdagangan Bitcoin dari data stream. Anotasi menunjukkan titik-titik penting dalam data. Analisis hubungan ini membantu trader mengidentifikasi momentum pasar dan membuat keputusan trading yang lebih terinformasi dalam jangka pendek.
    """)
    
    # Market Impact
    st.subheader("Market Impact Bitcoin")
    impact_viz = get_visualization_from_db("market_impact")
    if impact_viz:
        st.components.v1.html(impact_viz, height=500)
    else:
        st.warning("Visualisasi market impact tidak tersedia")
    
    st.markdown("""
    **Analisis Market Impact:**
    
    Market impact mengukur seberapa besar pengaruh transaksi terhadap pergerakan harga pasar. Nilai yang lebih tinggi menunjukkan bahwa transaksi memiliki pengaruh yang lebih besar terhadap harga, sementara nilai yang lebih rendah menunjukkan pasar yang lebih likuid dan efisien. Analisis ini penting untuk trader yang perlu memahami bagaimana order mereka dapat mempengaruhi pasar.
    """)

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("© 2025 Kelompok Rekayasa Data dan Visualisasi")
    st.sidebar.info(f"Pembaruan terakhir: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
