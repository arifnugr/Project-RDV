import streamlit as st
from datetime import datetime
import pandas as pd

# import fungsi dari crypto_analysis.py
from crypto_analysis import (
    get_data_from_db,
    prepare_data,
    plot_price_over_time,
    plot_volume_analysis,
    plot_volume_price_comparison
)

def main():
    st.set_page_config(page_title="Dashboard Bitcoin", layout="wide")
    st.title("📊 Dashboard Analisis Pasar Bitcoin")

    # 1. Load & prepare data
    df_raw = get_data_from_db()
    df = prepare_data(df_raw)
    if df is None:
        st.error("Gagal memuat data dari MongoDB")
        return

    # 2. Sidebar: pilih rentang tanggal
    min_date = df['timestamp'].dt.date.min()
    max_date = df['timestamp'].dt.date.max()
    start_date, end_date = st.sidebar.date_input(
        "Pilih Rentang Tanggal",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    mask = (df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)
    df_sel = df.loc[mask]

    # 3. Tren Harga dengan range slider & selector
    st.subheader("🎯 Tren Harga Bitcoin")
    fig_price = plot_price_over_time(df_sel)
    fig_price.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1d", step="day", stepmode="backward"),
                    dict(count=7, label="1w", step="day", stepmode="backward"),
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(visible=True)
        )
    )
    st.plotly_chart(fig_price, use_container_width=True)

    # 4. Volume Perdagangan
    st.subheader("📊 Volume Perdagangan Bitcoin")
    fig_vol = plot_volume_analysis(df_sel)
    fig_vol.update_layout(
        xaxis=dict(rangeslider=dict(visible=True))
    )
    st.plotly_chart(fig_vol, use_container_width=True)

    # 5. Perbandingan Harga & Volume
    st.subheader("🔄 Hubungan Harga & Volume Perdagangan")
    fig_pv = plot_volume_price_comparison(df_sel)
    fig_pv.update_layout(
        xaxis=dict(rangeslider=dict(visible=True))
    )
    st.plotly_chart(fig_pv, use_container_width=True)

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"Pembaruan terakhir: {datetime.now():%Y-%m-%d %H:%M:%S}")

if __name__ == "__main__":
    main()