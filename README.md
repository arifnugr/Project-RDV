# Project-RDV

Proyek Rekayasa Data dan Visualisasi untuk analisis data pasar cryptocurrency.

## Struktur Proyek

```
Project-RDV/
├── data/                  # Data SQLite dan CSV
├── etl/                   # Kode ETL pipeline
│   ├── extractor.py       # Ekstraksi data dari Binance
│   ├── transformer.py     # Transformasi data
│   ├── loader.py          # Loading data ke SQLite
│   ├── main.py            # Main pipeline sederhana
│   ├── main_spark.py      # Main pipeline dengan integrasi semua komponen
│   ├── spark_integration.py # Modul integrasi dengan Apache Spark
│   ├── scheduler.py       # Scheduler untuk tugas periodik
│   ├── stream.producer.py # Kafka producer
│   ├── stream_consumer.py # Kafka consumer
│   ├── visualizer.py      # Visualisasi data
│   └── ...
├── Dockerfile             # Konfigurasi Docker untuk aplikasi
├── docker-compose.yml     # Orkestrasi multi-container dengan semua komponen
├── requirements.txt       # Dependensi Python
├── spark_processor.py     # Aplikasi Spark untuk processing data
├── spark_submit.sh        # Script untuk submit job Spark
└── run_all.bat            # Script untuk menjalankan semua komponen
```

## Menjalankan dengan Docker

### Prasyarat

- Docker dan Docker Compose terinstall
- Koneksi internet untuk mengunduh image Docker

### Menjalankan Semua Komponen

Untuk menjalankan aplikasi dengan semua komponen (ETL, Kafka, Spark, Scheduler):

1. Jalankan script `run_all.bat` atau eksekusi:

```bash
docker-compose up -d
```

2. Untuk melihat log aplikasi utama:

```bash
docker-compose logs -f app
```

3. Untuk melihat log Kafka producer:

```bash
docker-compose logs -f kafka-producer
```

4. Untuk melihat log Kafka consumer:

```bash
docker-compose logs -f kafka-consumer
```

5. Untuk melihat log scheduler:

```bash
docker-compose logs -f scheduler
```

6. Akses Spark UI di browser:

```
http://localhost:8080
```

7. Untuk menghentikan semua container:

```bash
docker-compose down
```

## Komponen Utama

### ETL Pipeline

Pipeline ETL mengekstrak data dari Binance API, melakukan transformasi, dan menyimpannya ke database SQLite.

### Kafka Streaming

Komponen streaming menggunakan Kafka untuk mengirim data secara real-time dari producer ke consumer:
- Producer: Mengekstrak data dari Binance dan mengirimkannya ke topic Kafka
- Consumer: Menerima data dari topic Kafka, melakukan transformasi, dan menyimpannya ke database

### Apache Spark

Spark digunakan untuk pemrosesan data besar dengan kemampuan:
- Windowing analysis
- Aggregations
- Batch dan stream processing

### Scheduler

Scheduler menjalankan tugas-tugas terjadwal seperti:
- Export data ke CSV
- Menjalankan batch processing Spark
- Tugas-tugas pemeliharaan lainnya

## Troubleshooting

- Jika visualisasi tidak muncul, pastikan X11 forwarding dikonfigurasi dengan benar
- Jika Kafka error, cek koneksi ke Zookeeper dengan `docker-compose logs zookeeper`
- Untuk masalah Spark, periksa log di Spark UI (http://localhost:8080)
- Jika container tidak berjalan, coba restart dengan `docker-compose restart`