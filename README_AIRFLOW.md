# Apache Airflow Integration untuk Proyek Crypto ETL

Panduan ini menjelaskan cara mengatur dan menjalankan Apache Airflow untuk mengorkestrasi pipeline cryptocurrency.

## Instruksi Pengaturan

### Windows

1. Buka Command Prompt dan navigasi ke direktori proyek:
   ```
   cd "c:\Perkuliahan\SEM 6\Rekayasa Data dan Visualisasi\Project-RDV"
   ```

2. Jalankan skrip pengaturan:
   ```
   setup_airflow.bat
   ```

3. Mulai Airflow webserver (di satu terminal):
   ```
   set AIRFLOW_HOME=%cd%\airflow
   airflow webserver --port 8080
   ```

4. Mulai Airflow scheduler (di terminal lain):
   ```
   set AIRFLOW_HOME=%cd%\airflow
   airflow scheduler
   ```

### Linux/macOS

1. Buka Terminal dan navigasi ke direktori proyek:
   ```
   cd "path/to/Rekayasa Data dan Visualisasi/Project-RDV"
   ```

2. Buat skrip pengaturan dapat dieksekusi dan jalankan:
   ```
   chmod +x setup_airflow.sh
   ./setup_airflow.sh
   ```

3. Mulai Airflow webserver (di satu terminal):
   ```
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow webserver --port 8080
   ```

4. Mulai Airflow scheduler (di terminal lain):
   ```
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow scheduler
   ```

## Mengakses UI Airflow

Buka browser web Anda dan kunjungi: http://localhost:8080

- Username: admin
- Password: admin

## DAG yang Tersedia

1. **kafka_streaming_pipeline**: Mengelola streaming Kafka untuk data real-time
   - Memeriksa koneksi Kafka
   - Memulai producer dan consumer sebagai proses daemon yang berjalan terus menerus
   - Mengekspor data ke CSV
   - **Catatan**: DAG ini harus dipicu secara manual dan akan menjalankan streaming secara terus menerus

2. **stop_kafka_streaming**: Menghentikan proses streaming Kafka
   - Menghentikan producer dan consumer
   - Mengekspor data terakhir ke CSV
   - **Catatan**: DAG ini harus dipicu secara manual ketika Anda ingin menghentikan streaming

3. **scheduled_export**: Mengekspor data secara berkala
   - Mengekspor data dari database ke CSV setiap jam
   - Menyimpan file dengan timestamp dan juga memperbarui file standar

4. **spark_processing_pipeline**: Menjalankan pemrosesan Spark pada data yang dikumpulkan
   - Memeriksa driver JDBC
   - Menjalankan job Spark batch
   - Menjalankan job Spark streaming
   - Berjalan setiap 6 jam

## Alur Kerja yang Direkomendasikan

1. Mulai streaming Kafka dengan memicu `kafka_streaming_pipeline` secara manual
2. Biarkan streaming berjalan terus menerus untuk mengumpulkan data
3. Data akan diekspor secara otomatis setiap jam oleh `scheduled_export`
4. Analisis data akan dijalankan setiap 6 jam oleh `spark_processing_pipeline`
5. Ketika Anda ingin menghentikan streaming, picu `stop_kafka_streaming` secara manual

## Catatan Penting

- Proses ETL (Extract, Transform, Load) terintegrasi dalam Kafka consumer dan producer yang berjalan terus menerus
- Kafka producer mengekstrak data dari Binance API
- Kafka consumer melakukan transformasi dan memuat data ke database SQLite
- File PID disimpan di direktori proyek untuk melacak proses yang sedang berjalan

## Pemecahan Masalah

1. **Error impor**: Pastikan semua paket yang diperlukan diinstal:
   ```
   pip install -r requirements-airflow.txt
   ```

2. **Proses Kafka tidak berjalan**: Periksa file PID di direktori proyek dan pastikan proses masih berjalan

3. **Error izin**: Pastikan Anda memiliki izin yang diperlukan untuk membuat dan menulis ke direktori airflow

4. **Error database**: Jika Anda mengalami error database, Anda mungkin perlu mereset database Airflow:
   ```
   airflow db reset
   airflow db init
   ```
   (Catatan: Ini akan menghapus semua data Airflow yang ada)

## Sumber Tambahan

- [Dokumentasi Apache Airflow](https://airflow.apache.org/docs/)
- [Tutorial Airflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)