# Gold Price Intelligence Pipeline

Proyek ini adalah sistem **Data Engineering & Machine Learning** *end-to-end* untuk memantau, memproses, dan memprediksi harga emas (XAUUSD) berdasarkan data makroekonomi (Inflasi & Nilai Tukar USD/IDR).

## 🏗️ Arsitektur Sistem
* **Ingestion (Bronze):** Apache Airflow menarik data mentah dari API dan menyimpannya di Object Storage MinIO (berbasis S3).
* **Processing (Silver & Gold):** Apache Spark membersihkan data dan meraciknya menjadi *Analytical Table*, lalu menyimpannya ke PostgreSQL.
* **Machine Learning:** Model dilatih (Linear Regression & Random Forest), dilacak (versioning) menggunakan MLflow, dan memprediksi harga emas masa depan.
* **Monitoring & Alerting:** Grafana memantau *resource* sistem (CPU/RAM/Disk) serta status Pipeline, dan mengirimkan notifikasi *alert* otomatis via Telegram.
* **Dashboard (Data Viz):** Streamlit (dikoneksikan via engine Trino) memvisualisasikan tren pasar harian dan hasil *Forecasting* model ML.

---

## 🚀 Cara Menjalankan Proyek (Mulai dari Nol)

### ⚠️ Prasyarat Penting (Wajib Dibaca)
Sebelum memulai *ingest* data dan menjalankan *pipeline*, pastikan kamu sudah melengkapi hal berikut:
1. **Minta file `.env` dari pemilik proyek (Project Owner).** File ini sangat krusial karena berisi kredensial rahasia (seperti Token Bot Telegram untuk Alerting, dll) yang sengaja tidak di-*upload* ke repository/Github. 
   > Taruh file `.env` tersebut di direktori paling luar (sejajar dengan file `README.md` ini). Tanpa file ini, sistem notifikasi dan *pipeline* bisa gagal berjalan!
2. Pastikan komputermu sudah terinstal **Docker Desktop** (atau Docker Engine + Docker Compose).

### 1. Menjalankan Infrastruktur (Docker)
Pastikan aplikasi **Docker Desktop** sudah menyala di komputermu, lalu buka terminal/command prompt, dan jalankan perintah berikut:
```bash
cd docker
docker compose up -d
```
> 💡 *Tunggu sekitar 2-3 menit agar semua layanan (Airflow, Spark, Postgres, MinIO, Trino, Streamlit, Grafana, MLflow) menyala sempurna sebelum lanjut ke langkah berikutnya.*

### 2. Eksekusi Pipeline secara Berurutan (Langkah Demo)
Buka UI Apache Airflow di browser: **http://localhost:8080**
*(Login: `admin` / Password: `admin`)*

Jalankan ketiga DAG berikut **secara berurutan**, dan pastikan DAG sebelumnya sudah berstatus **Success** (warna hijau) sebelum lanjut ke DAG berikutnya:

1. **Trigger DAG `batch_pipeline` (Ingest ke Bronze)**
   - **Tujuan:** Menarik data mentah (Inflasi, Harga Emas, Kurs) dari API publik ke *bucket* MinIO (`bronze-batch`).
   - **Cara:** Klik tombol **Trigger DAG (ikon Play)**. Tunggu hingga selesai.

2. **Trigger DAG `processing_pipeline` (Bronze ke Silver & Gold)**
   - **Tujuan:** Apache Spark bekerja membersihkan data ke *bucket* MinIO (`silver-batch`) dan menggabungkannya menjadi tabel analitik `fact_market_daily` di database PostgreSQL.
   - **Cara:** Klik tombol **Trigger DAG**. Tunggu hingga selesai. *(Catatan: Proses ini wajib sukses agar grafik di Streamlit bisa muncul!)*

3. **Trigger DAG `ml_training_pipeline` (Training & Prediksi)**
   - **Tujuan:** Melatih model AI berdasarkan data yang sudah dibersihkan tadi, menyimpan modelnya di MLflow, dan menghasilkan data prediksi (Forecasting) ke database.
   - **Cara:** Klik tombol **Trigger DAG**. Tunggu hingga selesai. Hasilnya akan otomatis mengirim notifikasi prediksi ke Telegram.

### 3. Eksekusi Streaming Pipeline (Opsional - Real-Time Data)
Pipeline *streaming* berjalan terpisah dari Airflow karena membutuhkan aplikasi terminal **MetaTrader 5 (MT5)** yang berjalan di Windows (komputer lokal) untuk mendapatkan data *tick* Harga Emas secara *real-time*.

Jika kamu ingin menjalankan *streaming*:
1. Pastikan aplikasi MetaTrader 5 sudah terbuka dan *login* ke akun Demo.
2. Buka terminal baru di komputermu (di luar Docker), masuk ke folder proyek ini, dan jalankan *Virtual Environment* (jika ada).
3. Jalankan *Producer* untuk menarik data dari MT5 dan mengirimnya ke Kafka:
   ```bash
   python src/streaming/mt5_producer.py
   ```
4. Buka terminal lain, masuk ke container Spark untuk menjalankan *Consumer* yang akan membaca data dari Kafka dan menyimpannya ke MinIO (`silver-streaming`):
   ```bash
   docker exec -it ipbd-spark-master spark-submit /opt/airflow/src/streaming/spark_consumer.py
   ```

### 4. Memantau Hasil di Dashboard Aplikasi (Streamlit)
Buka Streamlit di browser: **http://localhost:8501**
*(Jika diminta Login, gunakan Username: `admin` / Password: `admin123`)*

- **Tab 1 (Pasar & Tren):** Menampilkan metrik KPI (Inflasi, Kurs, Emas) dan grafik harga emas historis.
- **Tab 2 (Forecasting):** Menampilkan grafik tebakan (prediksi) harga emas bulan depan berdasarkan model Machine Learning yang baru saja dilatih.

### 5. Memantau Performa Sistem (Grafana)
Buka Grafana di browser: **http://localhost:3001**
*(Login: `admin` / Password: `admin`)*

- Buka menu **Dashboards > IPBD Monitoring Dashboard**.
- Di sini kamu bisa memantau kesehatan *hardware* komputermu (CPU/RAM/Disk space) dan log riwayat eksekusi pipeline.
- Jika terdeteksi anomali (misalnya CPU naik mencapai 90%, storage kepenuhan, atau pipeline gagal), sistem sudah terkonfigurasi untuk **otomatis mengirim pesan Alert ke Bot Telegram!**
