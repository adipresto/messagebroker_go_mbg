# Panduan Observabilitas MBG

Dokumen ini menjelaskan cara mengambil metrik performa dan memantau penggunaan *resource* pada Message Broker Go (MBG).

## 1. Statistik Ringkas (Dashboard)
Akses dashboard bawaan untuk melihat status antrean dan *circuit breaker* secara visual.
- **URL**: `http://localhost:8081/` (default)
- **Metrik**: 
    - `Queue Size`: Jumlah pesan aktif di memori.
    - `DLQ Size`: Jumlah pesan gagal permanen.
    - `CB Status`: Status kesehatan koneksi ke Target (Storage & Network).

---

## 2. Metrik Performa (Prometheus)
MBG mengekspos metrik dalam format Prometheus yang bisa ditarik oleh Grafana.
- **Endpoint**: `http://localhost:8081/metrics`
- **Metrik Utama**:
    - `mbg_messages_pushed_total`: Akumulasi pesan masuk.
    - `mbg_messages_dispatched_total`: Akumulasi pesan sukses terkirim.
    - `mbg_queue_size`: Jumlah pesan saat ini (Gauge).
    - `mbg_dispatch_latency_seconds`: Statistik kecepatan pengiriman (Histogram).

---

## 3. Profiling Resource (pprof)
Gunakan `pprof` untuk menganalisis penggunaan CPU dan Memori hingga ke level baris kode.
- **UI Index**: `http://localhost:8081/debug/pprof/`

### A. Analisis Memori (Heap)
Untuk melihat fungsi mana yang memakan RAM paling banyak:
```bash
# Lihat secara visual di browser (port 8082)
go tool pprof -http=:8082 http://localhost:8081/debug/pprof/heap
```

### B. Analisis CPU
Untuk melihat fungsi mana yang paling sibuk selama 30 detik:
```bash
# Jalankan perintah ini, lalu tunggu 30 detik
go tool pprof -http=:8082 http://localhost:8081/debug/pprof/profile
```

### C. Analisis Goroutine
Untuk melihat apakah ada *worker* yang nyangkut (leak):
```bash
go tool pprof -http=:8082 http://localhost:8081/debug/pprof/goroutine
```

---

## 4. Load Testing (k6)
Gunakan k6 untuk menguji performa di bawah tekanan tinggi.
- **Lokasi Script**: `tests/load_test/k6/push_test.js`
- **Perintah**:
```bash
k6 run tests/load_test/k6/push_test.js
```
- **Hasil**: k6 akan memberikan ringkasan **RPS** (Requests Per Second) dan **P95 Latency**.

---

## Tips Pengecekan Cepat
Jika Anda merasa MBG melambat:
1. Cek `/metrics` untuk melihat apakah `mbg_queue_size` terus membengkak tanpa penurunan.
2. Cek `/debug/pprof/` bagian `heap` untuk memastikan tidak ada alokasi memori yang tidak wajar.
3. Gunakan `top` atau `Task Manager` untuk melihat konsumsi CPU secara keseluruhan.
