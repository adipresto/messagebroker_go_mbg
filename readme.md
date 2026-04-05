# Message Broker Golang (MBG) - v0.10.0

Message Broker yang tangguh, modern, dan berperforma tinggi berbasis Go. Dirancang untuk keandalan data (*durability*), efisiensi sumber daya skala industri, dan visibilitas total melalui dashboard serta metrik Prometheus.

## Fitur Utama

- **Performa Skala Industri (v0.10.0 Upgrade)**:
    - **O(1) Memory Iteration**: Menggunakan Go 1.23 Iterators untuk pemindaian antrean tanpa alokasi memori tambahan.
    - **Event-Driven Dispatcher**: Sistem reaktif yang hanya aktif saat ada pesan baru atau jadwal retry, meminimalkan penggunaan CPU idle.
    - **O(log N) Min-Heap Scheduler**: Penjadwalan retry yang sangat efisien untuk menangani ribuan pesan secara bersamaan.
- **Observabilitas Total**:
    - **Dashboard Real-time**: Visualisasi statistik antrean via WebSocket & RxJS.
    - **Prometheus Metrics**: Endpoint `/metrics` siap pakai untuk monitoring profesional.
    - **Runtime Profiling**: Dukungan `pprof` di `/debug/pprof/` untuk analisis performa mendalam.
- **Dukungan Multi-Protokol**: gRPC, REST, dan WebSocket (Monitoring).
- **Dukungan Payload JSON Dinamis**: Menangani objek JSON bersarang secara orisinal menggunakan Go Generics (`any`).
- **Pengiriman Otomatis (Dispatcher) & Headers**: Pengiriman ke target eksternal dengan *Custom Headers* dan logika *Header Merging*.
- **Strategi Exponential Backoff**: Retry yang adaptif untuk pengiriman yang gagal.
- **Persistensi Data (Outbox Pattern)**: Menjamin durabilitas dengan simpan-ke-disk sebelum memori.
- **Stabilitas & Hardening**: Dioptimalkan dengan *asynchronous persistence* dan **SQLite WAL Mode**.

## Roadmap Stabilitas & Performa

Sistem telah mencapai standar produksi:
- **v0.10.0 (Performance Milestone)**: Optimasi CPU/Memori dan integrasi metrik industri.
- **v0.9.1 (Stability Milestone)**: Status **20/20 Scenarios Passed** dengan isolasi servis dan pengujian thread-safe.

## Struktur Proyek

- `api/proto/`: Kontrak gRPC (Protocol Buffers).
- `config/`: Manajemen konfigurasi sistem (YAML).
- `pkg/broker/`: Logika inti antrean, heap retry, dan dispatcher reaktif.
- `pkg/server/`: Handler multi-protokol (gRPC, HTTP, WebSocket) dan endpoint observabilitas.
- `features/`: Pengujian E2E (End-to-End) menggunakan Godog.

## Cara Menjalankan

1. **Jalankan Aplikasi**:
   ```bash
   go run main.go
   ```
2. **Akses Dashboard**: `http://localhost:8081`
3. **Cek Metrik**: `http://localhost:8081/metrics`
4. **Profiling**: `http://localhost:8081/debug/pprof/`

## API Endpoints

### REST API
- `POST /api/messages`: Menambahkan pesan.
- `GET /api/messages`: Ambil (Pop) pesan.
- `GET /api/stats`: Statistik dasar.
- **Observabilitas**:
    - `GET /metrics`: Metrik format Prometheus.
    - `GET /debug/pprof/`: Index profiling Go.

### WebSocket
- `/ws`: Streaming data real-time ke Dashboard.

### gRPC Service
- `BrokerService.Push` / `BrokerService.Pop`

## Simulasi Asynchronous Request-Reply (X-Y-X)

1.  **Siapkan Target**: `go run tests/target_mock/main.go`
2.  **Kirim Tugas**: Kirim ke port `8081` dengan field `reply_to`.
3.  **Pantau Log**: Lihat bagaimana Worker Y menerima tugas dan Service X menerima callback otomatis.
