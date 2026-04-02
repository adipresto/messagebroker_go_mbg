# Message Broker Golang (MBG)

Message Broker yang tangguh dan modern berbasis Go, dirancang untuk keandalan data (*durability*), ketahanan sistem (*resilience*), dan visibilitas waktu nyata (*real-time visibility*). Proyek ini kini mendukung protokol gRPC, REST, dan WebSocket lengkap dengan Dashboard pemantauan.

## Fitur Utama

- **Dashboard Real-time**: Visualisasi statistik antrean secara langsung melalui antarmuka web (WebSocket & RxJS).
- **Dukungan Multi-Protokol**:
    - **gRPC**: Komunikasi antar layanan berperforma tinggi.
    - **REST API**: Integrasi mudah dengan aplikasi web dan klien HTTP.
    - **WebSocket**: *Streaming* data statistik waktu nyata.
- **Dukungan Payload JSON Dinamis**: Pengiriman data kompleks melalui format JSON yang ditenagai oleh tipe data generik Go (`any`). Sistem ini kini mampu menangani objek JSON bersarang secara orisinal (native) tanpa perlu konversi manual.
- **Pengiriman Otomatis (Dispatcher) & Headers**: Kemampuan mengirim payload secara otomatis ke *endpoint* eksternal target melalui HTTP maupun gRPC dengan dukungan *Custom Headers*. Secara default, sistem kini hanya mengirim **field payload** sebagai body request untuk menjaga kebersihan data.
- **Logika Header Merging & Traceability**: Mendukung pengaturan header *default* per target yang dapat ditimpa (*override*) oleh header spesifik dari masing-masing pesan. Sistem secara otomatis menyertakan `X-Message-ID` pada header untuk kemudahan pelacakan (*traceability*).
- **Strategi Exponential Backoff**: Proses percobaan ulang (retry) yang tangguh dan adaptif pada pengiriman target yang mengalami kendala.
- **Persistensi Data (Outbox Pattern)**: Pesan disimpan ke penyimpanan fisik sebelum masuk ke memori untuk menjamin durabilitas.
- **Circuit Breaker Robust**: Melindungi sistem pengiriman/penerimaan dengan status *Closed, Open, dan Half-Open* (Mekanisme *self-healing*).
- **Auto-Recovery**: Memulihkan antrean pesan dari file JSON secara otomatis saat startup.
- **Pola Asynchronous Request-Reply (Smart Worker)**: Dukungan orisinal untuk alur *decoupled feedback* di mana Service X mengirim tugas ke Service Y dan menerima balasan melalui broker tanpa koneksi langsung.
- **Aset Tersemat (Embedded)**: Dashboard web dikemas langsung ke dalam binari aplikasi menggunakan `go:embed`.

## Struktur Proyek Terbaru

- `api/proto/`: Kontrak gRPC (Protocol Buffers).
- `config/`: Manajemen konfigurasi sistem (YAML).
- `models/`: Definisi model data deklaratif.
- `pkg/broker/`: Logika inti antrean, pengelolaan persistensi JSON, serta *dispatcher* untuk mengirim pesan secara eksponensial.
- `pkg/circuitbreaker/`: Implementasi proteksi sistem *thread-safe*.
- `pkg/server/`: Handler multi-protokol (gRPC, HTTP, WebSocket) dan Dashboard.
- `features/`: Pengujian E2E (End-to-End) menggunakan Godog terhadap objek binari.
- `tests/`: Pengujian unit dan integrasi gRPC.

## Cara Menjalankan

1. **Jalankan Aplikasi**:
   ```bash
   go run main.go
   ```
2. **Akses Dashboard**:
   Buka browser dan navigasi ke `http://localhost:8081` (sesuai konfigurasi).
3. **Pengujian E2E (Godog)**:
   Aplikasi harus dikompilasi menjadi `mbg.exe` agar dapat diverifikasi secara penuh:
   ```bash
   go build -o mbg.exe .
   cd features/
   go test -v .
   ```

## API Endpoints

### REST API
- `POST /api/messages`: Menambahkan pesan ke antrean. Mendukung field opsional `headers` (object) untuk metadata atau otorisasi.
- `GET /api/messages`: Mengambil (Pop) pesan tertua.
- `GET /api/stats`: Melihat statistik ukuran antrean saat ini.
- `POST /api/targets`: Mendaftarkan target baru. Mendukung field opsional `headers` (map string to string) untuk default headers.

### WebSocket
- `/ws`: Untuk menerima pembaharuan statistik secara *real-time*.

### gRPC Service
- `BrokerService.Push`: Mengirim pesan.
- `BrokerService.Pop`: Mengambil pesan.

## Simulasi Asynchronous Request-Reply (X-Y-X)

MBG mendukung simulasi di mana Service X mengirim tugas kepada Service Y dan menerima balasan secara otomatis melalui Broker.

1.  **Siapkan Target Mock**:
    Jalankan server target yang menyamar menjadi Service X dan Service Y:
    ```bash
    go run tests/target_mock/main.go
    ```
2.  **Kirim Tugas dari Service X**:
    Kirim payload ke MBG (`8081`) dengan menyertakan `reply_to` yang merujuk pada target callback:
    ```json
    {
      "id": "TASK-001",
      "target": "worker-service",
      "payload": {
        "task": "render-video",
        "reply_to": "callback-service"
      }
    }
    ```
3.  **Pantau Log**:
    - **Worker Y** (Port 9090) akan menerima tugas.
    - Setelah jeda pemrosesan, **Service X** (Port 9091) akan menerima pesan konfirmasi "COMPLETED" dari MBG.
