# Arsitektur Sistem Message Broker (MBS) - Lanjutan

Sistem MBS kini memiliki kemampuan observabilitas yang lebih baik dan strategi pengujian yang komprehensif.

## Diagram Arsitektur Komponen dan Pengujian

```mermaid
graph TD
    subgraph "Testing Suite"
        Godog[Godog Runner] -- "Executes" --> Binary[mbg.exe]
        Godog -- "Verifies" --> DiskCheck[Disk Storage Check]
        Godog -- "Connects" --> TestClient[HTTP/gRPC/WS Client]
    end

    subgraph "Production Runtime (mbg.exe)"
        TestClient -- "gRPC Push/Pop" --> GRPC[gRPC Service Layer]
        TestClient -- "HTTP POST/GET/Stats" --> HTTP[HTTP API Layer]
        TestClient -- "WebSocket Stats" --> WS[WebSocket Handler]
        
        WS -- "Internal Monitoring" --> Dashboard[MBG Dashboard]
        
        GRPC -- "Wrapped in CB" --> Broker[Broker Core]
        HTTP -- "Wrapped in CB" --> Broker
        
        subgraph "Broker Internal"
            Broker -- "Persist Asynchronously" --> Disk[Storage Layer / SQLite WAL]
            Broker -- "Manage Memory" --> RAM[Memory Queue]
            Broker -- "Propagate Update" --> Notify[Internal Pub/Sub]
            Broker -- "Triggers Delivery" --> Dispatcher[Dispatcher System]
        end
        
        subgraph "External Delivery (v0.7.0)"
            Dispatcher -- "Header Merging & Mapping" --> TargetHTTP[External HTTP Target]
            Dispatcher -- "Metadata & Header Mapping" --> TargetGRPC[External gRPC Target]
            Dispatcher -- "Exponential Backoff Update" --> Broker
        end

        Notify -- "Broadcast Stats" --> WS
        Disk -- "Load on Startup" --> Broker
    end
```

## Komponen Utama Baru (Lanjutan)

### 1. Strategi Pengujian E2E (v0.9.1 Breakthrough)
Sistem ini telah mencapai stabilitas **100% (20/20 scenarios passed)** melalui strategi pengujian terhadap objek binari asli (`mbg.exe`) yang sangat terisolasi:
- **Service Isolation (The Fix)**: Masalah *race conditions* dan *flakiness* pada gRPC diatasi dengan memberikan setiap mock server buffer pesan yang terisolasi dan dilindungi mutex.
- **Verifikasi Protokol**: Pengujian secara sinkron terhadap gRPC, HTTP, dan WebSocket dengan asersi yang lebih presisi pada field `id` dan metadata.
- **Isolasi Skenario**: Setiap skenario pengujian secara dinamis memulai dan mematikan proses `mbg.exe` untuk menjamin kebersihan state antar pengujian.
- **Validasi Durabilitas**: Pengujian memverifikasi keberadaan data di SQLite/JSON untuk memastikan persistensi benar-benar terjadi tanpa menghambat aliran dispatcher (asynchronous).

### 2. Dukungan JSON Dinamis (Go Generics)
Implementasi inti `Broker[T any]` kini menggunakan tipe data `any` yang memungkinkan sistem untuk menerima, menyimpan, dan meneruskan payload JSON apa pun secara transparan. Handler gRPC dan HTTP melayani sebagai gerbang (*gateways*) yang melakukan unmarshaling/marshaling secara otomatis ke dalam model data `models.Message[any]`, menjamin fleksibilitas tingkat tinggi tanpa mengorbankan integritas model pesan.

### 3. Kemampuan Observabilitas (Dashboard Only)
Dasbor waktu nyata (MBG Dashboard) bukan hanya sekadar visualisasi, tetapi juga berfungsi sebagai titik verifikasi kesehatan sistem (*health check*) yang memantau metrik antrean secara kontinu melalui WebSockets. Jalur WebSocket (`/ws`) saat ini bersifat *read-only* dan didedikasikan sepenuhnya untuk operasional Dashboard.

### 4. Automated Dispatch, Exponential Backoff & Header Support
Sistem pengiriman sekarang secara otomatis memonitor seluruh pesan antrean yang siap untuk diteruskan (*ready to be dispatched*) menggunakan `Dispatcher` yang berjalan secara asynchronous:
- Secara adaptif mengubah jarak waktu pengiriman ulang (*NextRetry*) berdasarkan kelipatan *Exponential Backoff* saat percobaan pertama gagal.
- **Header Propagation**: Pengiriman pesan kini mendukung pengiriman metadata tambahan (sepertitoken otorisasi) melalui HTTP Headers dan gRPC Metadata. Sistem secara otomatis menyertakan `X-Message-ID` untuk pelacakan.
- **Payload Refinement**: Khusus untuk target HTTP, sistem kini melakukan *decoupling* antara metadata broker dan data bisnis dengan hanya mengirim field `payload` di dalam request body.
- **Header Merging Strategy**: Sistem menggabungkan *default headers* dari konfigurasi target dengan *dynamic headers* dari payload pesan, di mana header dari pesan memiliki prioritas tertinggi.
- Integrasi mulus yang melindungi target maupun layanan melalui limitasi yang aman via *Circuit Breaker*.

## Pola Asynchronous Request-Reply (X-Y-X)

Mulai v0.8.0, MBG secara formal mendukung (melalui simulasi) pola Request-Reply yang memungkinkan layanan untuk berinteraksi secara asinkron tanpa kehilangan kepastian hasil.

### Alur Kerja Pesan:

```mermaid
sequenceDiagram
    participant X as Service X (Requester)
    participant MBG as MBG (The Broker)
    participant Y as Service Y (Worker)

    X->>MBG: 1. Push Task (Target: Y, Reply: X)
    MBG-->>X: 2. Respond "Pushed" (Async start)
    MBG->>Y: 3. Dispatch Task to Webhook
    Y-->>MBG: 4. Ack Delivery
    Note over Y: Long-running process...
    Y->>MBG: 5. Push Result (Target: X)
    MBG->>X: 6. Dispatch Result to Callback URL
```

### Keuntungan Arsitektural:
1.  **Decoupling**: Service X tidak perlu tahu alamat IP Service Y, dan sebaliknya. Mereka hanya perlu tahu nama target di MBG.
2.  **Persistence**: Jika Service X mati saat Service Y sedang bekerja, MBG akan menyimpan hasil pekerjaan tersebut dan mencoba mengirimkannya kembali saat Service X hidup kembali (*Self-healing via Retry*).
3.  **Scalability**: Worker Y bisa ditambah sesuka hati (horizontal scaling) tanpa mengganggu logika pengiriman balik ke X.

---

## Mekanisme Keamanan & Ketahanan (Lanjutan)

- **Zero-Block Dispatcher**: Logika pengiriman (dispatching) dipisahkan dari I/O persistensi. Dispatcher hanya mengambil data dari memori dan mengirimkannya, sementara proses penulisan ke disk dilakukan secara asinkron untuk menghilangkan *stalling*.
- **SQLite WAL (Write-Ahead Logging)**: Penggunaan mode WAL menjamin bahwa penulisan data oleh broker tidak menghambat pembacaan data statistik untuk dashboard, meningkatkan stabilitas di bawah beban tinggi.
- **Manajemen Proses**: Penggunaan utilitas sistem (seperti `taskkill` pada Windows) selama pengujian untuk mengelola siklus hidup aplikasi secara otomatis.
- **Health Check Integration**: Setiap klien (termasuk unit pengujian) kini melakukan pengecekan kesehatan melalui `/api/health` sebelum memulai transaksi, meningkatkan ketersediaan layanan.
