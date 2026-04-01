# Changelog

Semua perubahan signifikan pada proyek **Message Broker Sendiri (MBS) akan dicatat di file ini. Log ini mengikuti format [Keep a Changelog](https://keepachangelog.com/id/1.1.0/).

## [0.5.0] - 2026-04-01

### Added

- **Sistem Dispatcher Otomatis**: Menambahkan mekanisme yang secara aktif mendistribusikan pesan dari antrean ke endpoint eksternal target.
- **Strategi Exponential Backoff**: Mengimplementasikan algoritma percobaan ulang (*retry*) dengan jeda yang bertambah secara eksponensial jika pengiriman gagal.
- **Dukungan Target gRPC & HTTP**: Target pengiriman sekarang didukung melalui protokol koneksi gRPC (`grpc://...`) dan HTTP standar.
- **Perluasan Model Data**: Menambahkan atribut state tambahan pada pesan (`NextRetry`, `RetryCount`) untuk melacak proses siklus pengiriman.
- **Circuit Breaker pada Delivery**: Proses pengiriman atau *dispatch* ke target dibalut dengan *Circuit Breaker* untuk perlindungan *self-healing*.

## [0.4.0] - 2026-03-31

### Added

- **Suite Pengujian E2E (End-to-End)**: Implementasi pengujian komprehensif menggunakan Godog terhadap file binari asli (`mbg.exe`).
- **Cakupan Skenario Multi-Protokol**: Penambahan pengujian fungsional untuk gRPC Push/Pop, REST API (POST/GET), dan sinkronisasi Dashboard.
- **Dukungan Payload JSON Tertstruktur**: Kemampuan menangani data kompleks melalui gRPC dan REST.
- **Health Check Endpoint**: Menambahkan `/api/health` untuk memverifikasi kesiapan layanan.
- **Mekanisme Manajemen Layanan dalam Tes**: Kemampuan untuk memulai dan mematikan proses `mbg.exe` secara otomatis selama eksekusi `go test`.
- **Pemetaan Jalur Relatif dalam Tes**: Memperbaiki masalah akses file selama pengujian lintas direktori (`../data/messages/`).

## [0.3.0] - 2026-03-31

### Added

- **Web Dashboard & Monitoring**: Dasbor waktu nyata berbasis web (HTML/CSS/JS) dengan RxJS.
- **Dukungan WebSocket**: *Streaming* data statistik ukuran antrean secara langsung ke dasbor melalui `/ws`.
- **REST API Support**: Menyediakan *endpoint* `/api/messages` (POST/GET) dan `/api/stats`.
- **Embedded Dashboard**: Seluruh aset dasbor disematkan dalam binari menggunakan `go:embed dashboard/*`.
- **Mekanisme Internal Pub/Sub**: Fungsionalitas `Subscribe` dan `notify()` pada Broker untuk pembaruan status waktu nyata.
- **Modularitas Server**: Pemisahan handler gRPC dan HTTP ke dalam paket `pkg/server`.

## [0.2.0] - 2026-03-31

### Added

- **Implementasi gRPC**: Menambahkan layanan gRPC (`BrokerService`) dengan operasi `Push` dan `Pop`.
- **Proto Definitions**: Mendefinisikan kontrak komunikasi di `api/proto/broker.proto`.
- **Modul Konfigurasi**: Pemisahan logika konfigurasi ke paket `config`.

## [0.1.0] - 2026-03-31

### Added

- **Inti Broker**: Implementasi dasar Message Broker dengan Go Generics.
- **Circuit Breaker**: Proteksi sistem dengan dukungan state *Half-Open*.
- **Persistensi File**: Penyimpanan berbasis JSON (Outbox Pattern).
- **Auto-Recovery**: Mekanisme pemulihan pesan dari *disk* saat startup.
