# Lab 02 - Mini-DHT Registry và Messaging Client

Đây là bài thực hành cho slide cuối của `Lect02_Distributed_Communication_and_Naming.pdf`.
Hệ thống dùng Python 3.11, TCP sockets, JSON framing, SQLite và Docker Compose.
Mỗi node là một tiến trình độc lập, tham gia vòng Chord, giữ predecessor,
successor list và finger table, lưu bản ghi registry và nhận tin nhắn.

## Mục tiêu

- Tra cứu tên logic bằng Chord trên không gian ID `2^32`; ID được băm bằng SHA-256.
- Giao tiếp node bằng TCP với frame `4 byte big-endian length | JSON UTF-8`.
- Lưu bản ghi tại owner và tối đa hai successor tiếp theo.
- Giữ tên/ID ổn định khi endpoint của node đổi; dùng revision để loại bản ghi cũ.
- Retry gửi tin trong một deadline và chống gửi trùng bằng `message_id`.
- Dùng SQLite volume để giữ identity, revision, registry và inbox khi container tạo lại.
- Cung cấp CLI `lookup`, `register`, `send`, `status`, `inspect`.

Client bên ngoài chỉ kết nối tới seed bằng RPC. Client không mở listener, không
tạo SQLite và không tham gia vòng Chord.

## Kiểm thử cục bộ

Từ thư mục lab:

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
```

Mã nguồn chỉ dùng standard library. Test dùng `DHT_BITS=8` để vòng nhỏ và chạy nhanh.

## Chạy demo Docker

Docker Desktop phải chạy Linux Engine.

```powershell
docker compose build
docker compose up -d
docker compose ps
docker compose exec client python /app/scripts/wait_for_ring.py `
  --seed node-a:7000,node-b:7000,node-c:7000,node-d:7000,node-e:7000
```

Compose dùng healthcheck TCP cho các node; node mới join qua node-a sau khi node-a sẵn sàng.

Tra cứu node hiện tại:

```powershell
docker compose exec client python -m dht_lab.cli client lookup node-c `
  --seed node-a:7000,node-b:7000,node-c:7000
```

Gửi tin với ID tự sinh:

```powershell
docker compose exec client python -m dht_lab.cli client send node-c `
  --from client-01 --message "Xin chao tu DHT" `
  --seed node-a:7000,node-b:7000,node-c:7000 --timeout 30
```

Gửi lại cùng ID để kiểm tra at-most-once ở inbox:

```powershell
docker compose exec client python -m dht_lab.cli client send node-c `
  --from client-01 --message "Xin chao tu DHT" --message-id demo-001 `
  --seed node-a:7000,node-b:7000,node-c:7000 --timeout 30
docker compose exec client python -m dht_lab.cli node inspect `
  --seed node-c:7000 --messages
```

## Demo node lỗi

Giữ node-c đang chạy, dừng một node định tuyến rồi gửi qua các seed còn lại:

```powershell
docker compose stop node-b
docker compose exec client python -m dht_lab.cli client send node-c `
  --from client-01 --message "Sau khi node loi" `
  --seed node-a:7000,node-c:7000,node-d:7000 --timeout 30
```

Kịch bản này kiểm tra successor list, retry và replica. Nếu chính node đích
không hoạt động trong deadline, client trả lỗi `DELIVERY_TIMEOUT`; hệ thống
không giả vờ đã giao tin khi chưa nhận ACK.

## Demo đổi IP

Volume `node-c-data` giữ identity và revision của node-c:

```powershell
docker compose stop node-c
docker compose -f compose.yaml -f compose.ip-change.yaml up -d --build node-c
docker compose exec client python -m dht_lab.cli client lookup node-c `
  --seed node-a:7000,node-b:7000,node-d:7000
docker compose exec client python -m dht_lab.cli client send node-c `
  --from client-01 --message "IP moi" `
  --seed node-a:7000,node-b:7000,node-d:7000 --timeout 30
```

Kết quả cần kiểm tra: `node_id` giữ nguyên, địa chỉ chuyển từ
`172.30.0.13` sang `172.30.0.113`, revision tăng và message nhận ACK.

## Benchmark

Benchmark khởi động các TCP node, tạo một snapshot ổn định theo cùng đáp án
oracle về successor, predecessor và toàn bộ finger table, rồi tạm dừng
maintenance loop để đo routing thay vì đo thời gian hội tụ. Mỗi lookup được
kiểm tra owner đúng trước khi ghi latency/hop và tỷ lệ lỗi.

```powershell
$env:PYTHONPATH = "src"
python scripts/benchmark.py --nodes 4 8 16 32 --queries 1000 --bits 32 --runs 3 `
  > benchmark-results.json
```

Kết quả JSON gồm run, kích thước vòng, latency trung bình/p95, số hop trung bình
và hop lớn nhất cho finger routing và successor-only baseline.

## Giao thức và giới hạn

Các operation chính là `PING`, `FIND_SUCCESSOR`, `FIND_SUCCESSOR_LINEAR`,
`NOTIFY`, `ANNOUNCE`, `STORE_RECORD`, `GET_RECORD`, `RESOLVE`, `REGISTER`,
`SEND`, `DELIVER` và `LIST_MESSAGES`. Request phải có `request_id`; frame tối đa
1 MiB, timeout RPC mặc định 2 giây, deadline gửi mặc định 30 giây.

Bản lab xử lý một node lỗi tại một thời điểm sau khi vòng ổn định, với ít nhất
một seed còn sống và các replica còn truy cập được. Hệ thống chưa triển khai TLS,
xác thực peer, split-brain, mạng bị chia cắt, quorum, hàng đợi offline sau khi
client thoát hoặc phục hồi khi toàn bộ replica bị mất.
