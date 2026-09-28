# Báo cáo Lab 02 - Mini-DHT Registry và Messaging Client

## 1. Bài toán và mục tiêu

Bài lab xây dựng một dịch vụ naming phân tán tối giản. Tên logic được băm vào
vòng Chord để tìm owner; registry trả về endpoint hiện tại; client dùng endpoint
đó để gửi tin. Tên và ID của node độc lập với địa chỉ IP, nên node có thể trở
lại với IP mới mà không làm client đổi tên đích.

Thiết kế bám theo phần Communication và Naming trong giáo trình Tanenbaum & van
Steen và slide Lect02: TCP sockets cho giao tiếp transient, JSON framing cho
marshaling, Chord cho flat-name resolution, SQLite cho dữ liệu bền vững và
replication để che giấu một node lỗi.

## 2. Kiến trúc

Mỗi node chạy cùng một TCP server và có các trạng thái:

- `node_id = SHA256("node:" + name) mod 2^m` là định danh ổn định.
- `successor`, `predecessor`, successor list tối đa ba node và `m` finger entries.
- Registry key là `SHA256("service:" + name) mod 2^m`.
- SQLite lưu identity, revision, registry và inbox.
- Maintenance loop thực hiện stabilize, notify, kiểm tra predecessor, sửa finger
  và đồng bộ replica.

Client bên ngoài chỉ giữ danh sách seed endpoint. Nó không mở socket server,
không có predecessor/successor và không tạo database. Request được gửi tới seed;
node nhận request thực hiện lookup và trả kết quả.

## 3. Giao thức

Mỗi frame TCP có dạng:

```text
4 byte unsigned big-endian length | JSON UTF-8 payload
```

Payload là object có `request_id` và `op`. Frame tối đa 1 MiB. Các operation
được dùng trong demo gồm:

| Operation | Vai trò |
|---|---|
| `PING` | kiểm tra endpoint và lấy `NodeRef` |
| `FIND_SUCCESSOR` | lookup bằng finger table |
| `FIND_SUCCESSOR_LINEAR` | baseline đi qua successor |
| `NOTIFY`, `ANNOUNCE` | hội tụ predecessor và lan truyền endpoint mới |
| `STORE_RECORD`, `GET_RECORD`, `RESOLVE`, `REGISTER` | registry |
| `SEND`, `DELIVER` | định tuyến và giao message |
| `STATUS`, `LIST_MESSAGES` | quan sát và demo |

`NodeRef` gồm tên, node ID, host, port và revision. Bản ghi registry gồm cùng
endpoint và revision. Revision thấp bị bỏ qua; cùng revision nhưng nội dung
khác bị báo xung đột.

## 4. Lookup và replication

Node `p` dùng finger entry gần nhất nhưng vẫn nằm trước khóa theo chiều kim đồng
hồ. Khi successor hoặc finger lỗi, node thử các ứng viên còn lại và successor
list, với giới hạn 64 hop và phát hiện routing loop. Trong vòng ổn định, Chord
giảm số bước kỳ vọng từ tuyến tính theo số node xuống `O(log N)`.

Owner và tối đa hai successor tiếp theo cùng lưu một bản ghi. Với vòng có từ hai
node, `REGISTER` chỉ thành công khi owner và ít nhất một replica ACK. Client đọc
owner trước; nếu owner lỗi, node dùng replica hint, successor list hoặc các
endpoint đã biết và chọn bản ghi có revision lớn nhất.

Đây là replication nhất quán cuối cùng: một chủ thể cập nhật một tên; các bản
sao hội tụ sau khi update ngừng. Hệ thống không cố cung cấp consensus hoặc
quorum đầy đủ.

## 5. Gửi tin và xử lý lỗi

Luồng gửi là:

```text
client -> seed -> Chord owner -> registry endpoint -> DELIVER -> SQLite commit -> ACK
```

Một `message_id` được giữ nguyên trong toàn bộ retry. Node đích kiểm tra cả tên
và node ID của người nhận, ghi inbox trước khi ACK, và coi cùng message ID với
cùng nội dung là duplicate hợp lệ. Cùng ID với nội dung khác là conflict.

RPC có timeout mặc định 2 giây; một lần gửi có deadline mặc định 30 giây. Khi
hết deadline, client trả `DELIVERY_TIMEOUT` cùng message ID và không tuyên bố
đã giao thành công. Mô hình lỗi mục tiêu là một node lỗi tại một thời điểm sau
khi vòng đã ổn định.

## 6. Kiểm thử

Test tự động hiện có kiểm tra:

- interval wrap, successor boundary, hash và kích thước finger table;
- frame ghép/tách, Unicode, JSON framing và giới hạn 1 MiB;
- join, lookup, registry, duplicate delivery, stale revision;
- loại successor lỗi khỏi routing;
- giữ node ID và cập nhật endpoint sau khi đổi IP.

Chạy bằng:

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
```

Bên benchmark tạo các node TCP thật, thiết lập một snapshot ổn định theo oracle về
successor/predecessor/finger và tạm dừng maintenance loop để loại chi phí hội
tụ khỏi phép đo. Hai phép đo dùng cùng key và source node; mỗi kết quả đều kiểm
tra owner đúng và ghi tỷ lệ lỗi. Chạy với `--nodes 4 8 16 32`,
`--queries 1000`, `--runs 3`.

## 7. Kết quả cần trình bày khi demo

1. Năm node hội tụ; `inspect` hiển thị ID, predecessor, successor list và finger table.
2. Register/lookup trả owner, trace và hop count.
3. Gửi lại cùng message ID chỉ tạo một dòng inbox.
4. Dừng một node định tuyến; seed còn sống vẫn lookup và gửi được.
5. Dừng rồi tạo lại node-c với IP mới; ID giữ nguyên, revision tăng và client nhận ACK.

## 8. Giới hạn

Chưa có TLS, peer authentication, chống giả mạo node, split-brain, network
partition, nhiều node lỗi đồng thời, hàng đợi offline sau khi client thoát,
quorum hoặc phục hồi khi toàn bộ replica bị mất. Docker demo cần Linux Engine
đang chạy; kiểm thử cục bộ dùng vòng nhỏ để giảm thời gian.

## 9. Câu hỏi ôn tập

RPC client stub và server stub phải sinh từ cùng đặc tả để thống nhất tên thủ
tục, thứ tự và kiểu tham số, kiểu kết quả và wire format. Nếu không, hai bên có
thể truyền được byte nhưng giải mã sai hoặc gọi sai hàm.

MOM phù hợp khi sender và receiver không cần chạy đồng thời, receiver tạm offline
hoặc tải đến theo burst. Queue tách thời điểm gửi khỏi xử lý nhưng cần xử lý
duplicate, retry, thứ tự và giới hạn chờ.

Chord đạt lookup `O(log N)` kỳ vọng vì finger table tạo các bước nhảy tăng theo
lũy thừa hai; mỗi hop loại bỏ một phần lớn khoảng tìm kiếm. Successor đúng vẫn là
điều kiện nền tảng để lookup kết thúc đúng.
