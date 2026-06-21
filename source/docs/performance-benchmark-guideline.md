# Performance Benchmark Guideline

Tài liệu này hướng dẫn cách đo hiệu năng hệ thống hiện tại theo cách có thể lặp lại, so sánh được, và đủ rõ để chỉ ra bottleneck.

Mục tiêu là trả lời 3 câu hỏi:

1. Hệ thống hiện tại đọc/ghi nhanh đến đâu?
2. Bottleneck nằm ở Kafka, Spark transform, hay Delta write/read?
3. Partitioning, compaction, và fan-out kiến trúc hiện tại có thực sự đáng giá không?

---

## 1. Phạm vi benchmark

Đo theo đúng luồng dữ liệu của repo:

- `BK_activity_logs_unzipped` -> `tracking_log_replayer`
- `tracking_log_replayer` -> Kafka `mooc.raw.events`
- Kafka -> Spark Bronze -> Delta `bronze.mooc_events_raw`
- Bronze -> Spark Silver -> nhiều Silver tables
- Silver -> Gold feature tables
- Delta read/query side, nếu cần, qua Spark batch hoặc Trino

Các điểm chạm chính trong code:

- [kafka/src/producers/tracking_log_replayer.py](/home/cuong/Desktop/DATN/source/kafka/src/producers/tracking_log_replayer.py)
- [spark/pipelines/bronze/ingest_pipeline.py](/home/cuong/Desktop/DATN/source/spark/pipelines/bronze/ingest_pipeline.py)
- [apps/spark/run_silver.py](/home/cuong/Desktop/DATN/source/apps/spark/run_silver.py)
- [spark/pipelines/gold/aggregation_pipeline.py](/home/cuong/Desktop/DATN/source/spark/pipelines/gold/aggregation_pipeline.py)
- [spark/pipelines/maintenance/delta_maintenance_pipeline.py](/home/cuong/Desktop/DATN/source/spark/pipelines/maintenance/delta_maintenance_pipeline.py)

---

## 2. Nguyên tắc benchmark

- Giữ dataset cố định.
- Giữ cấu hình Spark cố định.
- Chạy ít nhất 3 lần mỗi case.
- Ghi cả `warm-up` và `steady-state`.
- Đo theo layer, không chỉ đo end-to-end.
- Không trộn benchmark đo throughput với benchmark đo query latency.

Nếu chỉ đo một con số tổng, bạn sẽ không biết nghẽn nằm ở đâu.

---

## 3. Benchmark matrix nên chạy

### Matrix A: Ingest throughput

Mục tiêu: đo tốc độ từ raw file đến Kafka và đến Bronze Delta.

| Scenario | Input | Biến số chính | Output cần ghi |
|---|---|---|---|
| A1 | 100k events | replay speed `1x` | rows/sec, latency, DLQ rate |
| A2 | 1M events | replay speed `1x` | rows/sec, files written, batch duration |
| A3 | 1M events | replay speed `2x` | backpressure, lag, checkpoint growth |
| A4 | 1M events | replay speed `5x` | stability, failure rate, commit time |
| A5 | 1M events | different batch sizes | micro-batch duration, file count |

Đo gì:

- `sent_raw`, `sent_dlq`, `sent_raw_anonymous`
- Kafka lag
- Spark `inputRowsPerSecond`
- Spark `processedRowsPerSecond`
- micro-batch duration
- số file Delta sinh ra
- kích thước checkpoint

### Matrix B: Bronze write efficiency

Mục tiêu: đo chi phí ghi Delta ở Bronze.

| Scenario | Partitioning | Mục đích |
|---|---|---|
| B1 | `ingest_date`, `ingest_hour` | baseline hiện tại |
| B2 | không partition | xem chi phí scan/write thay đổi thế nào |
| B3 | partition khác | kiểm tra sensitivity với partition strategy |
| B4 | before/after `OPTIMIZE` | đo tác dụng compact file nhỏ |

Đo gì:

- thời gian sink commit
- số file sinh ra mỗi batch
- average file size
- bytes written
- số partition bị chạm
- query latency cho đọc lại Bronze

### Matrix C: Silver fan-out overhead

Mục tiêu: đo việc một input Bronze sinh ra nhiều Silver sinks có làm tốn compute quá nhiều không.

| Scenario | Case | Mục đích |
|---|---|---|
| C1 | chỉ `learning` | baseline 1 sink |
| C2 | `learning + performance` | đo fan-out nhỏ |
| C3 | tất cả Silver tables | đo fan-out đầy đủ |
| C4 | tắt một nhóm normalizer | xem cost từng bảng |

Đo gì:

- thời gian từ Bronze input đến từng Silver sink
- số record vào từng bảng
- CPU time của driver/executor
- memory peak
- duplicate compute do cùng lineage

### Matrix D: Read/query latency

Mục tiêu: đo Delta đọc nhanh đến đâu khi query đúng partition.

| Scenario | Query pattern | Mục đích |
|---|---|---|
| D1 | filter theo ngày | partition pruning cơ bản |
| D2 | filter theo ngày + course | compound pruning |
| D3 | filter theo `silver_class` | đọc theo domain |
| D4 | query `gold.video_friction_signals` | đọc feature hẹp |
| D5 | before/after `OPTIMIZE` | đo lợi ích compact |

Đo gì:

- query latency p50/p95
- bytes scanned
- số file chạm vào
- rows returned
- CPU time

### Matrix E: Maintenance impact

Mục tiêu: đo `OPTIMIZE` và `VACUUM` có giúp gì cho query/write không.

| Scenario | Action | Mục tiêu |
|---|---|---|
| E1 | không maintenance | baseline |
| E2 | chỉ `OPTIMIZE` | xem read speed thay đổi |
| E3 | `OPTIMIZE + VACUUM` | xem storage footprint thay đổi |
| E4 | maintenance theo partition | đo hiệu quả tập trung |

Đo gì:

- compaction duration
- vacuum duration
- file count before/after
- avg file size before/after
- read latency before/after

---

## 4. Hướng đo chi tiết

### 4.1 Đo replay và ingest

Chạy `tracking_log_replayer` với dataset cố định, ví dụ:

```bash
uv run python -m kafka.src.producers.tracking_log_replayer \
  --input-root ../BK_activity_logs_unzipped \
  --topic mooc.raw.events \
  --speed 1.0 \
  --max-files 0 \
  --max-lines 0
```

Ghi lại:

- số record đã gửi
- số record vào DLQ
- thời gian chạy tổng
- replay speed

Nếu muốn benchmark ổn định hơn, nên giới hạn bằng `--max-files` hoặc `--max-lines` để giữ input giống nhau qua các lần chạy.

### 4.2 Đo Bronze streaming

Bronze hiện đọc Kafka bằng:

- [spark/infrastructure/kafka/reader.py](/home/cuong/Desktop/DATN/source/spark/infrastructure/kafka/reader.py)

và ghi Delta bằng:

- [spark/infrastructure/storage/delta.py](/home/cuong/Desktop/DATN/source/spark/infrastructure/storage/delta.py)

Đây là bộ metric cần lấy:

- `inputRowsPerSecond`
- `processedRowsPerSecond`
- `batchDuration`
- `numInputRows`
- `sink` commit time
- số file được tạo trong path Bronze

Từ Spark UI, xem tab **Structured Streaming** và **Jobs**.

Từ storage, đếm:

- số file `part-*`
- dung lượng trung bình mỗi file
- số file theo partition `ingest_date` / `ingest_hour`

### 4.3 Đo Silver fan-out

Silver transformer đọc một Bronze stream rồi tạo nhiều sink:

- learning
- performance
- exam
- video
- navigation
- pdf
- system
- unknown

Điểm cần đo là:

- tổng thời gian xử lý của toàn query
- throughput từng sink
- sink nào tạo nhiều file nhỏ nhất
- sink nào có record count lệch nhiều nhất

Vì Silver đang fan-out từ cùng một lineage, bạn nên đo:

- tổng CPU toàn job
- thời gian tới từng table
- tổng bytes written của toàn bộ Silver

Đây là cách thấy chi phí kiến trúc hiện tại rõ nhất.

### 4.4 Đo Gold

Gold hiện chỉ đọc `silver.video_interactions`, bucket theo thời gian, rồi aggregate.

Điều cần đo:

- latency cho từng batch
- số record vào `gold.video_friction_signals`
- số file output
- thời gian aggregate theo `bucket_seconds`

Nếu Gold chạy nhanh hơn nhiều so với Bronze/Silver, đó là dấu hiệu good design: compute hẹp hơn vì đọc đúng lát cắt dữ liệu.

### 4.5 Đo query/read side

Nếu dùng Spark batch hoặc Trino:

- query theo `event_date`
- query theo `event_date + course_id`
- query theo domain table thay vì raw

Thông số cần ghi:

- wall-clock query time
- scan bytes
- file count scanned
- partition pruning hiệu quả hay không
- row count result

---

## 5. Thông số nên lưu mỗi lần chạy

Một record benchmark tối thiểu nên có:

- `run_id`
- `timestamp`
- `dataset_name`
- `input_rows`
- `input_bytes`
- `replay_speed`
- `layer` = `replayer | bronze | silver | gold | query | maintenance`
- `scenario_id`
- `runtime_seconds`
- `rows_per_second`
- `bytes_written`
- `bytes_read`
- `file_count_before`
- `file_count_after`
- `avg_file_size`
- `p50_latency`
- `p95_latency`
- `errors`
- `dlq_count`

Nếu có Spark UI metrics thì thêm:

- `inputRowsPerSecond`
- `processedRowsPerSecond`
- `batchDuration`
- `numActiveExecutors`
- `shuffleReadBytes`
- `shuffleWriteBytes`

---

## 6. Cách diễn giải kết quả

### Nếu Bronze chậm

Khả năng cao là:

- Kafka consume chậm
- Delta sink tạo quá nhiều file nhỏ
- partition strategy chưa hợp lý

### Nếu Silver chậm

Khả năng cao là:

- fan-out quá nhiều sink
- classification hoặc `from_json` tốn CPU
- cùng một input bị xử lý lặp qua nhiều nhánh

### Nếu query chậm

Khả năng cao là:

- partition chưa đúng
- file nhỏ quá nhiều
- cần `OPTIMIZE`
- query không tận dụng pruning

### Nếu maintenance quá nặng

Khả năng cao là:

- file nhỏ sinh ra quá nhiều từ ingest
- cần batch lớn hơn
- cần lịch compaction hợp lý hơn

---

## 7. Nguyên tắc làm việc

- Đo theo từng lớp, không chỉ đo end-to-end.
- Giữ dataset, cấu hình, và cách chạy cố định giữa các lần test.
- Chạy ít nhất 3 lần cho mỗi scenario.
- Ghi rõ input size, replay speed, partitioning, và trạng thái tối ưu hóa.
- Chỉ thay đổi một biến tại một thời điểm.

---

## 8. Kết luận thực dụng

Nếu chỉ chọn 3 benchmark để bắt đầu, hãy chạy:

1. `Bronze ingest throughput`
2. `Silver fan-out overhead`
3. `Read latency before/after OPTIMIZE`

Ba benchmark này đủ để trả lời:

- hệ thống có ingest nhanh không
- kiến trúc phân tầng hiện tại có đang tạo thêm overhead đáng kể không
- Delta hiện tại có đang bị file nhỏ hoặc partition kém không
