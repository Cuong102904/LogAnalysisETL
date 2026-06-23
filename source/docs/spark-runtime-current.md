# Spark Runtime Current

Tài liệu này mô tả cấu hình Spark hiện tại của repo, đặc biệt là luồng `bronze-stream` đang đọc dữ liệu từ Kafka và ghi vào MinIO/Delta.

## 1. Phần đã sửa gần đây

- Bronze stream trước đây dùng `batch_df.collect()` trong `foreachBatch`, tức là kéo toàn bộ batch về driver một lần. Với batch lớn, driver dễ bị áp lực và query có thể chết giữa chừng.
- Hiện tại bronze đã đổi sang `batch_df.toLocalIterator()` và ghi Delta theo từng chunk nhỏ, để giảm tải driver.
- Bronze cũng có throttle ở phía Kafka bằng `maxOffsetsPerTrigger`, nên có thể giới hạn lượng record mỗi micro-batch.
- `docker-compose.yaml` hiện đặt mặc định `BRONZE_MAX_OFFSETS_PER_TRIGGER=1000` và `BRONZE_TRIGGER_PROCESSING_TIME=1 second` để test throughput cao hơn nhưng vẫn giữ nhịp micro-batch ngắn.

Các file liên quan:
- [apps/spark/run_bronze.py](/home/cuong/Desktop/DATN/source/apps/spark/run_bronze.py:1)
- [apps/spark/common.py](/home/cuong/Desktop/DATN/source/apps/spark/common.py:1)
- [src/learnlake/connectors/kafka.py](/home/cuong/Desktop/DATN/source/src/learnlake/connectors/kafka.py:1)
- [src/learnlake/runtime/spark.py](/home/cuong/Desktop/DATN/source/src/learnlake/runtime/spark.py:1)

## 2. Spark hiện tại đang chạy như thế nào

### bronze-stream

Service `bronze-stream` trong `docker-compose.yaml` chạy:

```bash
spark-submit --master spark://spark-master:7077 --deploy-mode client \
  /opt/project/apps/spark/run_bronze.py --source daotao_ai --stream
```

Luồng xử lý:

1. `apps/spark/common.py:load_profile()` đọc source profile của `daotao_ai`.
2. Nếu có env Kafka, profile input được override sang mode `kafka`.
3. `read_kafka_stream()` tạo `readStream` từ Kafka topic `learnlake.daotao.raw`.
4. Stream lấy các cột:
   - `raw_json`
   - `kafka_topic`
   - `kafka_partition`
   - `kafka_offset`
5. `foreachBatch()` parse JSON, build bronze envelope, rồi ghi vào Delta trên MinIO.
6. Checkpoint Kafka offset nằm trên MinIO, nên Spark giữ được tiến trình đọc giữa các lần restart.

### SparkSession

`src/learnlake/runtime/spark.py` đang tạo SparkSession với:

- `spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension`
- `spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog`
- `spark.pyspark.driver.python = sys.executable`
- cấu hình S3A/MinIO lấy từ env theo thứ tự ưu tiên:
  - endpoint: `S3A_ENDPOINT`, `AWS_ENDPOINT_URL_S3`, `OBJECT_STORE_ENDPOINT`, `MINIO_ENDPOINT`
  - access key: `AWS_ACCESS_KEY_ID`, `OBJECT_STORE_ACCESS_KEY`, `MINIO_ACCESS_KEY`, `MINIO_ROOT_USER`
  - secret key: `AWS_SECRET_ACCESS_KEY`, `OBJECT_STORE_SECRET_KEY`, `MINIO_SECRET_KEY`, `MINIO_ROOT_PASSWORD`
  - path style: `S3A_PATH_STYLE_ACCESS`, `OBJECT_STORE_PATH_STYLE_ACCESS`, `MINIO_PATH_STYLE_ACCESS`
  - ssl: `S3A_SSL_ENABLED`, `OBJECT_STORE_SSL_ENABLED`, `MINIO_SSL_ENABLED`

## 3. Các config đang ảnh hưởng trực tiếp

### Config của source profile

`src/learnlake/contracts/source.py` định nghĩa các field chính:

- `input.mode`: `kafka`, `file`, hoặc `fixture`
- `input.topic`
- `input.path`
- `input.bootstrap_servers`
- `input.starting_offsets`
- `input.max_offsets_per_trigger`
- `input.trigger_processing_time`
- `input.event_time_field`
- `input.event_type_field`
- `bronze.path`
- `bronze.checkpoint`
- `bronze.partition_by`
- `silver.event_index`
- `silver.targets`
- `silver.invalid`

### Env hiện dùng trong compose

Các env quan trọng cho Spark runtime:

- `SPARK_MASTER_URL`
- `SPARK_DRIVER_MEMORY`
- `SPARK_EXECUTOR_MEMORY`
- `KAFKA_BOOTSTRAP_SERVERS`
- `BRONZE_INPUT_TOPIC`
- `KAFKA_STARTING_OFFSETS`
- `KAFKA_MAX_OFFSETS_PER_TRIGGER`
- `KAFKA_TRIGGER_PROCESSING_TIME`
- `BRONZE_TABLE_PATH`
- `BRONZE_CHECKPOINT_PATH`
- `S3A_ENDPOINT` / `AWS_ENDPOINT_URL_S3` / `OBJECT_STORE_ENDPOINT` / `MINIO_ENDPOINT`
- `AWS_ACCESS_KEY_ID` / `OBJECT_STORE_ACCESS_KEY` / `MINIO_ACCESS_KEY`
- `AWS_SECRET_ACCESS_KEY` / `OBJECT_STORE_SECRET_KEY` / `MINIO_SECRET_KEY`

Trong compose hiện tại:

- `BRONZE_TABLE_PATH` mặc định là `s3a://lakehouse/learnlake/bronze/bronze_events`
- `BRONZE_CHECKPOINT_PATH` mặc định là `s3a://platform/learnlake/checkpoints/bronze/daotao_ai`
- `KAFKA_MAX_OFFSETS_PER_TRIGGER` mặc định là `1000`
- `KAFKA_TRIGGER_PROCESSING_TIME` mặc định là `1 second`
- `SPARK_DRIVER_MEMORY` mặc định là `1024m`
- `mem_limit` của bronze-stream là `1536m`

## 4. Bronze đang đọc và ghi dữ liệu ra sao

### Đọc từ Kafka

`read_kafka_stream()` chỉ làm việc với Kafka Structured Streaming source. Nó luôn set:

- `kafka.bootstrap.servers`
- `subscribe`
- `startingOffsets`

Nếu có `maxOffsetsPerTrigger`, nó sẽ gắn thêm option này để giới hạn số message mỗi micro-batch.

### Parse và envelope

Mỗi record Kafka được:

- `json.loads(row.raw_json)`
- đưa qua `build_bronze_envelope()`
- tạo các trường metadata:
  - `event_id`
  - `source_id`
  - `source_type`
  - `event_time_raw`
  - `event_time`
  - `ingestion_time`
  - `raw_payload`
  - `kafka_topic`
  - `kafka_partition`
  - `kafka_offset`
  - `processing_date`

### Ghi vào MinIO/Delta

Bronze ghi bằng `df.write.format("delta").mode("append").save(output_path)`.

Ý nghĩa thực tế:

- MinIO chỉ là object storage backend.
- Delta table là lớp logic bên trên MinIO, với `_delta_log` giữ transaction history.
- Checkpoint của Structured Streaming cũng nằm trên MinIO để Spark resume đúng offset đã commit.

### Kafka offset và Spark batch

Đây là chỗ dễ nhầm nhất:

- Kafka gán **1 offset cho 1 record trong 1 partition**.
- Spark không lưu “1 offset của Spark” theo kiểu riêng. Spark lưu **range offset** mà mỗi micro-batch đã đọc ở từng Kafka partition.
- Khi batch xong và sink ghi thành công, Spark mới commit checkpoint.

Ví dụ đơn giản:

- Partition 0: đọc từ offset `100` đến `120`
- Partition 1: đọc từ offset `80` đến `90`
- Partition 2: không có data mới

Spark sẽ xem đó là 1 micro-batch hợp lệ và ghi lại progress vào checkpoint dưới dạng các offset range này. Lần restart sau, Spark đọc tiếp từ offset cuối đã commit, không đọc lại phần đã commit.

Điều cần nhớ:

- `maxOffsetsPerTrigger=1000` nghĩa là mỗi trigger được phép lấy nhiều record hơn.
- `trigger(processingTime='1 second')` chỉ quyết định nhịp Spark thử chạy batch.
- Hai cái này không thay thế nhau.

Nếu muốn đọc lại chi tiết hơn:
- Kafka offset là định danh của record trong partition.
- Spark checkpoint là progress đã xử lý của query.
- Delta `_delta_log` là lịch sử commit của bảng output.

## 5. Kafka storage, replication, và lag

### Kafka đang lưu bao nhiêu

Về lý thuyết:

- Kafka lưu dữ liệu theo topic và partition, nhưng dữ liệu có thể được nhân bản qua nhiều broker.
- Dung lượng đĩa bạn thấy trên cluster là **dung lượng vật lý đã nhân bản**, không phải dữ liệu unique.
- Vì vậy số GB trên broker thường lớn hơn khối lượng dữ liệu thật của bài toán.

Trong bài này, giá trị thật đang dùng là:

- Topic source: `learnlake.daotao.raw`
- Replication factor: `3`
- Min in-sync replicas: `2`
- Checkpoint bronze: `s3a://platform/learnlake/checkpoints/bronze/daotao_ai`
- Checkpoint silver: `s3a://platform/learnlake/checkpoints/silver/daotao_ai_v2`
- Bronze throttle: `KAFKA_MAX_OFFSETS_PER_TRIGGER=1000`
- Bronze trigger: `KAFKA_TRIGGER_PROCESSING_TIME=1 second`

Vì sao chọn như vậy:

- `replication factor = 3` và `min ISR = 2` là cấu hình cân bằng giữa an toàn dữ liệu và khả năng chịu lỗi. Cluster có 3 broker thì 3 bản sao là mức hợp lý để không mất dữ liệu khi rớt 1 broker.
- Checkpoint nằm trên MinIO để Spark có thể resume chính xác sau restart. Không có checkpoint thì stream rất dễ đọc lại hoặc lệch tiến độ.
- `maxOffsetsPerTrigger=1000` là mức test throughput cao hơn. Nó cho mỗi micro-batch lấy nhiều record hơn, để quan sát xem pipeline có chịu được tải lớn hơn hay không.
- `trigger(processingTime='1 second')` làm stream “nhịp ngắn” để dễ quan sát và phản hồi nhanh khi test.

Điểm đổi lại:

- Cấu hình này ổn cho debug và ổn định, nhưng không tối ưu throughput.
- Nếu dữ liệu nhiều, micro-batch sẽ tăng độ trễ tổng thể vì chỉ nạp rất ít record mỗi lần.
- Nếu processing của một batch lâu hơn 1 giây, Spark không chạy chồng batch cùng query; nó sẽ xếp hàng và chạy trễ hơn nhịp trigger.

Hướng tối ưu thực tế:

- Khi pipeline đã ổn, tăng `KAFKA_MAX_OFFSETS_PER_TRIGGER` lên mức cao hơn, ví dụ 10, 100, hoặc 1000 tùy CPU/memory và tốc độ write.
- Nới `KAFKA_TRIGGER_PROCESSING_TIME` lên vài giây nếu không cần quan sát realtime quá sát.
- Giữ checkpoint trên MinIO, vì đây là phần cần thiết cho correctness.
- Nếu muốn giảm storage cho môi trường dev, có thể hạ replication factor xuống 1, nhưng đổi lại mất khả năng chịu lỗi và không còn phản ánh cấu hình gần production.
- Nếu silver vẫn chậm, tối ưu tiếp ở write path và số target tables, không chỉ ở Kafka throttle.

### Cách hiểu “lag”

Có 3 lớp trạng thái khác nhau:

- Kafka offset: vị trí record trong từng partition.
- Spark checkpoint: tiến độ query đã xử lý tới đâu.
- Delta log: commit history của bảng output.

Với Bronze, progress thực tế của job là offset range trong checkpoint, không phải consumer group lag kiểu truyền thống.

Với Silver, `offsets/` và `commits/` trong checkpoint cho biết batch nào đã đọc và batch nào đã commit.

### Dấu hiệu thực tế khi debug

- Nếu `offsets/` tăng mà `commits/` chưa tăng, batch đang xử lý hoặc commit chưa xong.
- Nếu Kafka topic rất lớn nhưng checkpoint bronze chỉ tăng chậm, nghẽn nằm ở bronze source/sink hoặc Spark runtime.
- Nếu bronze đã chạy tiếp nhưng silver `offsets/` và `commits/` đứng yên, nghẽn nằm ở silver stream path hoặc silver output write.

## 6. Silver hiện dùng Spark như thế nào

Silver stream không đọc Kafka trực tiếp. Nó đọc Bronze Delta từ MinIO, sau đó:

- decode `raw_payload`
- áp mapping/routing theo source pack
- normalize ra các target tables
- ghi từng target table xuống Delta path riêng
- xử lý batch bằng `toLocalIterator()` và flush theo chunk nhỏ để tránh kéo toàn bộ batch về driver một lần
- ghi invalid records vào `silver.invalid` nếu có cấu hình

Silver cũng đang dùng cùng một Spark runtime adapter, nên các cấu hình S3A/Delta/driver memory vẫn đi qua `build_spark()`.

## 7. Ghi nhớ khi debug

- Nếu bronze chết trước khi log `learnlake bronze batch_id=... wrote ... records`, thường là lỗi ở path parse/write trong `foreachBatch`.
- Nếu checkpoint `offsets/` tăng nhưng `commits/` không tăng, batch chưa commit xong.
- Nếu Delta log trong MinIO tăng mà checkpoint chưa nhích, nghĩa là job còn đang xử lý hoặc bị gián đoạn trước bước commit streaming.
- Với job nặng, ưu tiên giảm `KAFKA_MAX_OFFSETS_PER_TRIGGER` trước khi tăng memory.

## 8. Kết luận ngắn

Hiện tại Spark của repo này đang chạy theo mô hình:

- Bronze: Kafka -> Spark Structured Streaming -> Delta trên MinIO
- Silver: Bronze Delta -> normalize -> Delta trên MinIO
- Runtime chung: `build_spark()` + env override + Delta/S3A

Phần ổn định nhất để chỉnh hiện giờ là `bronze-stream`, vì nó là điểm đầu vào của toàn bộ pipeline.
