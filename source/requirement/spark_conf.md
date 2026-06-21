Bạn hãy sửa codebase để chuyển hệ thống sang final design dùng Spark Standalone trong Docker.

Context hiện trạng:
- spark/infrastructure/spark/session.py đang hard-code SparkSession.builder.master("local[*]").
- airflow/tasks/bronze/runtime_checks.py cũng hard-code SparkSession.builder.master("local[*]") cho maintenance.
- spark/conf/spark-defaults.conf có spark.master=spark://spark-master:7077 nhưng hiện bị code override.
- Hiện có Spark jobs:
  - bronze_ingestor: đọc Kafka -> ghi Delta Bronze trên MinIO
  - silver_transformer: đọc Delta Bronze -> ghi nhiều Delta Silver
  - gold_aggregator: đọc Delta Silver -> ghi Delta Gold
- Delta tables dùng s3a:// trên MinIO.
- Checkpoints nằm trên s3a://checkpoints/...
- Airflow hiện chủ yếu health check và Bronze optimize/vacuum; chưa thật sự submit Spark jobs chính.

Mục tiêu final:
1. Pipeline chính phải chạy trên Spark Standalone, không chạy local[*].
2. Spark master URL lấy từ env/config, default trong Docker là spark://spark-master:7077.
3. Docker Compose final phải có:
   - spark-master
   - spark-worker(s)
   - spark-history-server
   - Kafka
   - MinIO
   - Airflow
   - service chạy bronze/silver/gold streaming jobs bằng spark-submit
4. Bronze/Silver/Gold streaming jobs phải được submit bằng spark-submit vào Spark Standalone.
5. Không chạy production bằng python -m apps.* nếu cách đó bypass Spark Standalone.
6. Airflow chỉ dùng cho maintenance/health.
7. Delta OPTIMIZE/VACUUM phải chạy bằng Spark job submit vào Standalone, không tạo SparkSession local trong Airflow.
8. MinIO/S3A/Delta/Kafka config phải tập trung qua env/config file, hạn chế hard-code.
9. Mỗi streaming job phải có checkpoint riêng, queryName rõ, output path rõ.
10. Cập nhật README/runbook để chạy full stack.

Yêu cầu thực hiện:

Bước 1 — Phân tích trước khi sửa
- Liệt kê các file sẽ sửa.
- Giải thích vì sao cần sửa từng file.
- Chỉ ra các chỗ đang hard-code:
  - local[*]
  - localhost
  - 127.0.0.1
  - spark://
  - MinIO/Kafka endpoint
- Chưa patch code cho đến khi đưa xong plan.

Bước 2 — Sửa SparkSession
- Sửa spark/infrastructure/spark/session.py để bỏ .master("local[*]").
- Đọc SPARK_MASTER_URL từ env.
- Nếu SPARK_MASTER_URL không set thì để spark-submit/spark-defaults quyết định.
- Không để pipeline chính ép local mode.

Bước 3 — Chuẩn hóa Spark defaults
- Cập nhật spark/conf/spark-defaults.conf cho:
  - Spark Standalone master
  - Delta Lake extension/catalog
  - S3A MinIO config
  - Spark event logs
  - serializer/AQE nếu phù hợp
- Đảm bảo không conflict với config trong code.

Bước 4 — Chuẩn hóa config Bronze/Silver/Gold
- Kafka topics, Kafka bootstrap servers, MinIO paths, Delta paths, checkpoint paths phải lấy từ env/config.
- Mỗi streaming query phải có:
  - checkpointLocation riêng
  - queryName rõ
  - output path rõ
  - appName rõ
- Không để nhiều job dùng chung checkpoint.

Bước 5 — Tạo Spark maintenance job riêng
- Tạo app riêng, ví dụ spark/apps/maintenance/delta_maintenance.py.
- App này nhận:
  - table path
  - optimize flag
  - vacuum flag
  - vacuum retention hours
- App này tạo SparkSession bình thường thông qua build_spark.
- Không để Airflow tự tạo SparkSession local[*] nữa.

Bước 6 — Sửa Airflow maintenance
- Sửa DAG bronze_table_maintenance để gọi spark-submit hoặc SparkSubmitOperator.
- Lệnh submit phải trỏ tới spark://spark-master:7077.
- Airflow chỉ schedule maintenance/health.
- Không dùng Airflow để giữ long-running streaming jobs, trừ khi có lý do rõ.
- Nếu cần, tạo table registry để sau này maintenance được bronze/silver/gold.

Bước 7 — Docker hóa Spark Standalone final
- Tạo hoặc cập nhật docker-compose final gồm:
  - spark-master
  - spark-worker-1
  - optional spark-worker-2
  - spark-history-server
  - Kafka
  - MinIO
  - minio-init tạo bucket `lakehouse` và `platform`
  - Airflow webserver/scheduler
  - bronze-stream service
  - optional silver-stream/gold-stream services
- Streaming services phải chạy bằng spark-submit vào spark://spark-master:7077.
- Trong Docker network, dùng service names:
  - spark-master
  - minio
  - broker1 hoặc kafka service name tương ứng
- Không dùng localhost trong container configs.

Bước 8 — Chuẩn hóa Docker image
- Spark image phải chứa đủ:
  - Spark
  - PySpark
  - Delta Lake dependency
  - hadoop-aws
  - AWS SDK dependency
  - Python requirements
  - source code của spark apps
  - spark-defaults.conf
- Nếu Airflow dùng spark-submit thì Airflow image cũng phải có spark-submit hoặc gọi sang submitter container rõ ràng.
- Kiểm tra compatibility version:
  - Spark version
  - Hadoop version
  - Delta version
  - hadoop-aws version
  - AWS SDK version
  - Python version

Bước 9 — Observability
- Bật spark.eventLog.enabled=true.
- Lưu event logs vào s3a://platform/spark-events/...
- Thêm Spark History Server ở port 18080.
- Spark Master UI ở port 8081.
- README phải ghi cách xem Spark Master UI, app UI, History Server, Airflow UI, MinIO UI.

Bước 10 — Smoke tests / validation
Thêm hướng dẫn hoặc script test cho:
1. Spark master lên và workers registered.
2. spark-submit smoke job chạy trên Standalone.
3. Spark đọc/ghi được s3a:// MinIO.
4. Spark đọc/ghi được Delta table trên MinIO.
5. Kafka produce sample event -> Bronze Delta có dữ liệu.
6. Checkpoint Bronze được tạo trên MinIO.
7. Airflow maintenance DAG chạy optimize/vacuum thành công.
8. Spark app xuất hiện trên Master UI hoặc History Server.

Bước 11 — Documentation
Cập nhật README/runbook với:
- Architecture diagram final.
- Cách build images.
- Cách start full stack.
- Cách submit/restart streaming jobs.
- Cách trigger Airflow maintenance.
- Cách xem UI/logs.
- Bảng mapping:
  - Kafka topic -> Spark job -> Delta table -> checkpoint path.
- Cảnh báo không xóa checkpoint khi restart streaming.

Acceptance criteria:
- Không còn hard-code local[*] trong pipeline chính.
- Airflow maintenance không tạo SparkSession local[*].
- Spark jobs chạy qua spark-submit vào spark://spark-master:7077.
- Workers register với Spark Master.
- Bronze streaming đọc Kafka và ghi Delta trên MinIO.
- Checkpoint được ghi vào s3a://checkpoints/...
- Delta maintenance chạy qua Spark Standalone.
- Không dùng localhost sai trong Docker network.
- README mô tả đúng final architecture.
- Spark Master UI và History Server hoạt động.

Lưu ý không được làm sai:
- Đừng chỉ sửa spark-defaults.conf mà quên session.py đang override local[*].
- Đừng để Airflow runtime_checks.py tiếp tục tạo SparkSession local[*].
- Đừng chạy hai streaming jobs cùng checkpoint.
- Đừng hard-code MinIO/Kafka endpoint trong nhiều nơi.
- Đừng VACUUM retention quá thấp; default nên để an toàn, ví dụ 168 hours, trừ khi config nói khác.
- Đừng xóa checkpoint khi test restart streaming.
- Đừng để docs nói Spark Standalone nhưng code vẫn chạy local.

Hãy bắt đầu bằng việc xuất implementation plan và danh sách file cần sửa. Sau đó mới thực hiện patch.
