# Schema Strategy

## Raw Strategy

- Kafka payload giữ nguyên JSON line từ source tracking log.
- Không ép schema cứng ở ingest stage để tránh mất dữ liệu do drift.

## Bronze Strategy

- Bronze giữ `value_raw` string + metadata.
- Parse tối thiểu để đánh dấu `parse_status`:
  - `ok`
  - `invalid_json`
  - `missing_required`

## Silver Strategy

- Parse `value_raw` bằng schema permissive với tất cả field nullable.
- `event` có thể là object hoặc JSON string, normalize qua parse hai bước.
- Classifier đọc rule từ config, không hard-code event list trong code.

## Gold Strategy

- Gold chỉ dùng cột đã chuẩn hóa từ silver.
- Feature schema có version để backward-compatible khi thêm metrics mới.
