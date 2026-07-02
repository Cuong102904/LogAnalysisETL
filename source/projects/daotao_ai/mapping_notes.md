# Daotao Silver Notes

- Silver không còn dùng mapping file canonical kiểu cũ.
- Source pack hiện khai báo:
  - `routing.yaml`
  - `parsers.yaml`
- Routing chọn `parser_family`, `event_group`, `event_subgroup`, target tables, và parser function.
- Parser functions chạy bằng Spark DataFrame expressions trong runtime mới.
- Unknown event chỉ đi `silver_unknown_events`.
