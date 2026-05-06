# Classification Rules

Rule được đặt trong `spark/configs/rules/classification.yaml`.

## Nhóm chính

- `learning`: `event_source=browser` và `event_type` thuộc nhóm học tập/nội dung:
  - `video`, `seq`
  - `textbook.pdf`, `textbook.book`, `pdfbook`, `book_reader`
  - navigation gần nội dung học như `link_clicked`, `page_close`, `jump_to`, `courseware`
- `performance`: event làm bài/điểm số:
  - `edx.grades.problem.submitted`, `problem_check`, `problem_graded`
  - event chứa `quiz`, `grade`, `progress`, `score`
- `system`: event hệ thống/truy cập:
  - proctoring/exam runtime (`proctoring`, `proctored_exam`, `special_exam`, `timed.attempt`)
  - authentication/session (`auth`, `login`, `logout`, `session`)
  - truy cập mức hệ thống như `/dashboard`
- `unknown`: fallback khi không match các rule trên.

## Nguyên tắc

- Mọi rule là config-driven.
- Code classifier chỉ compile rule thành Spark expression.
- Có thể thêm rule mới mà không đổi logic lõi.
