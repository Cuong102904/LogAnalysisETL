
# 📔 Tài liệu Đặc tả Cấu trúc Đa lớp Bộ dữ liệu Tracking Logs (MOOC LMS)

Tài liệu này phân tích chi tiết các lớp dữ liệu (Layers) trong bộ log. Dữ liệu được thiết kế theo dạng **Nested JSON**, nghĩa là thông tin chi tiết được bao bọc trong các vật chứa (containers) như `context` và `event`.

---

## 🏗️ 1. Lớp 1: Các Trường Gốc (Root Fields)
Các trường này xuất hiện ở hầu hết các dòng log, đóng vai trò là "khung xương" định danh.

| Trường | Xuất hiện | Ý nghĩa |
| :--- | :--- | :--- |
| `username` | Luôn có | Mã số sinh viên (VD: `202510766`). |
| `time` | Luôn có | Thời điểm log (UTC). |
| `event_source` | Luôn có | `browser` (từ Client) hoặc `server` (từ Backend). |
| `event_type` | Luôn có | Loại sự kiện (VD: `play_video`, `problem_check`). |
| `ip` | Luôn có | Địa chỉ IP của người dùng. |
| `session` | Luôn có | 32 ký tự định danh phiên làm việc. |
| `agent` | Hầu hết | Thông tin trình duyệt/Hệ điều hành. |
| `page` | Browser | URL trang web đang thao tác (Server log thường để `null`). |

---

## 📂 2. Lớp 2: Ngữ cảnh Dữ liệu (`context`)
Trường `context` chứa thông tin về môi trường học tập. Đây là nơi chứa các ID quan trọng.

| Trường con | Ý nghĩa |
| :--- | :--- |
| `course_id` | ID duy nhất của môn học (VD: `course-v1:SoICT+NLS01+2025-1`). |
| `org_id` | Mã tổ chức quản lý (VD: `SoICT`, `SoDITEC`). |
| `user_id` | ID số nội bộ của người dùng trong hệ thống. |
| `path` | Đường dẫn API hoặc tài nguyên đang truy cập. |
| **`module`** | (Chỉ có ở Server Log) Chứa thông tin về thành phần bài học: |
| &nbsp;&nbsp; └─ `display_name` | **Tên hiển thị của bài tập/video** (VD: "Câu hỏi tổng hợp số 7"). |
| &nbsp;&nbsp; └─ `usage_key` | ID định danh thành phần (XBlock ID). |

---

## ⚡ 3. Lớp 3: Nội dung chi tiết Sự kiện (`event`)
Trường `event` có cấu trúc thay đổi hoàn toàn dựa trên `event_type`:

### A. Đối với nộp bài thi (`problem_check`) - Server Source
Chứa toàn bộ trạng thái bài làm của sinh viên tại thời điểm đó.
- `problem_id`: ID của câu hỏi.
- `answers`: Danh sách các lựa chọn sinh viên đã tích (VD: `choice_0`, `choice_2`).
- `submission`: Chi tiết nội dung câu hỏi và câu trả lời đã map.
- `success`: Kết quả (`correct` hoặc `incorrect`).
- `attempts`: Số lần sinh viên đã thử làm câu này.
- `grade`/`max_grade`: Điểm số đạt được trên thang điểm của câu đó.

### B. Đối với xem video (`play_video`, `pause_video`) - Browser Source
- `id`: ID của trình phát video.
- `code`: Mã Video (thường là ID Youtube/Vimeo).
- `currentTime`: Vị trí thời gian hiện tại của video khi hành động xảy ra.

---

## 📑 4. Phân loại theo "Hồ sơ Log" (Log Profiles)

Để dễ dàng lập trình Parser, bạn có thể phân chia thành 3 hồ sơ chính:

### 1. Hồ sơ Học tập (Learning Profile)
*   **Dấu hiệu**: `event_source: "browser"` & `event_type` chứa "video" hoặc "seq".
*   **Dữ liệu khai thác**: Thời gian học, tốc độ xem, thói quen tua bài.

### 2. Hồ sơ Kết quả (Performance Profile)
*   **Dấu hiệu**: `event_source: "server"` & `event_type: "edx.grades.problem.submitted"`.
*   **Dữ liệu đặc thù**: Có trường `event.weighted_earned` (Điểm thực tế) và `event.weighted_possible` (Điểm tối đa).
*   **Gắn kết**: Dùng `context.module.display_name` để biết tên bài tập tương ứng.

### 3. Hồ sơ Hệ thống (System Profile)
*   **Dấu hiệu**: `event_type` liên quan đến `proctoring`, `heartbeat` hoặc `auth`.
*   **Dữ liệu khai thác**: Trạng thái giám sát thi, lỗi đăng nhập, thời gian phản hồi server.

---


## 🛠️ 5. Ví dụ Thực tế từng loại Log (Raw Logs)

Dưới đây là các dòng log thực tế được trích xuất từ bộ dữ liệu. Lưu ý sự khác biệt về cấu trúc trường `event` và sự hiện diện của các IDs.

### 🎥 A. Browser Log: Hành động xem Video
Log sinh ra từ trình duyệt khi sinh viên tương tác với nội dung.
- **Đặc trưng**: `event_source: "browser"`, `event` là một **JSON Object** trực tiếp.
```json
{
    "name": "play_video",
    "event_source": "browser",
    "username": "20235644",
    "ip": "1.54.208.204",
    "context": {
        "user_id": 63239,
        "course_id": "course-v1:SoICT+IT3080+2023-1",
        "org_id": "SoICT"
    },
    "event": {
        "id": "c8e965e6067349319764938d2747fe46",
        "code": "kpVUmO2xuuA",
        "duration": 225,
        "currentTime": 45.227
    },
    "time": "2025-10-01T04:37:24.844309+00:00",
    "page": "https://soict.daotao.ai/courses/..."
}
```

### 📝 B. Server Log: Khi nộp bài (Problem Check)
Log sinh ra từ Backend khi xử lý yêu cầu nộp bài của sinh viên.
- **Đặc trưng**: `event_source: "server"`, `event` có thể là **JSON Object** chứa chi tiết câu trả lời và kết quả đúng/sai.
- **Metadata giá trị**: Có `context.module.display_name` giúp định danh tên bài tập.
```json
{
  "name": "problem_check",
  "context": {
    "course_id": "course-v1:SoICT+IT4010E+DucTV",
    "course_user_tags": {},
    "user_id": 395707,
    "path": "/courses/course-v1:SoICT+IT4010E+DucTV/xblock/block-v1:SoICT+IT4010E+DucTV+type@problem+block@20258bd5e63508441a98/handler/xmodule_handler/problem_check",
    "org_id": "SoICT",
    "module": {
      "display_name": "Question 5",
      "usage_key": "block-v1:SoICT+IT4010E+DucTV+type@problem+block@20258bd5e63508441a98",
      "original_usage_key": "lib-block-v1:SoICT+LIT4010E+type@problem+block@570f2ba54ea440faaa2e6e355649d298",
      "original_usage_version": "696c776fc8b7938650814107"
    },
    "asides": {}
  },
  "username": "20250072S",
  "session": "ed7b67b0fb0bd65d4b9862856a79660e",
  "ip": "1.55.19.93",
  "agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
  "host": "soict.daotao.ai",
  "referer": "https://soict.daotao.ai/courses/course-v1:SoICT+IT4010E+DucTV/courseware/e4d61082693b4f0c9d7fb92e90491ce7/8330bf08eea9411ca13b84b3023adca6/?child=first",
  "accept_language": "fr,fr-FR;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
  "event": {
    "state": {
      "seed": 6,
      "student_answers": {
        "20258bd5e63508441a98_2_1": ["choice_3"]
      },
      "has_saved_answers": false,
      "correct_map": {
        "20258bd5e63508441a98_2_1": {
          "correctness": "correct",
          "npoints": null,
          "msg": "",
          "hint": "",
          "hintmode": null,
          "queuestate": null,
          "answervariable": null
        }
      },
      "input_state": {
        "20258bd5e63508441a98_2_1": {}
      },
      "done": true,
      "mask_display_name": "Question #5e6350",
      "selected_answer_index": null
    },
    "problem_id": "block-v1:SoICT+IT4010E+DucTV+type@problem+block@20258bd5e63508441a98",
    "answers": {
      "20258bd5e63508441a98_2_1": ["choice_3"]
    },
    "grade": 1,
    "max_grade": 1,
    "correct_map": {
      "20258bd5e63508441a98_2_1": {
        "correctness": "correct",
        "npoints": null,
        "msg": "",
        "hint": "",
        "hintmode": null,
        "queuestate": null,
        "answervariable": null
      }
    },
    "success": "correct",
    "attempts": 3,
    "submission": {
      "20258bd5e63508441a98_2_1": {
        "question": "",
        "answer": [
          "It is difficult to construct two distinct messages \\(m_0, m_1\\) such that \\(H(m_0) = H(m_1)\\)."
        ],
        "response_type": "choiceresponse",
        "input_type": "checkboxgroup",
        "correct": true,
        "variant": 6,
        "group_label": ""
      }
    }
  },
  "time": "2026-01-18T07:17:07.267733+00:00",
  "event_type": "problem_check",
  "event_source": "server",
  "page": "x_module"
}
```

### 🏆 C. Server Log: Ghi nhận điểm số (Grades)
Log chuyên biệt dùng để cập nhật điểm vào sổ điểm (Gradebook).
- **Đặc trưng**: `event_type: "edx.grades.problem.submitted"`.
```json
{
  "name": "edx.grades.problem.submitted",
  "context": {
    "course_id": "course-v1:SoICT+IT4010E+DucTV",
    "course_user_tags": {},
    "user_id": 395707,
    "path": "/courses/course-v1:SoICT+IT4010E+DucTV/xblock/block-v1:SoICT+IT4010E+DucTV+type@problem+block@20258bd5e63508441a98/handler/xmodule_handler/problem_check",
    "org_id": "SoICT",
    "module": {
      "display_name": "Question 5",
      "usage_key": "block-v1:SoICT+IT4010E+DucTV+type@problem+block@20258bd5e63508441a98",
      "original_usage_key": "lib-block-v1:SoICT+LIT4010E+type@problem+block@570f2ba54ea440faaa2e6e355649d298",
      "original_usage_version": "696c776fc8b7938650814107"
    }
  },
  "username": "20250072S",
  "session": "ed7b67b0fb0bd65d4b9862856a79660e",
  "ip": "1.55.19.93",
  "agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
  "host": "soict.daotao.ai",
  "referer": "https://soict.daotao.ai/courses/course-v1:SoICT+IT4010E+DucTV/courseware/e4d61082693b4f0c9d7fb92e90491ce7/8330bf08eea9411ca13b84b3023adca6/?child=first",
  "accept_language": "fr,fr-FR;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
  "event": {
    "user_id": "395707",
    "course_id": "course-v1:SoICT+IT4010E+DucTV",
    "problem_id": "block-v1:SoICT+IT4010E+DucTV+type@problem+block@20258bd5e63508441a98",
    "event_transaction_id": "70346952-ee1a-4f11-90ef-658837cc1c99",
    "event_transaction_type": "edx.grades.problem.submitted",
    "weighted_earned": 1.0,
    "weighted_possible": 1.0
  },
  "time": "2026-01-18T07:17:07.228926+00:00",
  "event_type": "edx.grades.problem.submitted",
  "event_source": "server",
  "page": null
}
```


---
*Ghi chú: Trong dữ liệu thô, trường `event` của một số log Server đôi khi là một chuỗi văn bản (String) chứa JSON được encode, cần parse thêm một lần nữa khi xử lý.*

#### ⚠️ Ví dụ biến thể: Event dạng String (Server Handler)
Một số log Server ghi nhận request trực tiếp sẽ để `event` ở dạng chuỗi:
```json
{
    "event_source": "server",
    "event_type": "/courses/.../handler/xmodule_handler/problem_check",
    "event": "{\"GET\": {}, \"POST\": {\"input_id[]\": [\"choice_1\"]}}",
    "time": "2025-09-30T23:17:20Z"
}
```

---

## 📊 6. Thống kê Tổng quan Dữ liệu

### A. Số lượng Unique Name (Trường `name`)

| Loại ngày | Tổng Unique Name | Standard Events (không phải URL) | URL Paths (API/Navigation/Bot) |
| :--- | ---: | ---: | ---: |
| **Exam Days** | 23,452 | ~80 | ~23,372 |
| **Normal Days** | 76,404 | ~100 | ~76,304 |

> **Giải thích**: Mỗi request từ client đến server sẽ sinh ra một dòng log có `name` là đường dẫn URL (`/courses/...`). Vì vậy phần lớn unique name là URL path. Chỉ các **Standard Events** (tên sự kiện có quy chuẩn như `play_video`, `problem_check`) mới là dữ liệu phân tích hành vi trực tiếp.

### B. Số lượng Unique Courses & Users

| Chỉ số | Exam Days | Normal Days |
| :--- | ---: | ---: |
| **Unique Courses** (`context.course_id`) | 585 | 665 |
| **Unique Users** (`username`) | 4,464 | 12,548 |

> **Ghi chú**: Số lượng course và user bao gồm tất cả giá trị xuất hiện trong log gốc (chưa lọc noise). Các giá trị này bao gồm cả course test, course demo, và user admin/giảng viên.

---

## 🏷️ 7. Danh sách Sự kiện Quan trọng (Phân loại theo Nhóm)

### 🎯 Nhóm 1: Đánh giá & Kiểm tra (Assessment)
Các sự kiện liên quan trực tiếp đến quá trình làm bài thi/bài tập.

| Sự kiện (`name`) | Mô tả |
| :--- | :--- |
| `problem_check` | Sinh viên bấm **Submit** để nộp câu trả lời. Có 2 bản: `browser` (gửi lên) và `server` (xử lý kết quả đúng/sai). |
| `problem_graded` | Hệ thống đã chấm điểm và trả kết quả về cho trình duyệt. |
| `problem_save` | Sinh viên lưu nháp đáp án (chưa nộp chính thức). |
| `problem_show` | Sinh viên bấm "Show Answer" để xem đáp án (nếu được phép). |
| `problem_reset` | Sinh viên đặt lại câu trả lời về trạng thái ban đầu. |
| `showanswer` | Hiển thị đáp án đúng cho sinh viên (bản server). |
| `edx.grades.problem.submitted` | Hệ thống ghi nhận điểm vào sổ điểm (Gradebook). Chứa `weighted_earned` / `weighted_possible`. |
| `edx.grades.problem.state_deleted` | Admin/giảng viên xóa trạng thái bài làm của sinh viên. |
| `essay_question.create_submission` | Sinh viên nộp bài tự luận (essay). |

### 🕐 Nhóm 2: Thi tập trung (Timed/Proctored Exam)
Các sự kiện chỉ xuất hiện khi sinh viên tham gia bài thi có giới hạn thời gian.

| Sự kiện (`name`) | Mô tả |
| :--- | :--- |
| `edx.special_exam.timed.attempt.created` | Phiên thi được tạo (sinh viên bấm "Bắt đầu thi"). |
| `edx.special_exam.timed.attempt.started` | Đồng hồ đếm ngược bắt đầu chạy. |
| `edx.special_exam.timed.attempt.ready_to_submit` | Sinh viên bấm "Nộp bài" (chờ xác nhận). |
| `edx.special_exam.timed.attempt.submitted` | Bài thi đã được nộp chính thức. Có `attempt_completed_at` và `attempt_event_elapsed_time_secs`. |
| `edx.special_exam.timed.created` | Giảng viên tạo đề thi mới trên hệ thống. |
| `edx.special_exam.timed.updated` | Giảng viên cập nhật cấu hình đề thi (thay đổi thời gian, tên đề). |

### 🎥 Nhóm 3: Tương tác Video
Các sự kiện khi sinh viên xem bài giảng video.

| Sự kiện (`name`) | Mô tả |
| :--- | :--- |
| `play_video` | Bắt đầu phát video. Chứa `currentTime` (vị trí bắt đầu). |
| `pause_video` | Tạm dừng video. Chứa `currentTime`. |
| `seek_video` | Tua video. Chứa `old_time` và `new_time`. |
| `stop_video` | Dừng hẳn video. |
| `load_video` | Video được tải vào trình phát (player). |
| `speed_change_video` | Thay đổi tốc độ phát (0.5x, 1x, 1.5x, 2x). |
| `video_show_cc_menu` / `video_hide_cc_menu` | Bật/tắt menu phụ đề. |

### 🧭 Nhóm 4: Điều hướng & Tương tác LMS
Các sự kiện khi sinh viên di chuyển giữa các phần bài học.

| Sự kiện (`name`) | Mô tả |
| :--- | :--- |
| `edx.ui.lms.sequence.next_selected` | Bấm nút "Tiếp theo" để chuyển sang phần kế tiếp. |
| `edx.ui.lms.sequence.previous_selected` | Bấm nút "Quay lại" phần trước. |
| `edx.ui.lms.sequence.tab_selected` | Chọn một tab cụ thể trong phần bài học. |
| `edx.ui.lms.link_clicked` | Bấm vào một liên kết trong giao diện LMS. |
| `page_close` | Đóng trang (rời khỏi bài học). |
| `completion` | Đánh dấu hoàn thành một phần nội dung. |
| `book` | Truy cập giáo trình/tài liệu. |
| `edx.bookmark.added` / `edx.bookmark.removed` | Đánh dấu/gỡ đánh dấu trang. |

### 🏫 Nhóm 5: Quản trị & Hệ thống
Các sự kiện từ giảng viên, admin hoặc hệ thống tự động.

| Sự kiện (`name`) | Mô tả |
| :--- | :--- |
| `edx.course.enrollment.activated` | Sinh viên được kích hoạt ghi danh vào khóa học. |
| `edx.course.enrollment.deactivated` | Sinh viên bị hủy ghi danh. |
| `edx.cohort.user_added` | Thêm sinh viên vào nhóm (cohort). |
| `edx.certificate.created` | Chứng chỉ hoàn thành khóa học được tạo. |
| `edx.instructor.report.requested` | Giảng viên yêu cầu xuất báo cáo. |
| `edx.instructor.report.downloaded` | Giảng viên tải báo cáo xuống. |

### 📝 Nhóm 6: Chỉ có ở Normal Days
Các sự kiện xuất hiện thêm trong ngày thường (không có trong ngày thi).

| Sự kiện (`name`) | Mô tả |
| :--- | :--- |
| `openassessmentblock.create_submission` | Nộp bài ORA (Open Response Assessment - bài tự luận được chấm bởi bạn học). |
| `openassessmentblock.save_submission` | Lưu bản nháp bài ORA. |
| `openassessment.upload_file` | Upload file đính kèm bài tự luận. |
| `edx.certificate.created` | Chứng chỉ được tạo (sinh viên hoàn thành khóa học). |
| `unknown` | Sự kiện không xác định được loại. |

---

## 🚫 8. Dữ liệu Rác (Noise) & Bot/Scan

### Tỷ lệ Dữ liệu Rác so với Tổng thể
Dựa trên phân tích toàn bộ dữ liệu trong `BK_ACTIVITY_LOGS_UNZIPPED` (hơn 12,7 triệu bản ghi), tỷ lệ các request "linh tinh" (bot/scan/noise) được ghi nhận như sau:

| Đối tượng | Tổng số Log | Số lượng Rác | Tỷ lệ (%) |
| :--- | ---: | ---: | ---: |
| **Ngày thi (Exam Days)** | 2,364,524 | 59,847 | **2.53%** |
| **Ngày thường (Normal Days)** | 10,382,988 | 623,287 | **6.00%** |
| **Toàn bộ Bộ dữ liệu** | **12,747,512** | **683,134** | **5.36%** |

> **Nhận xét**: 
> - **Ngày thường có tỷ lệ rác cao hơn gấp đôi so với ngày thi (~6% vs 2.5%)**. Điều này có thể lý giải do trong ngày thi, lưu lượng từ người dùng thực tăng đột biến (sinh viên tập trung thi), làm "loãng" mật độ các request tự động từ bot.
> - Trung bình cứ khoảng **18 request** thì có **1 request** là dữ liệu rác không phục vụ phân tích hành vi học tập.

### Giải thích: Các `name` dạng URL lạ (ví dụ `/zz8.php`) là gì?

Trong bộ dữ liệu, rất nhiều dòng log có `name` là các đường dẫn URL không liên quan đến hệ thống LMS. Đây là **request từ bot tự động hoặc hacker scanner** quét lỗ hổng bảo mật trên server. Ví dụ:

| Tên URL | Mục đích của kẻ tấn công |
| :--- | :--- |
| `/zz8.php` | Tìm file PHP backdoor đã cài trước đó. |
| `/wp-login.php` | Quét xem server có chạy WordPress (để tấn công brute-force mật khẩu). |
| `/.env` | Tìm file biến môi trường chứa mật khẩu cơ sở dữ liệu, API key. |
| `/.git/config` | Tìm thư mục Git để download toàn bộ mã nguồn. |
| `/xmlrpc.php` | Khai thác lỗ hổng XML-RPC của WordPress. |
| `/phpinfo.php`, `/info.php` | Xem cấu hình PHP server (phiên bản, module, đường dẫn). |

**Các request này hoàn toàn không liên quan đến hoạt động học tập** và cần được **loại bỏ (filter)** khi phân tích hành vi người dùng. Dấu hiệu nhận biết:
- `name` chứa đuôi file `.php`, `.env`, `.sql`, `.git`, `.asp`, `.tar.gz`.
- `username` thường là rỗng (người dùng chưa đăng nhập).
- `event_source` là `server` và `event_type` là chính đường dẫn URL đó.

### Top 10 request Bot/Scan phổ biến nhất (Normal Days)

| Thứ tự | Request | Số lần |
| :--- | :--- | ---: |
| 1 | `/import_status/.../course.caxpitnn.tar.gz` | 7,133 |
| 2 | `/wp-login.php` | 2,587 |
| 3 | `/.env` | 1,857 |
| 4 | `/.git/config` | 1,648 |
| 5 | `/xmlrpc.php` | 1,210 |
| 6 | `/info.php` | 447 |
| 7 | `/api/.env` | 403 |
| 8 | `//xmlrpc.php` | 384 |
| 9 | `/index.php` | 358 |
| 10 | `/phpinfo.php` | 327 |

---
*Tài liệu này giúp bạn phân biệt rõ ràng giữa các trường "luôn có" (Root) và các trường "có điều kiện" (Nested Module/Event), cũng như nhận diện dữ liệu hữu ích và dữ liệu rác trong bộ log.*

---

## 🚀 9. Tiềm năng Khai thác Dữ liệu (Extended Use Cases)

Ngoài thống kê cơ bản, bộ dữ liệu 12.7 triệu bản ghi này có thể được khai thác cho các bài toán phân tích hành vi chuyên sâu (Learning Analytics). Dưới đây là các ý tưởng tiêu biểu phân loại theo phương thức xử lý:

### A. Nhóm Xử lý Lô (Batch Processing)
*Dùng để phân tích dữ liệu lịch sử, tìm quy luật lâu dài và tối ưu hóa hệ thống.*

1.  **Dựng lại Lộ trình Học tập (Learning Path Reconstruction)**:
    - **Mô tả**: Kết nối các log theo `session` để xem sinh viên di chuyển như thế nào giữa Video -> Tài liệu -> Bài tập.
    - **Giá trị**: Xác định các "mô hình thành công" để gợi ý lộ trình cho sinh viên khác.
    - **Phương pháp**: Chạy định kỳ sau mỗi chương hoặc cuối khóa học.

2.  **Bản đồ Nhiệt Video (Video Hotmap)**:
    - **Mô tả**: Tổng hợp các sự kiện `seek_video` và `pause_video` theo từng giây của video bài giảng.
    - **Giá trị**: Tìm ra đoạn nào khó hiểu (bị tua lại nhiều) hoặc đoạn nào chán (bị bỏ qua).
    - **Phương pháp**: Xử lý tập trung trên toàn bộ log của một môn học.

3.  **Hệ thống Cảnh báo sớm (Early Warning System)**:
    - **Mô tả**: Tính toán mức độ tương tác (Engagement Score). Nếu sinh viên không đăng nhập quá 1 tuần hoặc điểm bài tập thấp dần -> gửi email nhắc nhở.
    - **Giá trị**: Giảm tỷ lệ sinh viên bỏ học (retention).
    - **Phương pháp**: Chạy ETL (Extract, Transform, Load) hàng đêm.

### B. Nhóm Xử lý Luồng (Streaming Processing)
*Dùng để phát hiện sự cố hoặc can thiệp ngay lập tức khi hành động đang xảy ra.*

1.  **Giám sát Tính liêm chính Phòng thi (Exam Integrity)**:
    - **Mô tả**: Phát hiện ngay lập tức nếu 2 tài khoản làm bài thi từ cùng một địa chỉ IP, hoặc phát hiện sinh viên chuyển Tab liên tục.
    - **Giá trị**: Ngăn chặn gian lận ngay trong lúc quá trình thi đang diễn ra.
    - **Phương pháp**: Sử dụng Kafka/Flink để bắt log từ Server và phân tích Real-time.

2.  **Dự báo Tải trọng Hệ thống (Infrastructure Scaling)**:
    - **Mô tả**: Theo dõi tốc độ sinh log. Nếu số lượng request `problem_check` tăng vọt (Peak load) -> tự động mở rộng tài nguyên server (Auto-scaling).
    - **Giá trị**: Tránh sập web khi hàng nghìn sinh viên nộp bài cùng lúc vào 5 phút cuối giờ thi.
    - **Phương pháp**: Phân tích Windowing (5 phút/lần) trên luồng dữ liệu thô.

3.  **Gợi ý Học tập Tức thời (Next-step Recommendation)**:
    - **Mô tả**: Dựa trên kết quả `problem_check` vừa nộp (nếu sai 3 lần liên tiếp) -> hiển thị ngay pop-up gợi ý sinh viên xem lại đoạn Video tương ứng.
    - **Giá trị**: Hỗ trợ sinh viên đúng lúc họ đang gặp khó khăn.
    - **Phương pháp**: Xử lý sự kiện (Event-driven) trên luồng log client.

| Use Case | Phương thức | Giá trị chính |
| :--- | :--- | :--- |
| **Integrity Check** | **Streaming** | Công bằng trong thi cử |
| **Video Hotmap** | **Batch** | Tối ưu nội dung bài học |
| **Early Warning** | **Batch** | Chăm sóc sinh viên |
| **Peak Load Prediction**| **Streaming** | Ổn định hệ thống |
| **Learning Flow** | **Batch** | Nghiên cứu sư phạm |

---
