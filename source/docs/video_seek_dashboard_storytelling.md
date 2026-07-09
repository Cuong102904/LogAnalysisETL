# Video Seek Dashboard Storytelling Guide

Tài liệu này dùng để chụp ảnh chart và dashboard đưa vào slide.
Mục tiêu là kể một câu chuyện ngắn, rõ, đúng trọng tâm:

1. Learners đang xem video nào.
2. Họ tua ở đoạn nào.
3. Họ tua theo hướng nào.
4. Video nào có dấu hiệu cần xem lại.

## 1. Thông điệp chính

- `Course` là đơn vị phân tích chính.
- `Video` là đơn vị drill-down.
- `position_bucket_30s` cho biết đoạn video đang bị tương tác ở mốc 30 giây nào.
- `seek_count`, `seek_forward_count`, `seek_backward_count`, `retention_rate`, `completion_rate` là các metric chính để kể chuyện.

## 1.1. Gold tables được tạo từ đâu

Gold tables không đọc trực tiếp từ raw log.
Chúng được build từ bảng Silver đã chuẩn hóa cho video interaction, tức một bảng event-level có các cột như:

- `event_time_utc`
- `user_id`
- `course_id`
- `video_id`
- `video_block_id`
- `video_code`
- `action_type`
- `duration_s`
- `current_time_s`
- `old_time_s`
- `new_time_s`
- `speed`
- `seek_type`
- `watch_ratio`
- `position_bucket_30s`

Ý nghĩa của lớp Gold là:

- gom các event nhỏ thành insight theo câu hỏi nghiệp vụ
- mỗi bảng có một `grain` rõ ràng
- BI dashboard chỉ đọc Gold, không phải tính lại từ event thô

### Cách tạo Gold theo đúng tư duy

1. Chọn câu hỏi cần trả lời.
2. Chọn một `grain` duy nhất cho bảng.
3. Từ Silver, group by theo grain đó.
4. Tính metric bằng count, sum, avg, max, count distinct, hoặc tỷ lệ.
5. Dùng bảng Gold đó để vẽ chart hoặc làm filter drill-down.

Ví dụ:

- muốn xem từng user với từng video -> grain: `user_id + course_id + video_id`
- muốn xem đoạn nào bị tua nhiều -> grain: `course_id + video_id + position_bucket_30s`
- muốn xem tổng quan 1 video -> grain: `course_id + video_id`

## 2. Cấu trúc slide đề xuất

### Slide 1. Executive Overview

**Tiêu đề slide**

- `Where learners get stuck in course videos`

**Ảnh nên chụp**

- Ảnh dashboard tổng quan.

**Ý nghĩa**

- Cho thấy đây là một dashboard phục vụ phân tích hành vi video theo course.
- Người xem hiểu ngay có thể lọc theo `Course` và `Video`.
- Đây là ảnh mở đầu, không cần giải thích kỹ metric.

**Caption ngắn**

- `Video interaction overview by course, with drill-down to video and time segment.`

## 3. Các Gold tables và metric tạo ra chúng

Phần này dùng để giải thích ngắn gọn vì sao dashboard có các bảng Gold đó.

### Luồng batch để tạo Gold

1. Đọc dữ liệu từ `silver.video_events`.
2. Giữ lại các cột phân tích cần thiết cho video.
3. Tạo các cột dẫn xuất nếu thiếu:
   - `event_date` từ `event_time_utc`
   - `position_bucket_30s` từ `current_time_s` nếu chưa có sẵn
   - `seek_direction` từ `old_time_s` và `new_time_s`
4. Tách từng nhóm câu hỏi nghiệp vụ thành một bảng Gold riêng.
5. Group by theo đúng `grain` của bảng đó.
6. Tính metric bằng `count`, `sum`, `avg`, `max`, `count distinct`, hoặc tỷ lệ.
7. Ghi kết quả ra Delta table trong layer `gold`.
8. Trino register các bảng này để Superset đọc và vẽ chart.

### 1. `gold_user_video_engagement`

**Mục đích**

- Một dòng cho một `user` trong một `video` của một `course`.
- Dùng để trả lời: người dùng đó đã tương tác với video này như thế nào?

**Grain**

- `user_id + course_id + video_id + video_block_id + video_code`

**Metric chính và cách tính**

- `event_count`: `count(*)`
- `load_count`: đếm số event có `action_type = load_video`
- `play_count`: đếm số event có `action_type = play_video`
- `pause_count`: đếm số event có `action_type = pause_video`
- `stop_count`: đếm số event có `action_type = stop_video`
- `seek_count`: đếm số event có `action_type = seek_video`
- `speed_change_count`: đếm số event có `action_type = speed_change_video`
- `session_count`: `count(distinct session_id)`
- `video_length_s`: `max(duration_s)`
- `max_watch_ratio`: `max(watch_ratio)`
- `avg_watch_ratio`: `avg(watch_ratio)`
- `max_position_s`: `max(current_time_s)`
- `avg_position_s`: `avg(current_time_s)`
- `seek_forward_count`: số seek mà `new_time_s > old_time_s`
- `seek_backward_count`: số seek mà `new_time_s < old_time_s`
- `avg_seek_distance_s`: `avg(abs(new_time_s - old_time_s))`
- `completed_flag`: 1 nếu `max_watch_ratio >= 0.9`, ngược lại 0

**Cách kể trên slide**

- "Người học này xem bao nhiêu lần, tua bao nhiêu lần, và có hoàn thành video không?"

### 2. `gold_user_video_engagement_daily`

**Mục đích**

- Một dòng cho một `user` trong một `video` theo từng ngày.
- Dùng khi muốn xem hành vi thay đổi theo ngày.

**Grain**

- `event_date + user_id + course_id + video_id + video_block_id + video_code`

**Metric**

- giống `gold_user_video_engagement`, nhưng được tách theo `event_date`

**Cách kể trên slide**

- "Trong ngày hôm nay người học tương tác với video đó như thế nào?"

### 3. `gold_course_video_summary_daily`

**Mục đích**

- Một dòng cho một `video` trong một `course` theo ngày.
- Dùng để tổng hợp video nào đang gây nhiều tương tác hoặc completion thấp.

**Grain**

- `event_date + course_id + video_id + video_block_id + video_code`

**Metric chính và cách tính**

- `active_users`: `count(distinct user_id)`
- `session_count`: `count(distinct session_id)`
- `event_count`: `count(*)`
- `load_count`, `play_count`, `pause_count`, `stop_count`, `seek_count`, `speed_change_count`: đếm theo `action_type`
- `video_length_s`: `max(duration_s)`
- `avg_watch_ratio`: `avg(watch_ratio)`
- `max_watch_ratio`: `max(watch_ratio)`
- `max_position_s`: `max(current_time_s)`
- `completed_users`: đếm distinct `user_id` có `watch_ratio >= 0.9`
- `seek_users`: đếm distinct `user_id` có `action_type = seek_video`
- `completion_rate`: `completed_users / active_users`
- `seek_rate`: `seek_users / active_users`
- `dropoff_rate`: `1 - completion_rate`

**Cách kể trên slide**

- "Video nào dài nhưng completion thấp?"
- "Video nào nhiều người xem nhưng cũng nhiều người tua?"

### 4. `gold_course_video_seek_hotspots_daily`

**Mục đích**

- Một dòng cho một `course + video + bucket thời gian`.
- Dùng để tìm đoạn bị tua nhiều nhất.

**Grain**

- `event_date + course_id + video_id + video_block_id + video_code + position_bucket_30s`

**Metric chính và cách tính**

- chỉ lấy event có `action_type = seek_video`
- `seek_event_count`: `count(*)`
- `unique_seek_users`: `count(distinct user_id)`
- `seek_forward_count`: đếm seek có `new_time_s > old_time_s`
- `seek_backward_count`: đếm seek có `new_time_s < old_time_s`
- `avg_seek_distance_s`: `avg(abs(new_time_s - old_time_s))`
- `max_seek_distance_s`: `max(abs(new_time_s - old_time_s))`

**Cách kể trên slide**

- "Đoạn nào của video bị tua nhiều nhất?"
- "Họ tua đi hay tua lại?"

### 5. `gold_video_retention_by_bucket_daily`

**Mục đích**

- Một dòng cho một bucket 30 giây của một video.
- Dùng để xem người học còn đi tiếp đến đâu trong video.

**Grain**

- `event_date + course_id + video_id + video_block_id + video_code + position_bucket_30s`

**Metric chính và cách tính**

- `total_users`: tổng user có tương tác với video
- `users_reached_bucket`: số user đã đi đến bucket đó hoặc xa hơn
- `users_paused_here`: số user pause tại bucket đó
- `users_stopped_here`: số user stop tại bucket đó
- `users_seeked_here`: số user seek tại bucket đó
- `pause_event_count`: số event pause
- `stop_event_count`: số event stop
- `seek_event_count`: số event seek
- `retention_rate`: `users_reached_bucket / total_users`

**Cách kể trên slide**

- "Đến đoạn nào thì người học bắt đầu rơi rụng?"
- "Retention tụt ở bucket nào?"

### Slide 2. Seek Heatmap

**Tiêu đề chart**

- `Seek hotspots by video segment`

**Ảnh nên chụp**

- Chart heatmap ở phần trên cùng của dashboard.

**Cột chính**

- Trục X: `position_bucket_30s`
- Trục Y: `video_code`
- Màu sắc: `seek_event_count`

**Chart này trả lời**

- Trong course này, video nào bị tua nhiều nhất.
- Trong một video, đoạn nào bị tua nhiều nhất.
- Đoạn nào có mật độ tua cao bất thường.

**Cách đọc**

- Ô màu đậm hơn nghĩa là có nhiều lượt tua hơn.
- Nếu một bucket 30 giây nổi bật, đó là đoạn học viên dừng lại, tua lại, hoặc nhảy qua nhiều lần.

**Caption ngắn**

- `Darker cells indicate video segments with higher seek activity.`

## 4. Công thức tính metric, viết theo kiểu dễ nói khi thuyết trình

Nếu cần nói ngắn gọn trong slide hoặc thuyết trình, dùng các mẫu sau:

- `count(*)` nghĩa là đếm số sự kiện.
- `count(distinct user_id)` nghĩa là đếm số người dùng duy nhất.
- `sum(case when action_type = 'seek_video' then 1 else 0 end)` nghĩa là đếm số lần tua.
- `max(watch_ratio)` nghĩa là mức xem cao nhất mà user đạt được trong video đó.
- `avg(watch_ratio)` nghĩa là mức xem trung bình trên các event.
- `completion_rate = completed_users / active_users`
- `retention_rate = users_reached_bucket / total_users`
- `seek_forward_count` là số lần tua tới.
- `seek_backward_count` là số lần tua ngược lại để xem lại.

### Quy tắc đọc kết quả

- Metric lớn hơn không phải lúc nào cũng tốt.
- `seek_count` cao thường là dấu hiệu đoạn khó hiểu hoặc đoạn cần quay lại.
- `completion_rate` cao thường là dấu hiệu video dễ theo dõi hơn.
- `retention_rate` giảm mạnh tại một bucket là dấu hiệu có điểm rơi trong nội dung.

### Slide 3. Seek Direction

**Tiêu đề chart**

- `Selected video: seek direction by position`

**Ảnh nên chụp**

- Chart cột ở hàng thứ hai của dashboard.

**Cột chính**

- Trục X: `position_bucket_30s`
- Metric: `seek_forward_count`, `seek_backward_count`

**Chart này trả lời**

- Người học tua tới đoạn mới hay tua ngược lại để xem lại.
- Đoạn nào khiến người học phải quay lại nhiều hơn.

**Cách đọc**

- `forward seek` thường là dấu hiệu bỏ qua đoạn trước đó.
- `backward seek` thường là dấu hiệu cần xem lại để hiểu bài.

**Caption ngắn**

- `Forward seeks suggest skipping ahead, while backward seeks suggest rewatching for comprehension.`

### Slide 4. Retention by Position

**Tiêu đề chart**

- `Selected video: retention by position`

**Ảnh nên chụp**

- Chart line ở hàng thứ hai của dashboard.

**Cột chính**

- Trục X: `position_bucket_30s`
- Metric: `retention_rate`

**Chart này trả lời**

- Học viên còn đi tiếp đến đoạn nào của video.
- Đoạn nào bắt đầu rơi rụng nhiều.

**Cách đọc**

- Đường càng giảm mạnh, video càng dễ mất người học ở đoạn đó.
- Nếu retention tụt ở một bucket cụ thể, nên kiểm tra lại đoạn giảng, tốc độ nói, hoặc phần chuyển ý.

**Caption ngắn**

- `Retention drops mark the points where learners stop progressing through the video.`

### Slide 5. Summary Table

**Tiêu đề chart**

- `Course video summary table`

**Ảnh nên chụp**

- Bảng tóm tắt ở cuối dashboard.

**Cột chính**

- `video_code`
- `seek_count`
- `active_users`
- `video_length_s`
- `avg_watch_ratio`
- `completion_rate`

**Chart này trả lời**

- Video nào đang gây nhiều tương tác nhất.
- Video nào dài nhưng completion thấp.
- Video nào vừa dài vừa có nhiều seek.

**Caption ngắn**

- `This table ranks videos by interaction intensity and learning completion.` 

## 5. Dashboard hiện tại nên nói gì

Dashboard này không phải dashboard “để xem mọi thứ”.
Nó là dashboard để trả lời đúng một câu hỏi:

**Học viên đang bị vướng ở đâu trong video học?**

Nếu trình bày trên slide, nên nói theo thứ tự:

1. Tổng quan course.
2. Xác định video có vấn đề.
3. Xem đoạn bị tua nhiều.
4. Kiểm tra tua xuôi hay tua ngược.
5. Xem retention có tụt ở đoạn đó không.

## 6. Nên chụp ảnh nào

### Ảnh 1: Dashboard overview

- Chụp toàn bộ dashboard có filter bar.
- Mục đích: cho người xem biết có thể drill-down theo course và video.

### Ảnh 2: Heatmap

- Chụp phần heatmap phóng to.
- Mục đích: minh họa điểm nóng trong video.

### Ảnh 3: Direction + Retention

- Chụp hai chart ở hàng giữa cùng lúc nếu đủ khung hình.
- Mục đích: chứng minh đoạn bị tua không chỉ “nhiều” mà còn “tua theo hướng nào”.

### Ảnh 4: Summary table

- Chụp bảng tổng hợp nếu cần slide chốt.
- Mục đích: cho thấy bảng có thể dùng như danh sách video ưu tiên cần cải thiện.

## 7. Checklist trước khi chụp

- `Course` filter đã chọn đúng course cần kể chuyện.
- `Video` filter chỉ còn các video thuộc course đó.
- Heatmap đang hiển thị đúng bucket 30 giây.
- `Retention rate` không bị lỗi scale hay bị đè bởi filter thời gian.
- Tên chart rõ ràng, không dùng tên kỹ thuật khó hiểu.
- Không chụp ảnh có quá nhiều khoảng trắng trống nếu mục tiêu là đưa vào slide.

## 8. Kết luận ngắn để nói trên slide

- `Dashboard cho thấy video nào gây nhiều tương tác nhất, đoạn nào bị tua nhiều nhất, và học viên dừng lại ở đâu.`
- `Heatmap xác định điểm nóng, bar chart xác định hướng tua, line chart xác định điểm rơi retention.`
- `Từ đó có thể ưu tiên chỉnh sửa nội dung ở những đoạn gây vướng nhất.`
