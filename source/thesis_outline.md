# CHAPTER 1. INTRODUCTION

## 1.1. Motivation
- Trình bày learning logs như một dạng dữ liệu quan trọng trong các hệ thống học trực tuyến, vì nó phản ánh trực tiếp quá trình tương tác của người học với nội dung, bài kiểm tra và nền tảng học tập.
- Làm rõ vấn đề không chỉ nằm ở việc “có dữ liệu để phân tích”, mà là dữ liệu này có tính chất event-based, phát sinh theo thời gian, có thể đến từ nhiều nguồn và cần một kiến trúc xử lý dữ liệu phù hợp.
- Chỉ ra rằng nếu chỉ xử lý learning logs bằng cách thủ công hoặc theo mô hình dữ liệu truyền thống thì sẽ khó mở rộng, khó tái xử lý dữ liệu thô và khó hỗ trợ các nhu cầu phân tích gần thời gian thực.
- Từ đó dẫn tới nhu cầu thiết kế một Big Data pipeline có khả năng tổ chức dữ liệu theo nhiều tầng, hỗ trợ batch/near-real-time processing và có basic data governance để kiểm soát schema, chất lượng dữ liệu và lineage.
- Giới thiệu daotao.ai MOOC structured logs và EdNet như hai use case thực nghiệm, trong đó mục tiêu không phải phân tích riêng từng hệ thống học tập mà là kiểm chứng khả năng áp dụng của pipeline với nhiều nguồn learning logs.

## 1.2. Objectives and scope of the graduation thesis

### Objectives
- Mục tiêu chính của đồ án là thiết kế và triển khai một kiến trúc Big Data pipeline cho dữ liệu logs/events, được kiểm chứng thông qua các bộ dữ liệu learning logs như daotao.ai và EdNet.
- Đồ án sử dụng learning logs như một use case thực nghiệm để xác định các yêu cầu điển hình của dữ liệu logs/events đối với kiến trúc Big Data, bao gồm lưu trữ dữ liệu thô, xử lý batch, khả năng tái xử lý, mở rộng near-real-time và tạo dữ liệu phục vụ phân tích.
- Đồ án khảo sát các hướng kiến trúc Big Data như Data Warehouse, Data Lake, Lakehouse/Medallion, Lambda Architecture và Kappa Architecture để phân tích mức độ phù hợp của từng hướng với các yêu cầu đã xác định.
- Đồ án đề xuất một pipeline theo hướng Lakehouse/Medallion, trong đó Bronze layer lưu dữ liệu gốc, Silver layer chuẩn hóa dữ liệu, và Gold layer tạo dữ liệu phục vụ phân tích.
- Đồ án triển khai prototype để kiểm chứng pipeline trên dataset daotao.ai và sử dụng EdNet như một trường hợp kiểm tra khả năng thích nghi của pipeline với nguồn dữ liệu có schema khác.
- Đồ án đánh giá hệ thống ở mức prototype, tập trung vào tính đúng đắn của pipeline, khả năng xử lý dữ liệu, khả năng near-real-time/micro-batch và mức độ hỗ trợ các cơ chế governance cơ bản.

### Scope
- Phạm vi của đồ án là kiến trúc dữ liệu và data pipeline, không phải xây dựng một nền tảng MOOC hoàn chỉnh hoặc một hệ thống phân tích nghiệp vụ đầy đủ cho người dùng cuối.
- daotao.ai là dataset chính để triển khai pipeline, còn EdNet là dataset kiểm chứng bổ sung để xem thiết kế có bị phụ thuộc vào một schema duy nhất hay không.
- Phần phân tích trên Gold layer chỉ nhằm chứng minh pipeline tạo được dữ liệu analytics-ready, nên chủ yếu dừng ở descriptive analytics như hoạt động người học, phân bố event và hoạt động theo thời gian.
- Các hướng như anomaly detection, dropout prediction hoặc personalization chỉ được xem là ứng dụng tiềm năng của Gold layer; nếu có làm anomaly thì chỉ nên là minh họa rule-based đơn giản, không phải mô hình ML hoàn chỉnh.
- Streaming trong phạm vi đồ án được hiểu là near-real-time hoặc micro-batch processing, không đặt mục tiêu đạt low-latency real-time như hệ thống production.
- Data governance chỉ triển khai ở mức cơ bản, gồm data zones, schema management, data quality checks, metadata và lineage logic, không mở rộng sang enterprise governance đầy đủ.

## 1.3. Tentative solution
- Đồ án đề xuất một pipeline theo hướng Lakehouse/Medallion để giải quyết vấn đề cốt lõi là learning logs cần vừa được lưu trữ ở dạng gốc, vừa được chuẩn hóa và tổng hợp để phục vụ phân tích.
- Bronze layer được thiết kế để giữ dữ liệu gần nguyên bản từ daotao.ai và EdNet, giúp hệ thống có khả năng kiểm tra lại, tái xử lý và thích nghi khi logic phân tích thay đổi.
- Silver layer tập trung giải quyết vấn đề khác biệt schema giữa các nguồn dữ liệu bằng cách làm sạch, chuẩn hóa timestamp, chuẩn hóa event type và ánh xạ dữ liệu về một common learning event model.
- Gold layer tập trung tạo ra các bảng phân tích có ý nghĩa chung cho learning logs, ví dụ hoạt động theo người học, hoạt động theo nội dung/khóa học, phân bố event và thống kê theo thời gian.
- Với near-real-time processing, đồ án có thể mô phỏng luồng logs bằng cách chia dataset thành micro-batch hoặc replay event, từ đó kiểm chứng pipeline có khả năng xử lý dữ liệu liên tục ở mức prototype.
- Basic governance được gắn trực tiếp vào pipeline, không đứng riêng như một hệ thống lớn, nhằm bảo đảm mỗi tầng dữ liệu có schema, kiểm tra chất lượng, metadata và lineage rõ ràng.

## 1.4. Thesis organization
- **Chapter 2:** review các kiến trúc Big Data có liên quan đến xử lý dữ liệu logs/events, phân tích mức độ phù hợp của từng kiến trúc và xác định hướng kiến trúc được chọn cho đồ án.
- **Chapter 3:** trình bày thiết kế hệ thống ở mức lý thuyết, bao gồm kiến trúc tổng thể, mô hình dữ liệu chung, các lớp Bronze/Silver/Gold, pipeline xử lý, near-real-time/micro-batch và basic governance.
- **Chapter 4:** trình bày quá trình triển khai prototype, môi trường thực nghiệm, các kịch bản đánh giá, kết quả thực nghiệm và phần thảo luận dựa trên mục tiêu đã đặt ra.
- **Chapter 5:** tổng kết kết quả đạt được, nêu đóng góp, hạn chế của prototype và đề xuất hướng phát triển tiếp theo.

# CHAPTER 2. BIG DATA ARCHITECTURE REVIEW

## 2.1. Chapter Introduction
- Chương này trình bày tổng quan và đánh giá các kiến trúc Big Data có thể áp dụng cho bài toán xử lý dữ liệu logs/events, với learning logs được sử dụng như use case thực nghiệm.
- Trọng tâm của chương là phân tích mức độ phù hợp của từng kiến trúc đối với các yêu cầu như lưu trữ dữ liệu thô, xử lý batch, near-real-time, tái xử lý, analytics layer và data governance cơ bản.

## 2.2. Characteristics of Learning Logs as Big Data
- Phần này phân tích learning logs như một dạng dữ liệu sự kiện có timestamp, phản ánh hành vi người học và có thể phát sinh từ nhiều nguồn dữ liệu khác nhau.
- Từ các đặc trưng này, mục này rút ra các yêu cầu chính mà một kiến trúc Big Data cần đáp ứng, như lưu raw data, chuẩn hóa schema, tái xử lý, xử lý gần thời gian thực và tạo dữ liệu phục vụ analytics.

## 2.3. Review of Big Data Architectural Models
- Review các mô hình kiến trúc phổ biến gồm Data Warehouse, Data Lake, Lakehouse/Medallion, Lambda Architecture và Kappa Architecture.
- Với mỗi kiến trúc, nội dung sẽ không chỉ mô tả khái niệm, mà tập trung phân tích kiến trúc đó hỗ trợ tốt yêu cầu nào, hạn chế ở đâu và phù hợp trong bối cảnh nào.

## 2.4. Comparative Analysis of Architectural Suitability
- Đối sánh các kiến trúc dựa trên yêu cầu của đồ án, ví dụ khả năng lưu raw logs, hỗ trợ batch, hỗ trợ near-real-time, khả năng tái xử lý, governance và độ phức tạp triển khai.
- Việc đối sánh có thể được trình bày bằng bảng để làm rõ từng kiến trúc hỗ trợ tốt hoặc hạn chế ở những tiêu chí nào, nhưng không kết luận theo kiểu một kiến trúc luôn tốt hơn các kiến trúc còn lại.

## 2.5. Selected Architectural Direction
- Phần này trình bày lý do lựa chọn hướng Lakehouse/Medallion làm kiến trúc chính cho đồ án, dựa trên khả năng kết hợp lưu trữ raw data, chuẩn hóa dữ liệu, tạo analytics layer và hỗ trợ governance cơ bản.
- Đồng thời, mục này làm rõ rằng đồ án không triển khai đầy đủ Lambda hoặc Kappa, nhưng có thể kế thừa ý tưởng near-real-time/micro-batch để kiểm chứng pipeline ở mức prototype.

## 2.6. Chapter Summary
- Chương này tổng kết các kiến trúc đã review và nêu kết luận về hướng kiến trúc phù hợp với mục tiêu của đồ án.
- Kết quả của chương là cơ sở để Chapter 3 thiết kế hệ thống cụ thể, bao gồm data pipeline, data layers, common event model và deployment strategy.

# CHAPTER 3. SYSTEM DESIGN AND PROPOSED SOLUTION

## 3.1. Chapter Introduction
- Chương này trình bày thiết kế hệ thống dựa trên hướng kiến trúc đã lựa chọn ở Chapter 2.
- Nội dung chính gồm kiến trúc tổng thể, mô hình dữ liệu chung, thiết kế các data layers, pipeline, near-real-time processing, basic governance và phương án triển khai prototype.

## 3.2. Overall System Architecture
- Phần này mô tả kiến trúc tổng thể của hệ thống từ nguồn dữ liệu đầu vào đến lớp dữ liệu phân tích đầu ra.
- Trọng tâm là làm rõ vai trò của từng thành phần trong pipeline và cách các thành phần liên kết với nhau để xử lý learning logs.

## 3.3. Common Learning Event Model
- Phần này đề xuất một mô hình dữ liệu chung để chuẩn hóa learning logs từ nhiều nguồn khác nhau như daotao.ai và EdNet.
- Mục tiêu là giảm sự phụ thuộc của pipeline vào schema riêng của từng dataset và hỗ trợ khả năng mở rộng sang các nguồn logs khác.

## 3.4. Medallion Data Layer Design
- Phần này thiết kế các lớp Bronze, Silver và Gold, trong đó mỗi lớp giải quyết một vấn đề cụ thể của pipeline.
- Bronze tập trung vào lưu raw data, Silver tập trung vào làm sạch và chuẩn hóa, còn Gold tập trung vào tạo dữ liệu sẵn sàng cho phân tích.

## 3.5. Data Pipeline Design
- Phần này mô tả luồng xử lý dữ liệu từ ingestion đến analytics, bao gồm các bước kiểm tra schema, làm sạch dữ liệu, chuyển đổi dữ liệu và tổng hợp dữ liệu.
- Nội dung sẽ làm rõ pipeline chính dùng cho daotao.ai và cách pipeline có thể được kiểm chứng thêm với EdNet.

## 3.6. Near-Real-Time Processing Design
- Phần này trình bày cách hệ thống hỗ trợ near-real-time hoặc micro-batch processing trong phạm vi prototype.
- Mục tiêu là làm rõ cách mô phỏng hoặc xử lý luồng logs phát sinh liên tục, không đặt yêu cầu real-time production-level.

## 3.7. Basic Data Governance Design
- Phần này thiết kế các cơ chế governance cơ bản gắn với pipeline, gồm schema management, data quality checks, metadata và lineage logic.
- Mục tiêu là bảo đảm dữ liệu trong từng layer có thể được kiểm soát, theo dõi và tái xử lý khi cần.

## 3.8. Prototype Deployment Design
- Phần này mô tả phương án triển khai hệ thống ở mức prototype, bao gồm cách tổ chức môi trường, cách chạy pipeline và cách quan sát kết quả xử lý.
- Nội dung chỉ trình bày thiết kế triển khai ở mức tổng quan, còn thông số cấu hình và tham số thực nghiệm chi tiết sẽ được trình bày ở Chapter 4.

## 3.9. Chapter Summary
- Chương này tổng kết thiết kế hệ thống và cách thiết kế đó hiện thực hóa hướng kiến trúc đã chọn.
- Đây là cơ sở trực tiếp để triển khai prototype và thực hiện đánh giá thực nghiệm ở Chapter 4.

# CHAPTER 4. IMPLEMENTATION AND EXPERIMENTAL EVALUATION

## 4.1. Chapter Introduction
- Chương này trình bày quá trình triển khai prototype và đánh giá thực nghiệm hệ thống dựa trên thiết kế ở Chapter 3.
- Trọng tâm là kiểm chứng hệ thống có đáp ứng được mục tiêu về pipeline, near-real-time processing, governance cơ bản và khả năng thích nghi dữ liệu hay không.

## 4.2. Experimental Setup
- Phần này mô tả môi trường thực nghiệm, dữ liệu đầu vào, cấu hình phần cứng/phần mềm và các tham số chính được sử dụng trong quá trình chạy thử.
- Nội dung này chỉ tập trung vào các thông tin cần thiết để tái hiện thực nghiệm, tránh lặp lại phần thiết kế đã trình bày ở Chapter 3.

## 4.3. Prototype Implementation
- Phần này trình bày các thành phần đã được hiện thực hóa trong prototype, bao gồm pipeline xử lý dữ liệu, các layer dữ liệu, common event model, near-real-time simulation và governance cơ bản.
- Nội dung sẽ đối chiếu ngắn gọn giữa thiết kế dự kiến và phần đã triển khai thực tế.

## 4.4. Experimental Scenarios
- Phần này mô tả các kịch bản đánh giá chính, ví dụ kiểm chứng pipeline với daotao.ai, kiểm chứng near-real-time/micro-batch, kiểm chứng governance và kiểm chứng khả năng thích nghi với EdNet.
- Mỗi kịch bản sẽ được thiết kế để trả lời một câu hỏi đánh giá cụ thể, thay vì chỉ chạy thử hệ thống một cách chung chung.

## 4.5. Evaluation Metrics
- Phần này xác định các tiêu chí đánh giá như tính đúng đắn của pipeline, tính hợp lệ của schema, thời gian xử lý, độ ổn định của micro-batch, chất lượng dữ liệu và mức độ hoàn chỉnh của metadata/lineage.
- Các tiêu chí này dùng để đánh giá tính khả thi của kiến trúc trong phạm vi prototype, không nhằm benchmark toàn diện các công nghệ Big Data.

## 4.6. Experimental Results
- Phần này trình bày kết quả thực nghiệm theo từng kịch bản, bao gồm output của các layer, kết quả Gold analytics, kết quả kiểm tra chất lượng dữ liệu và kết quả xử lý near-real-time nếu có.
- Kết quả có thể được minh họa bằng bảng hoặc biểu đồ, nhưng chỉ phục vụ đánh giá pipeline chứ không mở rộng thành phân tích nghiệp vụ MOOC chuyên sâu.

## 4.7. Discussion
- Phần này phân tích kết quả thực nghiệm so với mục tiêu và yêu cầu đã đặt ra, làm rõ hệ thống đáp ứng tốt điểm nào và còn hạn chế ở đâu.
- Nội dung cần thảo luận về tính phù hợp của kiến trúc, giới hạn prototype, mức độ near-real-time, phạm vi governance và khả năng mở rộng trong tương lai.

## 4.8. Chapter Summary
- Chương này tổng kết quá trình triển khai và các kết quả thực nghiệm chính.
- Kết quả này là cơ sở để đưa ra kết luận, đóng góp và hướng phát triển ở Chapter 5.

# CHAPTER 5. CONCLUSION AND FUTURE WORK

## 5.1. Conclusion
- Phần này tổng kết những nội dung chính đã thực hiện, từ review kiến trúc, lựa chọn hướng Lakehouse/Medallion, thiết kế pipeline đến triển khai và đánh giá prototype.
- Nội dung cần đánh giá lại mức độ hoàn thành mục tiêu đã nêu ở Chapter 1.

## 5.2. Contributions
- Phần này nêu các đóng góp chính của đồ án, bao gồm thiết kế kiến trúc pipeline, mô hình event chung, tổ chức data layers và governance cơ bản cho learning logs.
- Các đóng góp nên được trình bày ở mức ứng dụng hệ thống, không phóng đại thành đóng góp thuật toán hoặc framework production.

## 5.3. Limitations
- Phần này trình bày các giới hạn của đồ án như môi trường tài nguyên hạn chế, near-real-time chỉ ở mức prototype, governance chưa đầy đủ như production và EdNet chỉ dùng để kiểm chứng bổ sung.
- Các giới hạn này giúp xác định rõ phạm vi thực tế của kết quả đạt được.

## 5.4. Future Work
- Phần này đề xuất hướng phát triển như triển khai trên cluster/cloud, mở rộng streaming pipeline, bổ sung metadata catalog, lineage tool, data quality framework và dashboard.
- Ngoài ra, có thể đề xuất các use case nâng cao trên Gold layer như anomaly detection, dropout prediction hoặc personalization khi pipeline đã hoàn thiện hơn.
