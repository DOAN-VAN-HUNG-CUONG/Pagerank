# Kịch bản thuyết trình + Q&A — PageRank trên hệ sinh thái Hadoop

Tài liệu này gồm:
- **Phần A:** Kịch bản theo từng slide (gợi ý lời nói, ước lượng thời gian ~20–22 phút).
- **Phần B:** Bộ câu hỏi Q&A có thể gặp + gợi ý trả lời.

Khớp với deck `PageRank_Presentation.pptx` (20 slide). Bạn tùy biến giọng văn cho tự nhiên.

---

## Phần A — Kịch bản theo slide

### Slide 1 — Tiêu đề (≈30s)
"Xin chào thầy/cô và các bạn. Em là **Doan Van Hung Cuong**, FPT University. Hôm nay em trình bày đồ án Big Data của mình: **PageRank trên hệ sinh thái Hadoop — một nghiên cứu so sánh có thể tái lập**. Mục tiêu là so sánh hiệu năng và độ chính xác của cùng một thuật toán PageRank khi viết bằng nhiều framework khác nhau, dưới cùng điều kiện."

### Slide 2 — Nội dung (≈30s)
"Bài trình bày gồm sáu phần: (1) bối cảnh và thuật toán, (2) các cách triển khai, (3) phương pháp thực nghiệm, (4) kết quả, (5) bài học kỹ thuật, (6) kết luận và hướng phát triển."

### Slide 3 — Giới thiệu (≈1.5 phút)
"PageRank là một thuật toán kinh điển — về ý tưởng nó rất đơn giản: mô phỏng một *người lướt web ngẫu nhiên*, và phân phối dừng của quá trình đó cho điểm số mỗi trang. Nó được dùng trong tìm kiếm web, hệ gợi ý, phát hiện gian lận, phân tích trích dẫn. Vấn đề thực tế là: cùng thuật toán có thể viết bằng MapReduce, Hadoop Streaming, Pig, Spark, mrjob — nhưng *hiệu năng, độ chính xác, bộ nhớ, công sức* hiếm khi được đo song song trong cùng điều kiện. Đó chính là khoảng trống mà đồ án này lấp."

### Slide 4 — Mục tiêu & đóng góp (≈1.5 phút)
"Đồ án có **bốn đóng góp chính**: thứ nhất, một *reference engine* viết bằng Python thuần, không phụ thuộc, làm chuẩn so sánh. Thứ hai, một *phương pháp kiểm chứng* — chứng minh các phiên bản tương đương số học so với oracle độc lập là NetworkX, ở mức 10⁻⁹. Thứ ba, một *so sánh hiệu năng đơn nút* trên một harness có thể tái lập. Thứ tư, một *catalogue lỗi kỹ thuật* xuyên framework — phần em thấy hữu ích nhất với người làm thực tế."

### Slide 5 — Thuật toán: random surfer & công thức (≈1.5 phút)
"Mô hình: với xác suất *d* (em dùng d = 0.85), người lướt đi theo một liên kết ra; với xác suất *1 − d*, họ teleport tới một trang theo phân phối p(u). Công thức tổng quát có ba số hạng: teleport, đóng góp từ các liên kết đến, và tái phân phối khối lượng dangling. Cùng một công thức bao trùm ba biến thể: chuẩn, có trọng số, và cá nhân hóa — chỉ khác cách chọn p và w."

### Slide 6 — Dangling node & tự chuẩn hóa (≈1 phút)
"Dangling node là đỉnh không có liên kết ra — chúng làm *rò rỉ* khối xác suất, khiến tổng nhỏ hơn 1. Cách khắc phục là tái phân phối khối lượng của chúng qua phân phối teleport. Một tính chất hay: tổng PR tuân theo **Sₖ₊₁ = (1−d) + d·Sₖ**, với điểm bất động S = 1. Nhờ tính tự chuẩn hóa này, bản Java có thể khởi tạo rank = 1.0 thay vì 1/N mà vẫn ra đúng — đơn giản hóa code."

### Slide 7 — Hội tụ & độ phức tạp (≈1 phút)
"Em lặp đến khi sai khác L1 giữa hai vòng nhỏ hơn ε = 10⁻³. Mỗi vòng chi phí O(|E|) — mọi phiên bản đều có cùng độ phức tạp tiệm cận, nên *khác biệt thời gian giữa chúng chỉ là hằng số*. Trên đồ thị minh họa nhỏ, thuật toán hội tụ sau 13 vòng lặp."

### Slide 8 — Mô hình thực thi (≈1.5 phút)
"Em khảo sát các framework đa dụng mà đội data thường vận hành, chứ không phải engine chuyên đồ thị. Bao gồm: **MapReduce** cổ điển, **Hadoop Streaming** (đưa MapReduce ra cho chương trình bất kỳ qua stdio), **Pig** (dataflow khai báo, compile xuống MapReduce), **Spark** (in-memory, RDD và DataFrame với Catalyst), và **mrjob** (Python wrapper cho MapReduce)."

### Slide 9 — Bảng 8 triển khai (≈1.5 phút)
"Bảng này tóm tắt tám phiên bản, mọi phiên bản đều cài cùng công thức. Có thể chia thành hai *họ*: **họ Python** (Core, mrjob-core, PySpark) tái phân phối dangling mass nên đúng cho mọi đồ thị, và **họ JVM 1-pass** (Java, Pig, Streaming) bỏ dangling mass — chỉ đúng khi mọi đỉnh có liên kết ra. Đây là một quyết định thiết kế trung thực: em làm rõ giới hạn này."

### Slide 10 — Họ Python (≈1 phút)
"Reference engine là module Python thuần, hiện thực trực tiếp công thức, được kiểm chứng lại bằng `networkx.pagerank`. mrjob có hai nhánh: *core* delegate cho reference engine, *MapReduce* chạy job thật. PySpark RDD join adjacency với rank mỗi vòng và reduce; PySpark DataFrame diễn đạt quan hệ để Catalyst tối ưu — cả hai đều cắt lineage mỗi vòng."

### Slide 11 — Họ JVM/Hadoop (≈1 phút)
"**Hadoop Streaming** nối init → map → sort → reduce qua shell. **Java MapReduce** dùng three-stage driver với Hadoop counters theo dõi Δ, khởi tạo rank = 1.0 nhờ tự chuẩn hóa. **Apache Pig** dùng ba script Pig Latin (init, iterate, sort). Tái phân phối dangling mass trong một lượt MapReduce cần thêm bước gộp toàn cục — em để đó làm future work."

### Slide 12 — Phương pháp thực nghiệm (≈1.5 phút)
"**Tập dữ liệu:** đồ thị Barabási–Albert (m = 3, seed cố định) — phân phối bậc lũy thừa giống đồ thị thật. Cộng thêm đồ thị 10 đỉnh và đồ thị có dangling để kiểm tra tính đúng. **Ground truth:** networkx.pagerank với tol = 10⁻¹². **Chỉ số:** sai số tuyệt đối cực đại, Δ mỗi vòng, thời gian, bộ nhớ. **Môi trường:** container Linux đơn nút, mỗi cấu hình chạy 3 lần báo cáo mean ± std."

### Slide 13 — Kết quả: tính đúng đắn (≈1.5 phút)
"Reference engine khớp NetworkX ở mức **10⁻¹²** trong cả ba biến thể — xác nhận em cài công thức đúng. Các phiên bản phân tán khớp Core ở mức **10⁻⁹ đến 10⁻⁸** — đúng bằng *sàn precision* do em lưu rank với 8 chữ số thập phân. Vậy mọi phiên bản đang thật sự tính ra cùng kết quả, đến mức precision của output. Chỉ sau khi xác lập điều này, em mới bắt đầu so sánh hiệu năng."

### Slide 14 — Kết quả: hội tụ (≈1 phút)
"Đường delta L1 giảm đơn điệu theo vòng lặp, và cắt ngưỡng ε = 10⁻³ ở vòng thứ 13 trên đồ thị minh họa. Mọi phiên bản có cùng dáng đường vì cùng O(|E|)/vòng. Khác biệt giữa chúng nằm ở *hằng số* mỗi vòng, không phải ở thuật toán."

### Slide 15 — Kết quả: hiệu năng đơn nút (≈2 phút)
"Bảng này cho thấy thời gian và bộ nhớ của bốn framework chạy được đơn nút. **Hai engine in-memory nhanh nhất**, scale gần tuyến tính theo số cạnh. **mrjob-core** thêm overhead nhỏ từ runner, giảm dần theo cỡ đồ thị — từ 1.7× ở N=10³ xuống ≈1× ở N=10⁴. **Hadoop Streaming** chậm 8–14× vì phải spawn process và tuần tự hóa lại trạng thái mỗi vòng. **mrjob MapReduce** chậm hơn nữa, 44–49× — vì nó *launch một job MapReduce mới cho mỗi trong 30 vòng lặp*: đây chính là chi phí điều phối thuần, không phải tính toán."

### Slide 16 — Speedup & bộ nhớ (≈1.5 phút)
"Speedup so với Core cho thấy mọi phiên bản phân tán đều dưới 1 — overhead framework chi phối ở quy mô đơn nút này. Về bộ nhớ thì ngược lại: **Hadoop Streaming dùng ít nhất (~10 MB)** vì nó streaming từng bản ghi; các engine in-memory phải giữ toàn đồ thị nên tốn hơn. Đây là *đánh đổi thời gian–bộ nhớ* kinh điển: ai chạy nhanh thì tốn bộ nhớ, ai tiết kiệm bộ nhớ thì chậm."

### Slide 17 — Bài học kỹ thuật (≈2 phút)
"Phần em thấy giá trị nhất với người làm thực tế. Bốn lỗi xuyên framework: (1) *Lineage của Spark phải bị cắt* — self-join mỗi vòng làm query plan phình vô hạn, driver hết heap khi *chỉ đang dựng plan*; cache không đủ, phải dùng localCheckpoint. (2) *Đường dẫn file://* — Spark phân giải relative path theo HDFS chứ không phải đĩa local. (3) *RDD.localCheckpoint() trả None* — nó đánh dấu RDD tại chỗ, khác với DataFrame; gán lại biến tạo dataset null âm thầm. (4) *mrjob phụ thuộc distutils* — đã bị bỏ ở Python 3.12. Không lỗi nào là lỗi thuật toán, nhưng mỗi lỗi có thể âm thầm làm sai kết quả trong sản xuất."

### Slide 18 — Chọn framework nào (≈1.5 phút)
"Khuyến nghị thực tế: **đồ thị vừa bộ nhớ thì hiện thực in-memory đơn giản và nhanh nhất**. Framework phân tán chỉ được biện minh bởi *khối lượng dữ liệu, khả năng chịu lỗi và tích hợp cluster sẵn có*, không phải tốc độ đơn nút. Trong các framework phân tán: DataFrame ngắn gọn nhất và hưởng lợi Catalyst, RDD kiểm soát tường minh nhất, MapReduce/Streaming/Pig phù hợp khi Hadoop đã là chuẩn vận hành. Nguyên tắc chủ đạo: **kiểm chứng trước, so sánh sau**."

### Slide 19 — Hạn chế & hướng phát triển (≈1 phút)
"Trung thực mà nói: đồ án này đo *đơn nút*, nên đo *overhead* chứ chưa đo *khả năng mở rộng phân tán*. PySpark, Java, Pig đã kiểm chứng đúng đắn nhưng chưa đo ở quy mô cluster. Hướng phát triển: nghiên cứu đa nút trên đồ thị 10⁶–10⁷ cạnh (như SNAP datasets), báo cáo strong/weak scaling cho Spark; tái phân phối dangling mass cho họ JVM 1-pass; thêm Topic-Sensitive PageRank; so với GraphX."

### Slide 20 — Kết luận + Q&A (≈1 phút)
"Tóm lại: em đã trình bày một nghiên cứu so sánh một-codebase, neo bởi reference engine + oracle NetworkX, chứng minh các phiên bản tương đương số học, rồi so sánh hiệu năng theo *cùng môi trường*, và tổng hợp một catalogue lỗi kỹ thuật. Nguyên tắc xuyên suốt là **kiểm chứng trước, so sánh sau**. Em hy vọng đồ án hữu ích cho người chọn framework cho bài toán đồ thị tương tự. Em xin cảm ơn và sẵn sàng nhận câu hỏi."

> **Tổng thời gian ước tính:** ~22 phút. Tăng/giảm tốc độ ở slide 17 (bài học) và slide 8–11 (mô hình & triển khai) để tinh chỉnh trong khung cho phép.

---

## Phần B — Bộ câu hỏi Q&A có thể gặp

### Câu hỏi kỹ thuật

**Q1. Tại sao phải có reference engine bằng Python thuần?**
A: Để có *ground truth* không phụ thuộc gói nào, dễ kiểm tra/debug, làm oracle so sánh tất cả framework khác. Nếu không có nó, mỗi framework chỉ so với chính nó hoặc với một engine có thể đã sai.

**Q2. Dangling node là gì và tại sao quan trọng?**
A: Là đỉnh không có liên kết ra. Trong PageRank, nó làm rò rỉ khối xác suất khỏi hệ, khiến Σ PR < 1 nếu không xử lý. Khắc phục: tái phân phối khối lượng của các dangling qua vector teleport p(u).

**Q3. "Kiểm chứng trước, so sánh sau" có ý nghĩa gì?**
A: Trước khi so sánh thời gian/bộ nhớ giữa các framework, phải chứng minh chúng đang ra cùng kết quả. Nếu không, chênh lệch thời gian có thể do tính *sai* chứ không phải overhead — đo cũng vô nghĩa.

**Q4. Tại sao mrjob MapReduce chậm hơn mrjob core 44–49 lần?**
A: Vì nó khởi chạy một job MapReduce *mới* cho mỗi trong 30 vòng lặp. Chi phí spawn process + setup mrjob inline runner mỗi vòng chiếm phần lớn thời gian — không phải tính toán O(|E|). mrjob-core thì chỉ delegate cho engine in-memory, nên overhead bị triệt tiêu dần.

**Q5. Tại sao Hadoop Streaming dùng ít bộ nhớ nhất?**
A: Vì nó *streaming* — xử lý từng bản ghi, sort qua đĩa, không giữ toàn đồ thị trong một address space. Đổi lại, mỗi vòng phải tuần tự hóa lại trạng thái qua đĩa → chậm. Đây là minh họa rõ ràng cho đánh đổi *time–memory*.

**Q6. localCheckpoint của Spark giải quyết vấn đề gì?**
A: Spark đánh giá lười (lazy). Chuỗi self-join mỗi vòng làm logical/physical plan phình vô hạn, driver eventually hết heap *khi chỉ đang render plan*. `cache()` không đủ vì cache không cắt lineage. `localCheckpoint()` vật chất hóa kết quả mỗi vòng và *cắt lineage* — plan không phình nữa.

**Q7. Khác biệt giữa PySpark RDD và DataFrame?**
A: RDD là API low-level: mọi biến đổi tường minh, kiểm soát chi tiết. DataFrame là quan hệ, ngắn gọn hơn, được tối ưu bởi Catalyst. Ở quy mô đơn nút trong đồ án này, DataFrame nhanh hơn nhiều RDD vì checkpoint của DataFrame (eager=True) rẻ hơn checkpoint của RDD (tốn ~59s/vòng so với ~3s).

**Q8. Self-normalization có ý nghĩa gì khi cài đặt?**
A: Vì tổng PR luôn hội tụ về 1 bất kể vector khởi tạo, bản Java có thể init rank = 1.0 thay vì 1/N mà vẫn ra đúng kết quả — đơn giản hóa code, chỉ tốn thêm vài vòng "thừa" cho tổng tự chuẩn hóa về 1.

**Q9. Vì sao catalogue 4 lỗi kỹ thuật được coi là một *đóng góp*?**
A: Vì chúng là lỗi xuyên framework, không phải lỗi thuật toán. Compiler/runtime không cảnh báo, nhưng kết quả có thể bị sai âm thầm trong sản xuất. Tài liệu hóa nguyên nhân gốc + cách sửa giúp người sau không lặp lại — đó là giá trị thực tế của nghiên cứu so sánh.

### Câu hỏi phương pháp luận

**Q10. Tính tái lập đảm bảo bằng cách nào?**
A: Reference engine không phụ thuộc gói lạ; Docker image cho môi trường chuẩn; CI pipeline build/test/lint mỗi commit; harness điều khiển bằng một file cấu hình; hình được tái sinh từ CSV bằng `tools/visualize.py`. Mọi số liệu trong paper là *đo thật*, không bịa.

**Q11. Vì sao chọn Barabási–Albert làm tập dữ liệu?**
A: Vì phân phối bậc lũy thừa (power-law) mô phỏng đặc tính đồ thị web/MXH thực — vốn có một số ít đỉnh "hub" và rất nhiều đỉnh ít liên kết. Sinh với seed cố định nên hoàn toàn tái lập được.

**Q12. ε = 10⁻³ có hợp lý không?**
A: Đủ chặt cho mục đích so sánh framework: tổng L1 thay đổi mỗi vòng nhỏ hơn 0.001, tức điểm số gần như ổn định. Có thể chặt hơn (1e-6) nhưng tốn thêm vòng lặp — và *cùng ε giữa các phiên bản* mới là điều quan trọng để so sánh công bằng.

**Q13. Tại sao mỗi cấu hình lặp 3 lần?**
A: Để báo cáo *phương sai*. Một lần chạy có thể nhiễu do scheduler/GC; lặp 3 lần và báo mean ± std cho thấy độ ổn định. Std nhỏ → kết quả đáng tin. Đây là độ chặt thống kê tối thiểu.

**Q14. Tại sao không đo trên cluster?**
A: Đây là hạn chế cốt lõi đã nêu trong Limitations — đồ án nằm trong khuôn khổ đơn nút. Harness đã sẵn sàng cho đa nút (chỉ cần spark-submit trên cluster); future work sẽ đo trên SNAP graphs 10⁶–10⁷ cạnh và báo cáo strong/weak scaling.

### Câu hỏi mở rộng

**Q15. PageRank có thể áp dụng ngoài web search không?**
A: Có. Ứng dụng phổ biến: gợi ý sản phẩm (đồ thị user–item), phát hiện gian lận (đồ thị giao dịch), phân tích trích dẫn học thuật, xếp hạng từ khóa (TextRank), ảnh hưởng trong mạng xã hội. Bất kỳ bài toán *ranking trên đồ thị* nào đều có thể áp dụng.

**Q16. Spark có ưu thế khi nào?**
A: Khi dữ liệu *vượt bộ nhớ một nút* và cần đệm trong RAM qua nhiều vòng lặp. Ở đơn nút, chi phí JVM startup + planning + per-iteration checkpoint vượt xa lợi ích. Vùng "thắng" của Spark bắt đầu khi nó cần một cluster thật và dữ liệu đủ lớn để khấu hao chi phí cố định.

**Q17. Java MapReduce và Pig vẫn để future work — vậy có vào paper làm gì?**
A: Để liệt kê đầy đủ "ecosystem" Hadoop và đảm bảo đồ án phủ phạm vi đã hứa. Cài đặt và kiểm chứng *tính đúng đắn* đã làm; chỉ chưa đo *thời gian* vì cần Hadoop cluster. Có mặt giúp paper hoàn chỉnh về phạm vi và làm rõ giới hạn (không che đậy).

---

## Mẹo thuyết trình

- Mỗi slide nói **không quá 1.5–2 phút**; vượt thì cắt câu, đừng đọc nguyên slide.
- Ở slide 5 (công thức): chỉ vào công thức trên slide, đừng đọc từng ký hiệu — giải thích *ý nghĩa* (random surfer, teleport, dangling).
- Ở slide 13 và 15 (bảng số): chỉ nhấn 1–2 *con số quan trọng* (10⁻¹², 44–49×), đừng đọc cả bảng.
- Ở slide 17 (4 lỗi kỹ thuật): đây là slide *gây ấn tượng nhất*; chậm lại, kể như câu chuyện ("em từng debug N giờ vì lỗi này…").
- Khi nhận câu hỏi: nhắc lại câu hỏi → trả lời ngắn (1–2 câu chính) → bổ sung nếu được hỏi thêm.

---

## Phần C — Bổ sung sau khi paper cập nhật & công bố trên Zenodo

> Slide hiện tại trình bày **số sandbox 4 framework**. Paper bản cuối đã mở rộng lên **VM 8-vCPU 5 framework**
> (thêm PySpark DataFrame). Khi thuyết trình, bạn có thể đề cập phần PySpark như "bonus finding của bản
> paper mới nhất" ở slide 15 và 19 mà không cần đổi slide.

### C.1. Cập nhật slide 1 (~10 giây thêm vào)
Thêm một câu cuối: *"Đồ án đã được công bố trên Zenodo với DOI **10.5281/zenodo.20383459** —
mã nguồn, dữ liệu và paper PDF đều public, ai cũng có thể tải về và chạy lại để xác minh."*
Điều này tạo ấn tượng *đã công bố chính thức*, không chỉ là báo cáo lớp.

### C.2. Talking point cho slide 15 (sau khi nói về mrjob MapReduce)
*"Trong bản paper mới em vừa công bố, em đã mở rộng đo thêm **PySpark DataFrame** trên một VM 8 lõi với
Spark 4.1.1. Kết quả khá ấn tượng: DataFrame chậm hơn engine in-memory **190 tới 1000 lần** ở các cỡ đơn nút
này, và quan trọng hơn — nó dùng **~1.6 GB bộ nhớ JVM**, gấp ~100 lần các engine Python (chỉ 14–52 MB).
Còn PySpark RDD em đo riêng một điểm: **30 phút cho một run ở N = 10³** — vì mỗi vòng `localCheckpoint`
phải materialize ra đĩa, tốn ~59 giây/vòng so với ~3 giây của DataFrame. Đây là minh họa cực rõ: ở đơn nút,
chi phí JVM startup + query planning + checkpoint của Spark **lấn át hoàn toàn** O(|E|) thực sự."*

### C.3. Talking point cho slide 19 (sửa câu "PySpark…chưa đo")
*"Bản paper mới nhất đã đo PySpark đơn nút như em vừa nói; **giới hạn còn lại là Java MapReduce và Apache Pig**
— hai cái này cần Hadoop cluster thật mới đo có ý nghĩa, em để vào future work."*

### C.4. Q&A bổ sung về PySpark VM

**Q18. PySpark chậm gấp 1000 lần — vậy nó có dùng được không?**
A: Ở **đơn nút** thì không nên dùng — em đo và khẳng định điều đó. PySpark được thiết kế cho **cluster +
dữ liệu lớn**. Vùng nó "thắng" là khi: (a) dữ liệu vượt RAM một máy, (b) đã có Hadoop/Spark cluster, (c) cần
fault tolerance. Đo của em chỉ ra rõ ràng *vùng nào không nên dùng* — đó cũng là một kết luận có giá trị.

**Q19. Tại sao PySpark dùng 1.6 GB bộ nhớ trong khi đồ thị chỉ ~30 KB?**
A: 1.6 GB là **JVM heap mặc định** của Spark driver + executor (Spark khởi tạo heap lớn để sẵn sàng cho
workload lớn), không phải kích thước dữ liệu. Đây là chi phí cố định: dù đồ thị 10³ hay 10⁵ đỉnh, footprint
JVM gần như không đổi. Đó cũng là lý do Spark *không hợp* cho jobs nhỏ — chi phí khởi tạo overhead vượt
xa lợi ích.

**Q20. RDD chậm hơn DataFrame 20 lần, tại sao?**
A: Cả hai cùng cắt lineage bằng `localCheckpoint`, nhưng cơ chế khác. **RDD localCheckpoint** phải
materialize toàn bộ RDD ra đĩa cục bộ mỗi vòng — tốn I/O lớn. **DataFrame localCheckpoint(eager=True)** tận
dụng Catalyst biết schema và planning, hiệu quả hơn nhiều. Trên VM em đo được RDD ~59 s/vòng so với
DataFrame ~3 s/vòng. Đây là quan sát thực nghiệm trên Spark 4.1.1 đơn nút — không khẳng định tổng quát cho
mọi cấu hình cluster.

**Q21. Bài đã công bố ở Zenodo có giá trị bằng peer review không?**
A: Zenodo là **open-access preprint** có DOI vĩnh viễn, được nhiều tổ chức (CERN, OpenAIRE, EU) công nhận
như một kênh công bố hợp lệ — nhưng *không qua peer review*. Em chọn vì mục tiêu chính là **công khai và
trích dẫn được**. Để có peer review (hội nghị IEEE/ACM), em cần mở rộng đa nút trước — đó là roadmap em
đề xuất trong phần future work.
