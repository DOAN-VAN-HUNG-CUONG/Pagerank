# Đánh giá paper & Hướng dẫn công bố

**Paper:** *A Reproducible Comparative Study of PageRank Across the Hadoop Ecosystem*
**Tác giả:** Doan Van Hung Cuong (FPT University)
**Phiên bản đánh giá:** preview build 6 trang (IEEE conference format)
**Ngày soạn:** 2026-05-23

> Tài liệu này gồm 3 phần: (A) đánh giá trung thực chất lượng paper, (B) khuyến nghị nơi công bố
> phù hợp với mục tiêu *công khai, phạm vi quốc tế*, và (C) hướng dẫn từng bước cách công bố.

---

## A. Đánh giá paper

### A.1. Nhận định tổng quan

Paper là một **nghiên cứu so sánh có tính tái lập cao (reproducibility study)**, được viết trung thực,
trình bày sạch, đúng chuẩn IEEE. Giá trị cốt lõi nằm ở *quy trình* và *tính tái lập*, không phải ở
một kết quả khoa học mới. Đây là một đồ án kỹ thuật tốt, nhưng **chưa phải một đóng góp nghiên cứu
mạnh** theo tiêu chuẩn hội nghị/tạp chí có phản biện, chủ yếu vì phạm vi đo còn hẹp.

### A.2. Điểm mạnh

1. **Tính tái lập (reproducibility).** Reference engine không phụ thuộc, Docker image, CI pipeline,
   harness điều khiển bằng cấu hình, hình được tái sinh từ CSV. Đây là tài sản lớn nhất của paper và
   là điều nhiều bài báo "nặng kết quả" còn thiếu.
2. **Phương pháp "kiểm chứng trước, so sánh sau".** Thiết lập tương đương số học so với oracle độc lập
   (NetworkX) *trước khi* so sánh hiệu năng. Đúng về mặt phương pháp luận và có giá trị sư phạm.
3. **Phạm vi trung thực.** Nêu rõ chỉ đo đơn nút; PySpark/Java/Pig được phân tích kiến trúc và xếp vào
   future work; không có số liệu bịa. Tính chính trực cao.
4. **Catalogue lỗi kỹ thuật xuyên framework.** Bốn lỗi cụ thể (lineage của Spark, đường dẫn `file://`,
   `RDD.localCheckpoint()` trả `None`, mrjob phụ thuộc `distutils`) là phần *hữu ích nhất với người làm
   thực tế* và hiếm khi được viết lại bài bản. Đây là điểm khác biệt đáng giá nhất.
5. **Trình bày & nền tảng toán.** Công thức tổng quát (chuẩn/trọng số/cá nhân hóa), xử lý dangling-mass,
   tính tự chuẩn hóa; 12 tài liệu tham khảo thật; văn phong IEEE chuẩn.

### A.3. Điểm yếu / hạn chế (trung thực)

1. **Chưa đo được khả năng mở rộng phân tán — đây là hạn chế cốt lõi.** Tiêu đề hứa hẹn so sánh
   *hiệu năng/scalability* của các framework, nhưng đo đạc chỉ ở **đơn nút** nên thực chất chỉ đo
   *overhead*, không đo *scalability*. Đúng vùng mà Spark/Hadoop phát huy (dữ liệu vượt bộ nhớ một nút)
   lại chính là vùng *chưa* được đo.
2. **Chỉ 3/8 bản triển khai được benchmark** (Core, mrjob-core, Hadoop Streaming). PySpark, Java, Pig
   mới được kiểm chứng tính đúng đắn, *chưa đo thời gian* → phần "so sánh" chưa trọn vẹn so với tiêu đề.
3. **Cỡ đồ thị nhỏ** (N = 10³–10⁴). Còn xa "big data". Ở quy mô này, kết luận "in-memory thắng" gần như
   hiển nhiên và ít tính mới.
4. **Tính mới hạn chế.** So sánh PageRank trên các framework đã tồn tại trong tài liệu. Đóng góp thiên
   về *sư phạm + tái lập* hơn là khoa học mới — đây là rào cản lớn nhất nếu nhắm venue phản biện.
5. **Độ chặt thống kê.** Số liệu dường như từ một lần chạy (hoặc số lần lặp không nêu rõ); thiếu
   phương sai/khoảng tin cậy, thiếu thông số phần cứng chi tiết ngoài "CI container". Với tuyên bố về
   hiệu năng, cần lặp nhiều lần + báo cáo phương sai.
6. **Một môi trường duy nhất.** Chưa khảo sát độ nhạy theo phần cứng, JVM heap, số partition Spark, v.v.

> Lưu ý: hạn chế (6) không phải lỗi khoa học — single author/độc lập là bình thường — nhưng ảnh hưởng
> tới *logistics* công bố (ví dụ yêu cầu endorsement của arXiv, xem phần C).

### A.4. Phản biện có thể gặp (nếu nộp venue phản biện)

- "Đo đơn nút không đủ cơ sở cho các tuyên bố về framework phân tán."
- "Quy mô quá nhỏ để liên quan tới hệ thống big-data."
- "Tính incremental; các nghiên cứu so sánh PageRank đã có."
- "Thiếu lặp lại/phương sai; phương pháp đo hiệu năng chưa đủ chi tiết."
- (Tích cực) Tính tái lập và catalogue lỗi kỹ thuật sẽ được đánh giá cao.

### A.5. Mức độ sẵn sàng theo loại venue

| Loại venue | Mức sẵn sàng | Ghi chú |
|---|---|---|
| **Preprint** (arXiv, Zenodo, TechRxiv, OSF) | ✅ **Sẵn sàng ngay** | Trung thực, viết tốt, tái lập được — đúng định dạng cho preprint. |
| **Workshop / reproducibility / student track** | 🟡 Gần sẵn sàng | Nên bổ sung đa nút + phương sai trước khi nộp. |
| **Hội nghị/tạp chí phản biện (IEEE/ACM)** | 🔴 Chưa sẵn sàng | Cần mở rộng đa nút, đồ thị lớn (10⁶–10⁷ cạnh, SNAP), đo đủ 8 bản, báo cáo phương sai, và làm sắc lại tính mới. |

### A.6. Nếu bạn mở rộng thí nghiệm (bạn đã chọn "có thể mở rộng")

Để paper đủ sức cạnh tranh ở venue phản biện, ưu tiên theo thứ tự:

1. **Chạy đa nút** trên cụm Spark/Hadoop thật, đồ thị SNAP (web-Google, LiveJournal, ~10⁶–10⁷ cạnh).
2. **Đo đủ 8 bản triển khai** (đặc biệt PySpark RDD/DataFrame, Java, Pig) cùng điều kiện.
3. **Báo cáo strong/weak scaling** cho Spark + **lặp ≥ 5 lần**, ghi trung bình ± độ lệch chuẩn.
4. **Tái định khung đóng góp**: lấy *scalability thực sự* + *catalogue lỗi kỹ thuật* làm trung tâm.

Preprint cho phép cập nhật phiên bản (v1 bây giờ → v2 sau khi mở rộng), nên bạn có thể công khai ngay
mà không phải chờ.

---

## B. Chọn nơi công bố

**Bối cảnh mục tiêu của bạn:** công khai, phạm vi quốc tế, không bắt buộc phản biện, sẵn sàng mở rộng.
→ Ưu tiên các kênh **preprint/repository quốc tế, miễn phí, cấp DOI, không qua phản biện**.

### B.1. So sánh các kênh phù hợp

| Kênh | Rào cản | DOI | Phù hợp | Lưu ý |
|---|---|---|---|---|
| **Zenodo** (CERN/OpenAIRE) | Không | ✅ | **Khuyến nghị #1** | Không cần endorsement; lưu cả PDF *và* snapshot mã nguồn; tích hợp GitHub. |
| **arXiv** (`cs.DC`) | ⚠️ Cần *endorsement* (2026 siết) | Không (có arXiv ID) | Khuyến nghị #2 | Uy tín cao nhất; cần người bảo trợ — xem C.2. |
| **TechRxiv** (IEEE) | Thấp (kiểm duyệt) | ✅ | Hợp định dạng IEEE | Có thể đang tạm đóng để chuyển nền tảng — kiểm tra trước. |
| **OSF Preprints** | Không | ✅ | Phương án thay thế tốt | Miễn phí, không endorsement. |
| **GitHub Release** | Không | (qua Zenodo) | Bắt buộc nên làm | Bạn đã có repo; gắn tag + PDF + `CITATION.cff`. |

### B.2. Khuyến nghị (lộ trình thực hiện)

1. **Zenodo — kênh chính.** Tải PDF paper lên dưới dạng *Publication → Preprint* (được cấp DOI), và
   bật tích hợp **Zenodo ↔ GitHub** để mỗi *release* của repo được lưu trữ kèm DOI riêng. Kết quả: một
   bản ghi quốc tế, trích dẫn được, tái lập được — **không có rào cản**.
2. **GitHub Release + `CITATION.cff`.** Tạo release `v1.0.0`, đính kèm `paper/main.pdf`. File
   `CITATION.cff` (đã tạo sẵn ở gốc repo) giúp GitHub hiện nút "Cite this repository" và cấp metadata
   cho Zenodo.
3. **arXiv (tùy chọn, nếu xin được endorsement).** Là chuẩn vàng preprint quốc tế. Người bảo trợ tự
   nhiên nhất là **giảng viên hướng dẫn** của bạn nếu họ từng đăng arXiv trong `cs.DC` — xem C.2.
4. **OSF/TechRxiv** chỉ dùng nếu muốn thêm kênh; tránh đăng trùng cùng một DOI ở nhiều nơi gây nhiễu.

> Mẹo trích dẫn: nếu lấy DOI Zenodo trước, hãy thêm dòng DOI đó vào trang đầu paper (`\thanks{...}` hoặc
> footnote) rồi mới khóa bản PDF cuối — để bản công bố tự tham chiếu được.

---

## C. Hướng dẫn chi tiết cách công bố

### C.0. Chuẩn bị trước (checklist)

- [ ] Biên dịch bản nộp `paper/main.pdf` bằng **IEEEtran** (xem C.1) — kiểm tra 0 lỗi, 0 ref/cite chưa định nghĩa.
- [ ] Chọn **giấy phép**: khuyến nghị **CC BY 4.0** cho paper (cho phép chia sẻ + ghi nguồn).
- [ ] Tạo **ORCID iD** miễn phí tại <https://orcid.org> (định danh tác giả; nhiều nền tảng yêu cầu).
- [ ] Hoàn thiện metadata: tiêu đề, abstract, từ khóa, phân loại chủ đề.
- [ ] Đảm bảo repo GitHub có `LICENSE` và `CITATION.cff` (đã tạo) ở thư mục gốc.

### C.1. Biên dịch bản PDF nộp (IEEEtran)

Sandbox không có `IEEEtran.cls`; hãy build trên máy bạn (đã cài TeX Live đầy đủ) hoặc Overleaf:

```bash
# Trên máy có TeX Live (gói texlive-publishers chứa IEEEtran)
cd paper
make -C .. paper          # hoặc: latexmk -pdf main.tex
# Kết quả: paper/main.pdf  (định dạng IEEE 2 cột)
```

Nếu không cài được local: tạo dự án trên **Overleaf** (miễn phí), upload `main.tex`, `content.tex`,
`abstract.tex`, thư mục `figures/`, chọn compiler **pdfLaTeX**, bấm Recompile, rồi Download PDF.

### C.2. arXiv (tùy chọn, uy tín cao — nhưng có rào cản 2026)

**Chính sách endorsement mới (từ 21/01/2026):** người nộp lần đầu cần **một trong hai**:
- *Đường 1:* email học thuật **VÀ** đã là đồng tác giả của một bài arXiv trong cùng nhóm chủ đề; hoặc
- *Đường 2:* **endorsement cá nhân** từ một tác giả arXiv đã có uy tín trong cùng nhóm chủ đề.

Bạn có email học thuật FPT (`...@fpt.edu.vn`) — điều này giúp định danh, nhưng theo chính
sách 2026, **email học thuật một mình KHÔNG đủ**: Đường 1 còn cần một bài arXiv trước đó.
Vì bạn chưa có bài arXiv nào, bạn nhiều khả năng vẫn phải đi **Đường 2** (xin endorsement).
Các bước:

1. Tạo tài khoản tại <https://arxiv.org> (dùng email + ORCID).
2. Bắt đầu submission mới → chọn category **chính `cs.DC`** (Distributed, Parallel, and Cluster
   Computing); cross-list thêm **`cs.PF`** (Performance) và có thể **`cs.DB`**.
3. Hệ thống sẽ yêu cầu **mã endorsement**. Gửi link endorsement cho một tác giả `cs.DC` đủ điều kiện
   (ví dụ giảng viên hướng dẫn, đồng nghiệp đã đăng arXiv) để họ bảo trợ.
4. **Upload nguồn LaTeX** (khuyến nghị, KHÔNG chỉ PDF): nén `main.tex` + `content.tex` + `abstract.tex`
   + `figures/*.pdf` thành `.zip`/`.tar.gz`. arXiv sẽ tự biên dịch (đảm bảo biên dịch sạch trước).
5. Chọn **license** (CC BY 4.0 nếu muốn mở nhất).
6. Điền metadata (title, authors, abstract, comments ví dụ "6 pages, 5 figures").
7. Submit → qua **moderation** (vài ngày). Nếu được nhận, bài có arXiv ID + trang abstract công khai.

> Nếu không xin được endorsement, **bỏ qua arXiv** và dùng Zenodo/OSF — không ảnh hưởng mục tiêu
> "công khai quốc tế".

### C.3. Zenodo — kênh chính (không rào cản, có DOI)

**(a) Đăng PDF paper như một preprint có DOI**

1. Đăng nhập <https://zenodo.org> (có thể "Log in with GitHub" hoặc ORCID).
2. **New upload** → kéo thả `paper/main.pdf`.
3. Resource type: **Publication → Preprint**.
4. Điền metadata: Title, Authors (kèm ORCID), Description (dán abstract), Keywords, Language = English.
5. License: **Creative Commons Attribution 4.0 International (CC BY 4.0)**.
6. (Tùy chọn) "Related/alternate identifiers": thêm link GitHub repo.
7. **Publish** → Zenodo cấp **DOI** ngay (ví dụ `10.5281/zenodo.XXXXXXX`). DOI này trích dẫn được vĩnh viễn.

**(b) Lưu trữ mã nguồn kèm DOI (tích hợp GitHub)**

1. Vào <https://zenodo.org> → menu hồ sơ → **GitHub**.
2. **Log in with GitHub** → Authorize Zenodo.
3. Gạt nút **On** cạnh repo `pagerank`.
4. Trên GitHub, tạo **release mới** (xem C.4). Zenodo tự tải snapshot repo và cấp **DOI riêng cho mã nguồn**.
5. Zenodo đọc metadata từ `LICENSE`, `CITATION.cff`, `.zenodo.json` ở gốc repo (bạn đã có `CITATION.cff`).

> Lưu ý: phải **bật repo trên Zenodo TRƯỚC khi tạo release** thì release đầu mới được bắt; không thể
> gán DOI cho release đã tạo trước đó.

### C.4. GitHub Release + CITATION.cff

```bash
cd /Users/hungcuong/Downloads/pagerank
git add CITATION.cff docs/PUBLISHING_GUIDE.md
git commit -m "docs: add CITATION.cff and publishing guide"
git push

# Tạo tag + release (cần GitHub CLI 'gh', hoặc tạo qua giao diện web Releases)
git tag -a v1.0.0 -m "PageRank comparative study v1.0.0"
git push origin v1.0.0
gh release create v1.0.0 paper/main.pdf --title "v1.0.0" \
   --notes "Reproducible comparative study of PageRank across the Hadoop ecosystem."
```

Nếu không dùng `gh`: vào tab **Releases** trên GitHub → **Draft a new release** → chọn tag `v1.0.0` →
kéo `main.pdf` vào phần assets → **Publish release**. Sau khi publish, Zenodo (đã bật ở C.3b) sẽ tự lưu trữ.

Sau khi push `CITATION.cff`, GitHub hiển thị nút **"Cite this repository"** ở sidebar repo.

### C.5. Sau khi công bố

- Thêm **DOI badge** vào `README.md`: `[![DOI](https://zenodo.org/badge/DOI/<DOI>.svg)](https://doi.org/<DOI>)`.
- Thêm bài vào hồ sơ **ORCID** và (tùy chọn) **Google Scholar**, **ResearchGate**.
- Nếu sau này mở rộng đa nút: tạo `v2` trên Zenodo (giữ chung "concept DOI", thêm phiên bản mới).

---

## Phụ lục: lựa chọn giấy phép nhanh

| Mục đích | Giấy phép khuyến nghị |
|---|---|
| Paper (PDF) | **CC BY 4.0** — chia sẻ tự do, yêu cầu ghi nguồn |
| Mã nguồn (repo) | **MIT** hoặc **Apache-2.0** — phổ biến, dễ tái sử dụng |

> Lưu ý: tài liệu này là hướng dẫn quy trình, không phải tư vấn pháp lý. Quy trình arXiv/Zenodo có thể
> thay đổi — hãy kiểm tra trang trợ giúp chính thức trước khi nộp.

---

## Phụ lục B: Zenodo chi tiết + metadata sẵn dùng

Hai DOI nên lấy: một cho **mã nguồn** (qua GitHub release), một cho **bài PDF** (preprint).

### B.0. Chuẩn bị
1. Tạo **ORCID** miễn phí tại <https://orcid.org/register> (đăng ký bằng email FPT) → lấy ID `0000-000X-XXXX-XXXX`.
2. Build `paper/main.pdf` trên VM: `make paper`.
3. Điền ORCID vào dòng comment trong `CITATION.cff`.

### B.1. DOI cho mã nguồn (GitHub → Zenodo)
1. <https://zenodo.org> → **Log in with GitHub** → **Authorize zenodo**.
2. Menu hồ sơ → **GitHub** → tìm repo **`Pagerank`** → gạt **On** (phải bật TRƯỚC khi tạo release).
3. GitHub → tab **Releases** → **Draft a new release**: tag `v1.0.0` (Create new tag on publish), title `v1.0.0`,
   đính kèm `paper/main.pdf` → **Publish release**.
4. Sau ~1 phút, Zenodo tự lưu bản ghi repo và cấp **DOI cho code** (`10.5281/zenodo.XXXXXXX`). Edit metadata nếu cần → **Publish**.

### B.2. DOI cho bài PDF (Preprint) — metadata copy-paste
Zenodo → **New upload** → kéo thả `paper/main.pdf`. (Tùy chọn: **Reserve DOI** để dán vào `\thanks{}` rồi build lại trước khi upload bản cuối.)

- **Resource type:** Publication → Preprint
- **Title:**
  `A Reproducible Comparative Study of PageRank Across the Hadoop Ecosystem`
- **Authors:** `Doan, Van Hung Cuong | FPT University | <ORCID của bạn>`
- **License:** Creative Commons Attribution 4.0 International (CC-BY-4.0)
- **Language:** English
- **Keywords:** PageRank; MapReduce; Apache Spark; Apache Pig; Hadoop; reproducibility; graph processing; benchmarking
- **Related works** (tùy chọn): `is supplemented by` → URL repo GitHub và/hoặc DOI code (B.1)
- **Description / Abstract:**

> PageRank is a canonical workload for evaluating large-scale graph-processing systems, yet implementations
> written for different frameworks are rarely checked for numerical equivalence before their performance is
> compared. We present a reproducible, single-codebase study that pairs a dependency-free pure-Python reference
> engine with five Hadoop-ecosystem implementations of PageRank: Python mrjob (a local engine and a genuine
> single-pass MapReduce job), PySpark (RDD and DataFrame), Hadoop Streaming, Java MapReduce, and Apache Pig.
> The reference engine implements the standard, weighted, and personalized variants with correct dangling-node
> handling and matches networkx.pagerank to a maximum per-node error of 2.6e-12; every executed distributed
> implementation agrees with it to within 2.4e-8 (up to eight-decimal output precision), so any timing
> difference reflects engineering overhead rather than a different computation. A single configuration-driven
> harness measures wall-clock time, peak memory, and accuracy on synthetic Barabasi-Albert graphs, repeating
> each configuration three times. On a single multi-core node the two in-memory engines are fastest and scale
> near-linearly; the mrjob local engine adds only a 1.0-1.8x constant, the process-per-iteration Hadoop
> Streaming pipeline is 6-13x slower while using the least memory (~10 MB), the genuine single-pass mrjob
> MapReduce job is 36-82x slower, and PySpark's DataFrame engine is 190-1000x slower with a ~1.6 GB JVM
> footprint (its RDD counterpart slower still, ~59 s/iteration, dominated by per-iteration localCheckpoint).
> We analyze the architectural causes, document a self-normalization property, a single-pass dangling-mass
> limitation, and a Spark lineage-growth pitfall, and release the full toolchain so that cluster-scale
> evaluation of the remaining JVM frameworks is a single command. The study argues for a discipline of
> "validate first, then compare".

Bấm **Save** → kiểm tra → **Publish**. DOI cấp ngay; trang public xem PDF online được.

### B.3. Hoàn thiện
- DOI badge vào `README.md`:
  `[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.XXXXXXX.svg)](https://doi.org/10.5281/zenodo.XXXXXXX)`
- `CITATION.cff`: bỏ comment dòng `doi:` trong `preferred-citation`, điền DOI bài.

> DOI Zenodo là **vĩnh viễn**: đảm bảo PDF đã đúng (tên *Doan Van Hung Cuong*, FPT University, bảng 5 framework)
> trước khi Publish. Bản đã publish chỉ sửa được metadata; đổi file phải tạo version mới (giữ chung concept DOI).
