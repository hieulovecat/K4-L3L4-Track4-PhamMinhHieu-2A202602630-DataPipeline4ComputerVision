# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Pham Minh Hieu |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602630` |
| GitHub | `hieulovecat` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07774` |


## Kết quả cuối

- `dataset_version` của bạn: `28d3f601b324`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **736 / 155 / 128** (tổng 1019; 18 / 4 / 4 trip)
- `report/verify.txt`: `27/27 checks passed` (chạy sau `step all --reset` cuối cùng).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `crash_task_log_trip-07774.png` (log task đỏ), `crash_graph_red.png` (Graph lúc đỏ), `crash_graph_green_after_clear.png` (Graph xanh sau khi clear).
- Đầu ra từng bước và thí nghiệm lưu trong `report/step_outputs/`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt thấy ở 2 dòng đầu mỗi bảng (`report/step_outputs/step1.txt`):
1. **Tên cột**: A `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, gas, brake, steer`; B `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, throttle_pct, brake_pct, steering`.
2. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili-giây (`1759933801509`).
3. **Đơn vị tốc độ**: A m/s (`1.213`), B km/h (`55.11`).
4. **Bàn đạp**: A là tỉ lệ 0..1 (`gas 0.028`), B là phần trăm (`throttle_pct 2.0`).
5. **Đồng hồ**: B chạy nhanh hơn video **1500 ms** (`telemetry_clock_offset_ms` trong `bronze.video_files`), A là 0.
6. Số dòng: A 3021, B 3234.

Giữ riêng mỗi nguồn một bảng và để mọi cột là chữ vì bronze là **bản ghi nguyên gốc, chỉ thêm**: nếu ép kiểu ngay, một giá trị lạ (ô trống, `"abc"`, đơn vị sai) sẽ làm hỏng cả lần nạp hoặc bị biến thành NULL im lặng, và ta mất bằng chứng dữ liệu gốc trông ra sao. Hai schema khác nhau nên gộp sớm sẽ buộc phải chọn một schema trước khi hiểu dữ liệu. Để chữ thì luôn nạp được, việc chuẩn hoá dồn vào staging/silver, và khi sửa luật có thể **chạy lại từ bronze** mà không cần nguồn gửi lại. Bằng chứng chạy lại an toàn: lần nạp thứ hai báo `already present`, `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, 3021 dòng) vì file nhận diện bằng sha256.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_b.sql` đổi tên cột về schema của A, chia `spd_kmh / 3.6` ra m/s, chia pedal `/100`, và đổi `timestamp_ms` thành `timestamptz` **sau khi trừ `offset_ms`** lấy từ `bronze.video_files` (1500 ms cho drive_b). Sau đó `silver_telemetry_classified` gộp hai nguồn bằng `union all by name` và gắn `reject_reason` cho từng dòng.

Bằng chứng: dòng đầu của B thô là `…801509` ms = 14:30:**01.509**, sau chuẩn hoá `bat_dau = 2025-10-08 14:30:00.009` khớp `video_start_utc` 14:30:00 (verify: *drive B clock offset removed*). Tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), hợp lý; sạch + cách ly = nhận về: A 2968 + 53 = 3021, B 3181 + 53 = 3234.

| drive | duplicate | speed_missing | speed_out_of_range | tổng |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 |
| drive_b | 20 | 30 | 3 | 53 |

Ba dòng `speed_out_of_range` mỗi drive đều có `speed_mps = 250` (900 km/h). Dòng lỗi vào `silver_telemetry_rejects` kèm lý do thay vì bị xoá để: (1) **kiểm toán được**: đếm được, và chứng minh "sạch + cách ly = nhận về", không mất dòng nào im lặng; (2) báo ngược cho đội thiết bị biết cảm biến nào hay lỗi; (3) nếu luật sai (ví dụ ngưỡng 70 m/s quá chặt) có thể sửa luật và lấy lại dòng mà không phải nạp lại.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống > 1000 ms giữa hai mẫu) là `warn` vì mất tín hiệu cảm biến là **khuyết tật thật, đã biết và chấp nhận được**: dữ liệu còn lại vẫn đúng, các khung rơi vào khoảng trống sẽ bị loại ở bước join (`sync_ok`). Ở bài của tôi test này cho **WARN 2**: mỗi drive một khoảng trống, dài nhất 3073 ms (A) và 3124 ms (B). Dừng pipeline vì chuyện này thì không bao giờ ra được dataset. Còn `in_range` (0..70 m/s) là `error` vì một tốc độ vô lý **lọt vào silver nghĩa là luật làm sạch đã hỏng**: dữ liệu sai sẽ đi tiếp vào nhãn huấn luyện (tốc độ, đánh lái) mà không ai thấy.

Thí nghiệm `break-gate`: nới `max_speed_mps` lên 1000 thì 6 dòng 250 m/s (3 mỗi drive) không bị cách ly nữa và vào silver; `dbt test` báo `PASS=5 WARN=1 ERROR=1`, test **`in_range_silver_telemetry_speed_mps__70__0` FAIL với 6 dòng**. Khôi phục luật: `PASS=6 WARN=1 ERROR=0`. Điều đó cho thấy cổng chất lượng là **lớp phòng thủ độc lập với code biến đổi**: luật làm sạch (trong model) và kỳ vọng về dữ liệu (trong `schema.yml`) được viết riêng, nên khi ai đó sửa nhầm luật, test vẫn chặn được, và trong DAG `dbt_test_silver` lỗi thì `dbt_run_gold` và mọi task sau không chạy.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Tôi chạy lại partition **`trip-07774`** (34 khung) hai lần: `silver.frames` trước **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau hai lần chạy lại vẫn **1246 dòng**, dấu vân tay **`6c7480dfde7e`**, không đổi. Còn kiểu ghi `append` hai lần cho **68 dòng cho một trip có 34 khung**, tức nhân đôi (2x). `cli history silver.frames` thấy version 26 và 27 đều là `WRITE` 34 dòng với predicate `trip_id = 'trip-07774'`: mỗi lần chạy **thay thế** đúng phần của trip đó chứ không cộng thêm.

Delta `overwrite` với predicate là **một commit nguyên tử** trong transaction log: các file mới được ghi trước, rồi một commit duy nhất vừa đánh dấu file cũ của trip là đã xoá vừa thêm file mới. Nếu tiến trình sập giữa chừng (trước commit) thì bảng vẫn ở version cũ đầy đủ, file dở dang chỉ là rác không ai đọc. Còn "xoá rồi ghi lại" là hai thao tác: sập sau khi xoá và trước khi ghi xong thì trip đó **mất dữ liệu hoặc chỉ còn một nửa**, và người đọc đồng thời có thể thấy trạng thái trung gian. Ngoài ra Delta còn giữ lịch sử (time travel) để quay lại version trước.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**Đen**: độ sáng trung bình < 12 (`dark_luma`), tức camera hỏng hay bị che, ảnh không có thông tin nên **bị loại** (A 6, B 4). **Tối**: độ sáng 12..40 (`dark_soft`), như đường hầm, bóng râm, chiều tối: ảnh vẫn có cảnh thật nên **gắn cờ `is_dark` nhưng giữ lại** (A 4, B **61**). Tôi đồng ý: xe tự lái phải chạy được trong hầm, nếu bỏ ảnh tối thì mô hình chỉ học trời sáng và hỏng đúng lúc khó nhất. Cờ `is_dark` vẫn hữu ích để đánh giá riêng theo điều kiện ánh sáng.

Ngưỡng: với 1246 khung, ngưỡng 6 coi 363/602 khung của A là trùng (60%), quá tay; ngưỡng 0 chỉ bắt khung giống y hệt, bỏ sót camera đứng hình có nhiễu nén. Tôi giữ **ngưỡng 2** (bắt 11/12 khung camera đứng hình của A) nhưng nó **nên phụ thuộc tốc độ**: lấy mẫu 2 khung/giây, ở 10 km/h xe chỉ đi ~1,4 m giữa hai khung nên trùng là thật; ở 70 km/h xe đi ~10 m. Thực tế trong bài tôi thấy `trip-04622` chạy trung bình **67 km/h** nhưng 34/60 khung bị coi là trùng: khung `02421` và `02426` khác nhau thật (cột điện đã dịch) mà dHash chỉ lệch 1 bit vì cảnh đường cao tốc rất đơn điệu. Vì vậy tôi đề xuất chỉ coi là trùng khi `hamming ≤ 2` **và** tốc độ thấp (ví dụ < 5 km/h), hoặc so với khung *được giữ* gần nhất thay vì khung liền trước.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Với 1019 khung đáng dùng thuộc 26 trip: chia **theo khung hình** (ngẫu nhiên 70/15/15) làm **26/26 trip** nằm ở nhiều hơn một split, và **26/26 trip có khung ở cả train lẫn test**. Chia **theo trip** (gold của pipeline): **0 trip** bị chia cắt (test dbt `no_straddle` và verify *no trip is in two splits* đều đạt).

Hai khung liền nhau chỉ cách 0,5 giây: cùng con đường, cùng xe phía trước, cùng ánh sáng; chính thí nghiệm ngưỡng cho thấy 149 + 53 cặp khung liền nhau lệch ≤ 2 bit dHash. Nếu chia theo khung, test gần như chứa **bản sao** của ảnh train, nên mô hình chỉ cần nhớ cảnh là đạt điểm cao. Điểm test khi đó đo khả năng *nhớ* chứ không phải *tổng quát hoá* sang con đường mới, và sẽ tụt khi chạy thật. Chia theo trip buộc test là những cảnh mô hình chưa từng thấy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`export_dataset` tính `sha256` của danh sách `frame_id|split` đã sắp xếp, lấy 12 ký tự đầu. Nhờ vậy version **chỉ đổi khi nội dung đổi**: cùng khung, cùng split thì cùng tên, dù chạy lúc nào, ở máy nào. Ngày giờ hay số đếm thì mỗi lần chạy lại sinh version mới dù dữ liệu y hệt (không biết hai dataset có giống nhau không), và hai người chạy song song có thể trùng số đếm cho hai nội dung khác nhau.

Kết quả của tôi: `28d3f601b324` ở **mọi lần chạy**: `step all --reset` lần đầu, `step 4`, chạy lại `cli export-dataset` (cả `manifest_sha256` cũng giống hệt `3b06489d…`), sau bài tập sập/chạy lại, và `step all --reset` cuối cùng. Thử đổi MSSV sang `2A202602631`: version thành **`c8936824962d`**, split thành 705/157/157 (của tôi 736/155/128). Lý do: `gold_dataset_frames.sql` xếp trip trong mỗi drive theo `md5(trip_id || LAB_STUDENT_ID)` rồi cắt 70/15/15, nên MSSV khác thì thứ tự trip khác, trip vào split khác, danh sách `frame_id|split` khác, và băm khác. Đặt lại MSSV của tôi thì quay về đúng `28d3f601b324`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/28d3f601b324/manifest.json` (bản sao ở `report/evidence/manifest_28d3f601b324.json`):

```
"lineage": { "bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
             "silver.frames": 25, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
             "input_video_sha256": { "drive_a": "636ba1a98dfc…e7e9aa", "drive_b": "bfa62703f645…ee9fdc" } }
```

Tức là dataset này được tạo từ **hai video có đúng sha256 trên**, telemetry A/B và bảng trips ở **version 0** của bronze, `bronze.video_files` version 1, `silver.frames` version 25 (26 lần ghi, mỗi trip một lần, version 0..25), và các bảng silver telemetry/flag/join ở version 0. Manifest còn ghi `rules` (bỏ `is_black, is_blurry, is_near_dup, not sync_ok`; split theo trip, phân tầng theo drive, thứ tự md5) và `by_split` 736/155/128.

3 tháng sau, để tái tạo: (1) sha256 của video đầu vào để chắc chắn đúng file nguồn; (2) version Delta của từng bảng để **time travel** đọc đúng trạng thái lúc đó; (3) luật lọc và cách chia; (4) `manifest.parquet` với `manifest_sha256` liệt kê từng `frame_id`, `split`, `image_path`. Manifest **chưa** ghi lại commit git của code dbt, các `vars` (ngưỡng `dup_hamming=2`, `max_speed_mps=70`…) và MSSV dùng làm salt; muốn tái tạo chắc chắn thì nên thêm ba thông tin này.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Đặt `LAB_FAIL_ONCE=trip-07774`, trigger `day09_reset` rồi `day09_video_pipeline` (run `pipeline_crash`). Kết quả (`report/step_outputs/crash_states_before_clear.txt`): **1 task đỏ**, là `silver_frames` **map_index 20**, log `RuntimeError: simulated crash while extracting trip-07774; run it again` (ảnh `crash_task_log_trip-07774.png`); **7 task `upstream_failed`** (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`); **29 task success** (3 bronze, `trip_commands`, 25 `silver_frames` còn lại). Lúc đó `silver.frames` có **1212 dòng, 25 trip, 0 dòng của trip-07774**.

Tôi **chỉ clear task đỏ** (`airflow tasks clear -t ^silver_frames$ --only-failed --downstream`): 25 trip đã xanh **không chạy lại**; chỉ `silver_frames[20]` chạy lần thử 2 (`attempt=2.log`, chỉ task này có attempt 2) và 7 task phía sau chạy lần đầu, tổng **8 task**. DAG chuyển **37/37 success** (ảnh `crash_graph_green_after_clear.png`). Sau đó `silver.frames` = **1246 dòng, 26 trip, 34 dòng của trip-07774, 0 `frame_id` trùng**: không nhân đôi, vì lần thử đầu sập *trước* khi ghi và lần thử hai ghi kiểu overwrite theo `trip_id`. `scripts/lab.sh verify` vẫn **27/27**, `dataset_version` vẫn **`28d3f601b324`** (`report/step_outputs/crash_rerun.txt`).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Quy mô: hiện tại 2 drive × 5 phút = 1246 khung. 500 xe × ~8 giờ × 2 khung/giây ≈ **28,8 triệu khung/ngày** (chỉ một camera), gấp ~23 000 lần.
1. **Giải mã video (`silver_frames`)** vỡ trước: một container Airflow, `LocalExecutor` với `PARALLELISM=2`, `max_active_tis_per_dagrun=1`, FFmpeg chạy CPU tuần tự, mỗi task tải cả `raw.mp4` về đĩa tạm. Thay bằng worker phân tán (KubernetesExecutor/Celery hoặc Spark/Ray), giải mã bằng GPU (NVDEC), chỉ đọc đúng byte range của trip, partition theo `xe/ngày/trip`, và trigger theo sự kiện khi file mới đến thay vì chạy cả DAG.
2. **DuckDB một file + dbt rebuild toàn bộ**: mỗi lần chạy dựng lại mọi bảng silver/gold từ đầu trên một máy, `publish_lake` ghi `overwrite` cả bảng, `max_share`/`max_gap` quét toàn bộ dữ liệu. Thay bằng model **incremental** theo partition ngày/xe, engine phân tán đọc Delta (Spark/Trino), và test chất lượng chỉ chạy trên partition mới.
3. **Xuất dataset và phục vụ**: `export_dataset` **copy từng ảnh** vào thư mục version (mỗi version nhân đôi dung lượng), `publish_warehouse` thay toàn bộ mart trong một Postgres, và gallery có một dòng mỗi khung. Thay bằng manifest chỉ chứa con trỏ tới ảnh (không copy) hoặc công cụ version hoá dữ liệu kiểu lakeFS/DVC, và warehouse cột (BigQuery/ClickHouse) với mart tổng hợp theo ngày.

Ngoài ra ingest bằng file cục bộ + sha256 cả file cũng cần đổi sang upload trực tiếp từ xe lên object storage và hàng đợi sự kiện.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Hai câu hỏi trên dashboard *Day 9: video data pipeline* (ảnh `metabase_dashboard.png`, SQL đầy đủ ở `report/metabase_questions.sql`).

**Câu hỏi riêng 1: Trip nào bị loại nhiều khung nhất?**
```sql
select trip_id, drive_id, lighting, split, avg_speed_kmh, frames,
       frames - frames_kept as frames_dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_dropped
from marts.mart_trip_summary order by pct_dropped desc, frames_dropped desc limit 10
```
Đứng đầu là `trip-04622` (drive_a, split **test**, 67,1 km/h): **34/60 khung bị loại (56,7%)**; tiếp theo `trip-03904` 50% và `trip-02086` 46,7%; trip của tôi `trip-07774` bị loại 38,2%. Đào sâu (`report/step_outputs/metabase_question_drilldown.txt`): **toàn bộ** số khung bị loại ở 5 trip này là do `is_near_dup`, không có khung đen, mờ hay mất đồng bộ. Với các trip chậm (6,5–17,6 km/h) điều này hợp lý, nhưng `trip-04622` chạy nhanh mà vẫn bị loại nhiều: dHash 9×8 quá thô với cảnh cao tốc đơn điệu. Hệ quả vận hành: split **test** của tôi mất hơn nửa số khung của một trip cao tốc.

**Câu hỏi riêng 2 (điểm cộng): Tốc độ xe ảnh hưởng tỷ lệ loại thế nào?**
```sql
select case when avg_speed_kmh < 20 then '1. < 20 km/h' when avg_speed_kmh < 50 then '2. 20-50 km/h'
            when avg_speed_kmh < 80 then '3. 50-80 km/h' else '4. >= 80 km/h' end as speed_band,
       count(*) as trips, sum(frames) as frames, sum(frames - frames_kept) as frames_dropped,
       round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as pct_dropped
from marts.mart_trip_summary group by 1 order by 1
```
Kết quả: < 20 km/h **32,4%** (5 trip), 20–50 km/h 17,1%, 50–80 km/h 18,9%, ≥ 80 km/h **8,1%**. Xe càng chậm thì càng nhiều khung bị coi là trùng, đúng với kỳ vọng, nhưng nhóm 50–80 km/h lại cao hơn nhóm 20–50, do `trip-04622` kéo lên. Đây là bằng chứng bằng số cho đề xuất ở Câu 5: ngưỡng trùng nên phụ thuộc tốc độ.

## Nhìn lại

Điều bất ngờ nhất: bài tập sập rồi chạy lại chỉ cần chạy lại 8/37 task mà dữ liệu và `dataset_version` không đổi chút nào; idempotent theo partition làm việc vận hành nhẹ hơn hẳn. Một lỗi dữ liệu pipeline **chưa bắt được**: loại khung trùng bằng dHash so với khung liền trước loại nhầm khung khác nhau thật trên đường cao tốc (`trip-04622`, 67 km/h, 57% khung bị loại; khung `02421` và `02426` khác cảnh mà chỉ lệch 1 bit), và không có test nào cảnh báo "một trip mất quá nhiều khung". Tôi sẽ thêm test `max_share` cho `is_near_dup` theo trip (mức `warn`). Điều chưa rõ: vì sao `max_gap_ms` chỉ có 2 khoảng trống nhưng mỗi drive có 7 khung mất đồng bộ; tôi đoán mỗi khoảng trống ~3 giây chứa ~6 khung, cộng thêm khung ở biên trip. Về môi trường: trên Windows phải tắt `core.autocrlf` (nếu không, các file `.sh` và các file được bảo vệ đổi hash) và đặt `MSYS_NO_PATHCONV=1` khi chạy `lab.sh` trong Git Bash.
