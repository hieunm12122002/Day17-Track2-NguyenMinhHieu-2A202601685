# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Minh Hiếu  **Lớp:** AICB-P2T2  **Ngày:** 2026-09-03

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 28.2s
  run 2/3 … 27.2s
  run 3/3 … 27.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 132,888 (37.6×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Kích thước bảng `gold_training_set` tăng lên sau mỗi lần chạy lại (Clear Task). |
| **Nguyên nhân** | Bảng incremental của dbt không khai báo `unique_key`, dẫn đến dbt mặc định dùng chiến lược `append`. Mọi bản ghi được cập nhật hoặc chạy lại đều bị ghi nối thêm thay vì ghi đè. Hơn nữa, việc cấu hình DAG `catchup=True` và thiếu `max_active_runs` khiến Airflow có thể chạy nhiều lô dữ liệu trong quá khứ cùng lúc mà không có kiểm soát. |
| **Cách khắc phục** | Thêm `unique_key='ticket_id'` và `incremental_strategy='merge'` vào `config()` của model `gold_training_set.sql`. Cập nhật `ai_training_pipeline.py` với `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt: giống hệt nhau |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` thiếu khoảng 5% bản ghi ở các ngày cũ so với dữ liệu trong silver. |
| **P99 độ trễ đo được** | **2.73 ngày** *(bắt buộc)* |
| **Lookback đã chọn** | 3 ngày — vì P99 là 2.73 ngày, đảm bảo 99% dữ liệu sự kiện trễ sẽ được xử lý lại mà không cần quét lại toàn bộ dữ liệu. |
| **Nguyên nhân** | Điều kiện incremental `event_date > max(event_date)` chỉ cho phép quét các bản ghi thuộc về ngày mới nhất trong bảng đích. Những sự kiện của các ngày quá khứ nhưng đến hệ thống trễ (nhập vào muộn) sẽ bị bỏ qua. |
| **Cách khắc phục** | Trong `gold_feature_daily.sql`, nới lỏng điều kiện incremental thành `event_date >= (select max(event_date) - interval 3 day)` và cấu hình `unique_key=['event_date', 'customer_id']` kết hợp với `incremental_strategy='merge'` để ghi đè (thay vì insert trùng). |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn `max` sẽ phải lookback số ngày bằng với khoảng thời gian tối đa dữ liệu có thể trễ (có thể vài tuần), dẫn đến chi phí quét và xử lý (write) lại lượng dữ liệu cực lớn cho *mọi* lần chạy DAG trong tương lai. Chọn P99 chỉ tốn thêm 3 ngày dữ liệu quét lại, cân bằng tối ưu giữa độ chính xác (99%) và chi phí vận hành (I/O, compute).

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Cột `priority` xuất hiện NULL, nhãn chữ (`urgent`, `high`), và giá trị lỗi (`0`, `5`, `-1`) dù theo thiết kế phải là số 1-4. Pipeline vẫn chạy nhưng model phân loại dự đoán sai. |
| **Nguyên nhân** | Tầng Bronze nhận CDC không kiểm tra cấu trúc. Tầng Silver dùng hàm `try_cast` sai: biến nhãn hợp lệ thành NULL nhưng lại cho qua các giá trị ngoài miền hợp lệ (vd 0, 5). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Nhóm 1 (số hợp lệ 1-4): Giữ nguyên số. Nhóm 2 (nhãn chữ 'urgent', 'high', ...): Trích xuất thông tin bằng cách quy về 1-4. Nhóm 3 (Lỗi 'P1', '0', NULL...): Trả về NULL để đưa vào bảng quarantine. |
| **Cách khắc phục** | Trong `normalize_priority.sql`, dùng khối `CASE` xử lý 3 nhóm. Ở `silver_tickets.sql`, lọc bỏ giá trị NULL *trước* khi `row_number()` để tránh drop mất nguyên cả ticket. Ở `quarantine_tickets.sql` thì hứng giá trị NULL. Cuối cùng, chỉnh `enforced: true` và test `accepted_values` trong `schema.yml`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` pass (11 test) |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Chặn ở tầng Silver thay vì Bronze, vì Bronze cần giữ nguyên y hệt source (ELT - raw events) để dễ truy vết lỗi và replay nếu cần. Không dừng pipeline (fail-fast) vì một vài trăm bản ghi lỗi không đáng để làm chết toàn bộ 130.000 event và 31.200 chunk hệ thống tốt đang cần được load hàng ngày; ưu tiên tính khả dụng cao bằng cách cách ly lỗi (quarantine).

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A và B |
| **Nguyên nhân** | Bài A (Dashboard chậm): Bị small-file problem (5000 file) và filter không sargable trên cột datetime. Bài B (Mất data crash): Consumer dùng semantics At-Most-Once (commit offset trước khi lưu vào DB) nên khi crash mất batch đang nạp. |
| **Cách khắc phục** | Bài A: Chạy `COPY TO` theo `partition_by (event_date)` với `row_group_size 100000` và sửa truy vấn dùng hive partition. Bài B: Dời thao tác `commit()` xuống dưới `write_batch()`, đặt `event_id` làm `PRIMARY KEY` và dùng `ON CONFLICT DO UPDATE SET` cho bảng stream. |
| **Bằng chứng** | Bài A: Scanned rows giảm xuống 132,888 (giảm 37.6x) và files giảm còn 14. Bài B: `make crash-test` thành công với output NHIỆM VỤ MỞ RỘNG B: ĐẠT. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Các model dbt incremental đã khai báo đúng `unique_key` và `incremental_strategy` hay chưa. |
| 2 | Phân bố độ trễ (latency) của dữ liệu từ thời điểm phát sinh đến khi vào warehouse (P99). |
| 3 | Contract/Schema ràng buộc (type, null, accepted values) tại các model cốt lõi (Silver/Gold) đã được bật chưa. |
