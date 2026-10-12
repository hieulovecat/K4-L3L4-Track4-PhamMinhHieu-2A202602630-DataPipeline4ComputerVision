-- Câu hỏi riêng 1 · Trip nào bị loại nhiều khung nhất?
select trip_id, drive_id, lighting, split, avg_speed_kmh, frames,
       frames - frames_kept as frames_dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_dropped
from marts.mart_trip_summary
order by pct_dropped desc, frames_dropped desc
limit 10
;

-- Câu hỏi riêng 2 · Tốc độ xe và tỷ lệ khung bị loại
select case when avg_speed_kmh < 20 then '1. < 20 km/h'
            when avg_speed_kmh < 50 then '2. 20-50 km/h'
            when avg_speed_kmh < 80 then '3. 50-80 km/h'
            else '4. >= 80 km/h' end as speed_band,
       count(*) as trips, sum(frames) as frames, sum(frames - frames_kept) as frames_dropped,
       round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as pct_dropped
from marts.mart_trip_summary
group by 1 order by 1
;
