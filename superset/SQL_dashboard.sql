-- 1. PieChart ~ Filter
SELECT 
    dc.campaign_name,
    f.date_start,
    SUM(f.spend) AS total_spend
FROM marketing_db.fact_fb_ad_daily f
JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
GROUP BY dc.campaign_name, f.date_start;


-- 2. BIỂU ĐỒ CHI TIẾT (Trendline, Big Number, Bảng biểu...)
-- CPM: Cost per mile (Tỉ lệ chuyển đổi từ impression -> Spend)
-- CTR: Click through rate (Tỉ lệ chuyển đổi từ impression -> click)
-- CPC: Cost per click (Tỉ lệ chuyển đổi từ click -> Spend)
SELECT 
    f.date_start,
    dc.campaign_name,
    SUM(f.spend) AS total_spend,
    SUM(f.new_messaging_connections) AS total_messaging,
    SUM(f.clicks) AS total_clicks,
    SUM(f.impressions) AS total_impressions,
    if(SUM(f.new_messaging_connections) > 0, SUM(f.spend) / SUM(f.new_messaging_connections), 0) AS cost_per_message,
    (SUM(clicks) / nullIf(SUM(impressions), 0)) * 100 AS ctr,
    SUM(f.spend) / nullIf(SUM(f.clicks), 0) AS cpc,
    (SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000 AS cpm,
    (SUM(f.new_messaging_connections) / nullIf(SUM(f.clicks), 0)) * 100 AS message_rate

FROM marketing_db.fact_fb_ad_daily f
JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
GROUP BY f.date_start, dc.campaign_name
ORDER BY f.date_start ASC;

-- 2.0. Diminishing Returns theo Campaign (tham chiếu mục 3.1 trong docs/mock_data_logic.md)
-- Cấp dữ liệu: 1 dòng / campaign / day để vẽ scatter/bubble chart với X = total_spend, Y = cpm, Series = campaign_name.
-- Giữ thêm total_impressions để có thể tính lại CPM đúng khi Superset aggregate theo filter thời gian.
SELECT
    f.date_start AS date_start,
    dd.year AS year,
    dd.quarter AS quarter,
    dd.month AS month,
    dd.month_name AS month_name,
    dd.week AS week,
    ds.campaign_id AS campaign_id,
    dc.campaign_name AS campaign_name,
    SUM(f.spend) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    (SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000 AS cpm
FROM marketing_db.fact_fb_ad_daily f
JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
JOIN marketing_db.dim_date dd ON f.date_start = dd.date
GROUP BY
    f.date_start,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.week,
    ds.campaign_id,
    dc.campaign_name
ORDER BY date_start ASC, total_spend DESC, campaign_name ASC;

-- 2.1. Phân tích seasonality theo Weekend / Holiday (tham chiếu mục 3.2 trong docs/mock_data_logic.md)
-- Dùng dataset này để so sánh CPM, CPC, CTR, message rate giữa ngày thường, cuối tuần và dịp lễ.
SELECT
    f.date_start,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.week,
    dd.day_of_week,
    dd.day_name,
    dd.is_weekend,
    dd.is_holiday,
    if(dd.is_weekend = 1, 'Weekend', 'Weekday') AS weekend_bucket,
    if(dd.is_holiday = 1, ifNull(dd.holiday_name, 'Holiday'), 'Non-Holiday') AS holiday_bucket,
    ifNull(dd.holiday_name, 'Non-Holiday') AS holiday_name,
    multiIf(
        dd.is_holiday = 1, concat('Holiday - ', ifNull(dd.holiday_name, 'Unknown')),
        dd.is_weekend = 1, 'Weekend',
        'Weekday'
    ) AS seasonality_bucket,
    f.account_id,
    acc.account_name,
    ds.campaign_id,
    dc.campaign_name,
    SUM(f.spend) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.reach) AS total_reach,
    SUM(f.clicks) AS total_clicks,
    SUM(f.link_clicks) AS total_link_clicks,
    SUM(f.landing_page_views) AS total_landing_page_views,
    SUM(f.new_messaging_connections) AS total_messaging,
    if(SUM(f.new_messaging_connections) > 0, SUM(f.spend) / SUM(f.new_messaging_connections), 0) AS cost_per_message,
    (SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100 AS ctr,
    SUM(f.spend) / nullIf(SUM(f.clicks), 0) AS cpc,
    (SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000 AS cpm,
    (SUM(f.new_messaging_connections) / nullIf(SUM(f.clicks), 0)) * 100 AS message_rate
FROM marketing_db.fact_fb_ad_daily f
JOIN marketing_db.dim_account acc ON f.account_id = acc.account_id
JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
JOIN marketing_db.dim_date dd ON f.date_start = dd.date
GROUP BY
    f.date_start,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.week,
    dd.day_of_week,
    dd.day_name,
    dd.is_weekend,
    dd.is_holiday,
    weekend_bucket,
    holiday_bucket,
    holiday_name,
    seasonality_bucket,
    f.account_id,
    acc.account_name,
    ds.campaign_id,
    dc.campaign_name
ORDER BY f.date_start ASC, seasonality_bucket ASC, dc.campaign_name ASC;

-- Phân tích theo nhân khẩu (Age, gender)
SELECT 
    f.date_start,
    f.account_id,
    acc.account_name,
    s.campaign_id,
    c.campaign_name,
    f.age,
    multiIf(
        f.age = '18-24', 1,
        f.age = '25-34', 2,
        f.age = '35-44', 3,
        f.age = '45-54', 4,
        f.age = '55-64', 5,
        f.age = '65+', 6,
        99
    ) AS age_order,
    f.gender,
    SUM(f.spend) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.reach) AS total_reach,
    SUM(f.clicks) AS total_clicks,
    SUM(f.inline_link_clicks) AS total_link_clicks,
    SUM(f.new_messaging_connections) AS total_messaging,
    (SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100 AS ctr,
    (SUM(f.spend) / nullIf(SUM(f.clicks), 0)) AS cpc,
    (SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000 AS cpm,
    (SUM(f.new_messaging_connections) / nullIf(SUM(f.clicks), 0)) * 100 AS message_rate
FROM marketing_db.fact_fb_ad_demographic_daily f
JOIN marketing_db.dim_account acc ON f.account_id = acc.account_id
JOIN marketing_db.dim_ad a ON f.ad_id = a.ad_id
JOIN marketing_db.dim_adset s ON a.adset_id = s.adset_id
JOIN marketing_db.dim_campaign c ON s.campaign_id = c.campaign_id
WHERE f.gender != 'unknown'
GROUP BY 
    f.date_start,
    f.account_id,
    acc.account_name,
    s.campaign_id,
    c.campaign_name,
    f.age, 
    age_order,
    f.gender
ORDER BY f.date_start ASC, age_order ASC, f.gender ASC;

-- Funnel waterfall
-- Dùng fact + dim để Superset filter được theo campaign/adset/account từ mô hình sao.
-- Lưu ý: fact_fb_ad_daily hiện không có leads/purchases, nên funnel này dùng các bước có thật trong fact table.
SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  1 AS step_order,
  'Impressions' AS step,
  toFloat64(ifNull(base.impressions, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.fact_fb_ad_daily f
    JOIN marketing_db.dim_account acc ON f.account_id = acc.account_id
    JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
    JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
) AS base

UNION ALL

SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  2 AS step_order,
  'Clicks' AS step,
  toFloat64(ifNull(base.clicks, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.fact_fb_ad_daily f
    JOIN marketing_db.dim_account acc ON f.account_id = acc.account_id
    JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
    JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
) AS base

UNION ALL

SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  3 AS step_order,
  'Link Clicks' AS step,
  toFloat64(ifNull(base.link_clicks, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.fact_fb_ad_daily f
    JOIN marketing_db.dim_account acc ON f.account_id = acc.account_id
    JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
    JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
) AS base

UNION ALL

SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  4 AS step_order,
  'Landing Page Views' AS step,
  toFloat64(ifNull(base.landing_page_views, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.fact_fb_ad_daily f
    JOIN marketing_db.dim_account acc ON f.account_id = acc.account_id
    JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
    JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
) AS base

UNION ALL

SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  5 AS step_order,
  'New Messenger' AS step,
  toFloat64(ifNull(base.new_messaging_connections, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.fact_fb_ad_daily f
    JOIN marketing_db.dim_account acc ON f.account_id = acc.account_id
    JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
    JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
) AS base;
