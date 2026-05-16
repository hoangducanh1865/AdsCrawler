-- =============================================================================
-- Real-Time Layer: Kafka Engine → MergeTree → Materialized View
-- Source : processed_* Kafka topics (output of spark_consumer/speed_layer.py)
-- Pattern: 3 objects per topic
--   1. kafka_rt_<name>  — Kafka Engine table (reads live from topic, no storage)
--   2. rt_<name>        — ReplacingMergeTree (actual storage, TTL = 1 day)
--   3. mv_rt_<name>     — Materialized View (pipes Kafka → storage automatically)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. processed_dim_account
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_account
(
    account_id   String,
    account_name String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_account',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_account
(
    account_id   String,
    account_name String,
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY account_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_account
TO marketing_db.rt_dim_account
AS SELECT * FROM marketing_db.kafka_rt_dim_account;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. processed_dim_campaign
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_campaign
(
    campaign_id   String,
    account_id    String,
    campaign_name String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_campaign',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_campaign
(
    campaign_id   String,
    account_id    String,
    campaign_name String,
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY campaign_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_campaign
TO marketing_db.rt_dim_campaign
AS SELECT * FROM marketing_db.kafka_rt_dim_campaign;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. processed_dim_adset
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_adset
(
    adset_id    String,
    campaign_id String,
    adset_name  String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_adset',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_adset
(
    adset_id    String,
    campaign_id String,
    adset_name  String,
    updated_at  DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY adset_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_adset
TO marketing_db.rt_dim_adset
AS SELECT * FROM marketing_db.kafka_rt_dim_adset;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. processed_dim_ad
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_ad
(
    ad_id            String,
    adset_id         String,
    ad_name          String,
    status           String,
    effective_status String,
    created_time     Nullable(DateTime)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_ad',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_ad
(
    ad_id            String,
    adset_id         String,
    ad_name          String,
    status           String,
    effective_status String,
    created_time     Nullable(DateTime),
    updated_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY ad_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_ad
TO marketing_db.rt_dim_ad
AS SELECT * FROM marketing_db.kafka_rt_dim_ad;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. processed_dim_creative
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_creative
(
    creative_id                String,
    creative_title             String,
    creative_body              String,
    creative_thumbnail_raw_url String,
    creative_link              String,
    page_name                  String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_creative',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_creative
(
    creative_id                String,
    creative_title             String,
    creative_body              String,
    creative_thumbnail_raw_url String,
    creative_link              String,
    page_name                  String,
    updated_at                 DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY creative_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_creative
TO marketing_db.rt_dim_creative
AS SELECT * FROM marketing_db.kafka_rt_dim_creative;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. processed_dim_date
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_date
(
    date         Date,
    year         Int16,
    quarter      Int8,
    month        Int8,
    month_name   String,
    week         Int8,
    day_of_year  Int16,
    day_of_month Int8,
    day_of_week  Int8,
    day_name     String,
    is_weekend   UInt8,
    is_holiday   UInt8,
    holiday_name Nullable(String)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_date',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_date
(
    date         Date,
    year         Int16,
    quarter      Int8,
    month        Int8,
    month_name   String,
    week         Int8,
    day_of_year  Int16,
    day_of_month Int8,
    day_of_week  Int8,
    day_name     String,
    is_weekend   UInt8,
    is_holiday   UInt8,
    holiday_name Nullable(String),
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY date
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_date
TO marketing_db.rt_dim_date
AS SELECT * FROM marketing_db.kafka_rt_dim_date;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. processed_fact_fb_ad_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_fb_ad_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    spend                     Float32,
    impressions               Int32,
    reach                     Int32,
    clicks                    Int32,
    ctr                       Float32,
    cpc                       Float32,
    cpm                       Float32,
    frequency                 Float32,
    new_messaging_connections Int32,
    cost_per_new_messaging    Float32,
    link_clicks               Int32,
    landing_page_views        Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_fb_ad_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_fb_ad_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    spend                     Float32 DEFAULT 0,
    impressions               Int32   DEFAULT 0,
    reach                     Int32   DEFAULT 0,
    clicks                    Int32   DEFAULT 0,
    ctr                       Float32 DEFAULT 0,
    cpc                       Float32 DEFAULT 0,
    cpm                       Float32 DEFAULT 0,
    frequency                 Float32 DEFAULT 0,
    new_messaging_connections Int32   DEFAULT 0,
    cost_per_new_messaging    Float32 DEFAULT 0,
    link_clicks               Int32   DEFAULT 0,
    landing_page_views        Int32   DEFAULT 0,
    updated_at                DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, ad_id, date_start)
TTL date_start + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_fb_ad_daily
TO marketing_db.rt_fact_fb_ad_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_fb_ad_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. processed_fact_fb_ad_creative_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_fb_ad_creative_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    creative_id               String,
    spend                     Float32,
    impressions               Int32,
    reach                     Int32,
    clicks                    Int32,
    new_messaging_connections Int32,
    post_engagements          Int32,
    post_reactions            Int32,
    post_shares               Int32,
    photo_views               Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_fb_ad_creative_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_fb_ad_creative_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    creative_id               String,
    spend                     Float32 DEFAULT 0,
    impressions               Int32   DEFAULT 0,
    reach                     Int32   DEFAULT 0,
    clicks                    Int32   DEFAULT 0,
    new_messaging_connections Int32   DEFAULT 0,
    post_engagements          Int32   DEFAULT 0,
    post_reactions            Int32   DEFAULT 0,
    post_shares               Int32   DEFAULT 0,
    photo_views               Int32   DEFAULT 0,
    updated_at                DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, ad_id, creative_id, date_start)
TTL date_start + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_fb_ad_creative_daily
TO marketing_db.rt_fact_fb_ad_creative_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_fb_ad_creative_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. processed_fad_ad_daily_report  (flat/denormalized real-time copy)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fad_ad_daily_report
(
    id                        String,
    name                      String,
    adset_id                  String,
    adset_name                String,
    campaign_id               String,
    campaign_name             String,
    account_id                String,
    account_name              String,
    date_start                Date,
    date_stop                 Date,
    spend                     Float32,
    impressions               Int32,
    reach                     Int32,
    clicks                    Int32,
    messaging_first_reply     Int32,
    new_messaging_connections Int32,
    post_comments             Int32,
    link_clicks               Int32,
    landing_page_views        Int32,
    page_likes                Int32,
    thru_play                 Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fad_ad_daily_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fad_ad_daily_report
(
    id                        String,
    name                      String,
    adset_id                  String,
    adset_name                String,
    campaign_id               String,
    campaign_name             String,
    account_id                String,
    account_name              String,
    date_start                Date,
    date_stop                 Date,
    spend                     Float32 DEFAULT 0,
    impressions               Int32   DEFAULT 0,
    reach                     Int32   DEFAULT 0,
    clicks                    Int32   DEFAULT 0,
    messaging_first_reply     Int32   DEFAULT 0,
    new_messaging_connections Int32   DEFAULT 0,
    post_comments             Int32   DEFAULT 0,
    link_clicks               Int32   DEFAULT 0,
    landing_page_views        Int32   DEFAULT 0,
    page_likes                Int32   DEFAULT 0,
    thru_play                 Int32   DEFAULT 0,
    updated_at                DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, date_start, id)
TTL date_start + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fad_ad_daily_report
TO marketing_db.rt_fad_ad_daily_report
AS SELECT * FROM marketing_db.kafka_rt_fad_ad_daily_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 10. processed_fact_fb_ad_demographic_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_fb_ad_demographic_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    age                       String,
    gender                    String,
    spend                     Float32,
    impressions               Int32,
    reach                     Int32,
    clicks                    Int32,
    inline_link_clicks        Int32,
    new_messaging_connections Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_fb_ad_demographic_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_fb_ad_demographic_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    age                       String,
    gender                    String,
    spend                     Float32 DEFAULT 0,
    impressions               Int32   DEFAULT 0,
    reach                     Int32   DEFAULT 0,
    clicks                    Int32   DEFAULT 0,
    inline_link_clicks        Int32   DEFAULT 0,
    new_messaging_connections Int32   DEFAULT 0,
    updated_at                DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, ad_id, age, gender, date_start)
TTL date_start + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_fb_ad_demographic_daily
TO marketing_db.rt_fact_fb_ad_demographic_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_fb_ad_demographic_daily;
