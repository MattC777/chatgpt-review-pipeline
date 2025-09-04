-- sql/transform.sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWS_PIPELINE;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- 去重清洗 → 生成 REVIEWS_CLEAN
CREATE OR REPLACE TABLE REVIEWS_CLEAN AS
WITH latest AS (
  SELECT
    review_id,
    review_text,
    TRY_TO_NUMBER(score)               AS score,
    TRY_TO_NUMBER(thumbs_up_count)     AS thumbs_up_count,
    app_id,
    app_version,
    user_name,
    TO_TIMESTAMP_NTZ(review_created_at) AS review_created_at,
    TO_TIMESTAMP_NTZ(scraped_at)        AS scraped_at,
    ROW_NUMBER() OVER (
      PARTITION BY review_id
      ORDER BY TO_TIMESTAMP_NTZ(scraped_at) DESC
    ) AS rn
  FROM REVIEWS_PIPELINE.STAGING.RAW_REVIEWS
  WHERE review_id IS NOT NULL
)
SELECT
  review_id,
  review_text,
  score::INTEGER                               AS score,
  COALESCE(thumbs_up_count, 0)::INTEGER        AS thumbs_up_count,
  app_id,
  app_version,
  user_name,
  review_created_at,
  scraped_at,
  CURRENT_TIMESTAMP()                          AS _transformed_at
FROM latest
WHERE rn = 1
  AND score BETWEEN 1 AND 5;

-- 最近 7 天视图
CREATE OR REPLACE VIEW V_RECENT_7D AS
SELECT *
FROM REVIEWS_CLEAN
WHERE review_created_at >= DATEADD(day, -7, CURRENT_DATE());
