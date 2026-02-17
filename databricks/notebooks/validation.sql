CREATE OR REPLACE TABLE validation_results AS

SELECT
    (SELECT COUNT(*) FROM silver_business) AS silver_business_cnt,
    (SELECT COUNT(*) FROM gold_business_ratings) AS gold_business_cnt,

    (SELECT COUNT(*) FROM silver_review) AS silver_review_cnt,
    (SELECT SUM(review_count) FROM gold_business_ratings) AS gold_review_cnt;
