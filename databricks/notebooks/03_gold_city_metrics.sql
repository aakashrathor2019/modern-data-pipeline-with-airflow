CREATE OR REPLACE TABLE gold_city_business_metrics AS

SELECT
    city,
    COUNT(DISTINCT business_id) AS total_businesses,
    AVG(avg_rating) AS avg_city_rating,
    SUM(review_count) AS total_reviews

FROM gold_business_ratings

GROUP BY city;
