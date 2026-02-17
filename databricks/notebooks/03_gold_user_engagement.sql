CREATE OR REPLACE TABLE gold_user_engagement AS
SELECT u.user_id, COUNT(r.review_id) AS total_reviews, AVG(r.stars) AS avg_rating_given, DATEDIFF(MAX(r.date), MIN(r.date))/365 AS active_years
FROM silver_user u JOIN silver_review r ON u.user_id = r.user_id GROUP BY u.user_id;