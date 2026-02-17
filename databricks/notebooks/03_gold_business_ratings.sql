CREATE OR REPLACE TABLE gold_business_ratings AS
SELECT b.business_id, b.name AS business_name, b.city, b.state, b.categories, AVG(r.stars) AS avg_rating, COUNT(r.review_id) AS review_count, MAX(r.date) AS latest_review_date
FROM silver_business b JOIN silver_review r ON b.business_id = r.business_id
GROUP BY b.business_id, b.name, b.city, b.state, b.categories;