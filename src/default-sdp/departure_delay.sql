CREATE OR REFRESH MATERIALIZED VIEW departure_delay AS
SELECT
    SUM(dep_delay) AS total_delay,
    EXTRACT(YEAR FROM dep_timestamp) AS year,
    EXTRACT(MONTH FROM dep_timestamp) AS month
FROM dgulacsy.demo.flights2;
