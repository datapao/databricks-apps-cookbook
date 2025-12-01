CREATE OR REFRESH MATERIALIZED VIEW departure_delay AS
SELECT
    origin,
    dest,
    month,
    SUM(dep_delay) AS total_delay
FROM demo_wzz_dbxapps_default.default.flights_2013 
GROUP BY origin, dest, month;