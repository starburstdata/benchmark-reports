-- Run executions
-- Reads all names and values of all executions of a particular benchmark run.
WITH
measurements AS (
    SELECT
        v.id
      , substr(v.name, strpos(v.name, '-') + 1) AS name
      , v.unit
      , v.value
      , CASE WHEN v.name LIKE '%-%' THEN split_part(v.name, '-', 1) ELSE 'driver' END AS scope
    FROM measurements v
    GROUP BY v.id, v.name, v.unit, v.value
)
SELECT
    ex.id as "Execution id"
  , m.name as "Metric name"
  --, m.unit
  , format_metric(m.value, m.unit) AS value
FROM execution_measurements em
JOIN executions ex ON ex.id = em.execution_id
JOIN measurements m ON m.id = em.measurement_id
WHERE ex.benchmark_run_id = :id
ORDER BY ex.id, name, unit, value
