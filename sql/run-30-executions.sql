-- Run executions
-- Reads all names and values of all executions of a particular benchmark run.
SELECT
    ex.id as "Execution id"
  , substr(m.name, strpos(m.name, '-') + 1) AS "Metric name"
  , CASE WHEN m.name LIKE '%-%' THEN split_part(m.name, '-', 1) ELSE 'driver' END AS "Metric scope"
  , format_metric(m.value, m.unit) AS value
FROM execution_measurements em
JOIN executions ex ON ex.id = em.execution_id
JOIN measurements m ON m.id = em.measurement_id
WHERE ex.benchmark_run_id = :id
ORDER BY ex.id, name, unit, value
