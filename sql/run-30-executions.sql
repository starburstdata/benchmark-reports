-- Run executions
-- Reads all names and values of all executions of a particular benchmark run.
SELECT
    ex.benchmark_run_id as run_id
  , ex.id as execution_id
  , substr(m.name, strpos(m.name, '-') + 1) AS metric_name
  , CASE WHEN m.name LIKE '%-%' THEN split_part(m.name, '-', 1) ELSE 'driver' END AS metric_scope
  , format_metric(m.value, m.unit) AS value_num
FROM execution_measurements em
JOIN executions ex ON ex.id = em.execution_id
JOIN measurements m ON m.id = em.measurement_id
WHERE ex.benchmark_run_id = ANY(:ids)
ORDER BY ex.benchmark_run_id, ex.id, metric_name, metric_scope, unit, value
