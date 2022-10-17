-- Run execution measurements
-- Reads all names and aggregated min, max, mean, stddev values of all execution metrics of a particular benchmark run.
WITH
execution_devs AS (
    SELECT
        runs.id AS run_id
      , substr(m.name, strpos(m.name, '-') + 1) AS name
      , CASE WHEN m.name LIKE '%-%' THEN split_part(m.name, '-', 1) ELSE 'driver' END AS scope
      , m.unit
      , avg(m.value) AS mean
      , min(m.value) AS min
      , max(m.value) AS max
      , stddev(m.value) AS stddev
    FROM execution_measurements em
    JOIN executions ex ON ex.id = em.execution_id
    JOIN benchmark_runs runs ON runs.id = ex.benchmark_run_id
    JOIN measurements m ON m.id = em.measurement_id
    WHERE runs.id = :id
    GROUP BY 1, 2, 3, 4
)
SELECT
    name as metric_name
  , scope as metric_scope
  , format_metric(mean, unit) AS mean_num
  , '±' || format_metric(stddev, unit) || ' (' || round(cast(stddev/nullif(cast(mean as float), 0) as numeric), 2) || '%)' AS stddev_num
  , format_metric(min, unit) AS min_num
  , format_metric(max, unit) AS max_num
FROM execution_devs
ORDER BY name, scope, unit, mean
