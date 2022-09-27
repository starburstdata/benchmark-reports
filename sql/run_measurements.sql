WITH
measurements AS (
    SELECT
        v.id
      , m.name
      , m.unit
      , v.value
      , array_agg(row(a.name, a.value) ORDER BY a.name) AS attributes
    FROM measurements v
    JOIN metrics m ON m.id = v.metric_id
    JOIN metric_attributes a ON m.id = a.metric_id
    GROUP BY v.id, m.name, m.unit, v.value
)
, execution_devs AS (
    SELECT
        runs.id AS run_id
      , m.name
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
    GROUP BY 1, 2, 3
)
SELECT
    name
  --, unit
  , format_metric(mean, unit) AS mean
  , '±' || format_metric(stddev, unit) || ' (' || round(cast(stddev/nullif(cast(mean as float), 0) as numeric), 2) || '%)' AS stddev
  , format_metric(min, unit) AS min
  , format_metric(max, unit) AS max
FROM execution_devs
ORDER BY name, unit, mean
