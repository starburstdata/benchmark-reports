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
SELECT
    ex.id
  , m.name
  --, m.unit
  , format_metric(m.value, m.unit) AS value
FROM execution_measurements em
JOIN executions ex ON ex.id = em.execution_id
JOIN measurements m ON m.id = em.measurement_id
WHERE ex.benchmark_run_id = :id
ORDER BY ex.id, name, unit, value
