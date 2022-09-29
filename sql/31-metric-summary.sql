-- Metrics summary
-- For every environment and benchmark, show the mean of every metric across all completed runs.
WITH
measurements AS (
    SELECT
        v.id
      , v.name AS metric_id
      , substr(v.name, strpos(v.name, '-') + 1) AS name
      , v.unit
      , v.value
      -- scope can be: prestoQuery, cluster, or driver if there's no prefix
      , CASE WHEN v.name LIKE '%-%' THEN split_part(v.name, '-', 1) ELSE 'driver' END AS scope
    FROM measurements v
    GROUP BY v.id, v.name, v.unit, v.value
)
, runs AS (
    SELECT
        runs.id
      , runs.environment_id
      , runs.sequence_id
      -- extract this one selected attribute because it's best as describing the whole run, even if it's not unique
      , regexp_replace(q.value, '/[^/]+$', '') AS benchmark_name
    FROM benchmark_runs runs
    LEFT JOIN benchmark_runs_attributes q ON q.benchmark_run_id = runs.id AND q.name = 'query-names'
    WHERE runs.status = 'ENDED' AND runs.environment_id = ANY(:env_ids)
)
, run_devs AS (
    SELECT
        runs.environment_id AS environment_id
      , runs.benchmark_name
      , m.metric_id
      , m.name
      , m.scope
      , m.unit
      , avg(m.value) AS mean
      , min(m.value) AS min
      , max(m.value) AS max
      , stddev(m.value) AS stddev
      , 100 * stddev(m.value) / nullif(cast(avg(m.value) as real), 0) AS stddev_pct
    FROM execution_measurements em
    JOIN executions ex ON ex.id = em.execution_id
    JOIN runs ON runs.id = ex.benchmark_run_id
    JOIN measurements m ON m.id = em.measurement_id
    WHERE m.name IN ('duration', 'totalCpuTime', 'peakTotalMemoryReservation')
    GROUP BY runs.environment_id, runs.benchmark_name, m.metric_id, m.name, m.scope, m.unit
)
SELECT
    e.name AS environment_pivot
  , r.benchmark_name AS benchmark_pivot
  , r.scope AS metric_scope
  , r.name AS metric_name
  , r.unit
  , r.mean AS mean_unit
  , r.stddev AS mean_err
  , cast(stddev_pct AS decimal(5,2)) AS err_pct_label
  , '[' || format_metric(r.min, unit) || ', ' || format_metric(r.max, unit) || ']' AS range_label
FROM environments e
JOIN run_devs r ON r.environment_id = e.id
ORDER BY 1, 2, 3, 4, 5
;
