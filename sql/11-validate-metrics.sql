-- Validate metrics
-- For every metric, check for statistical outliers across all executions of every run (query?) of every benchmark.
-- The number of runs should be equal across all metrics and there should be no invalid executions and no outliers.
WITH
metrics AS (
    SELECT
        m.name AS id
      , substr(m.name, strpos(m.name, '-') + 1) AS name
      , m.unit
      -- scope can be: prestoQuery, cluster, or driver if there's no prefix
      , CASE WHEN m.name LIKE '%-%' THEN split_part(m.name, '-', 1) ELSE 'driver' END AS scope
    FROM measurements m
    GROUP BY 1, 2, 3
)
, execution_devs AS (
    SELECT
        runs.id AS run_id
      , m.name
      , m.unit
      -- consider using this: https://github.com/sharkdp/hyperfine/blob/master/src/outlier_detection.rs
      -- this doesn't work well for low number of runs, because the ratio of the distance from the mean
      -- divided by the SD can never exceed (N-1)/sqrt(N)
      -- so for 3 runs no outlier can possibly be more than 1.155*SD from the mean
      , avg(m.value) - 2 * stddev(m.value) AS low
      , avg(m.value) + 2 * stddev(m.value) AS high
    FROM execution_measurements em
    JOIN executions ex ON ex.id = em.execution_id
    JOIN benchmark_runs runs ON runs.id = ex.benchmark_run_id
    JOIN measurements m ON m.id = em.measurement_id
    WHERE runs.environment_id = ANY(:env_ids)
    GROUP BY 1, 2, 3
)
, execution_stats AS (
    SELECT
        m.name AS metric_id
      , count(DISTINCT ex.id) AS num_executions
      , count(DISTINCT ex.id) FILTER (WHERE m.value NOT BETWEEN devs.low AND devs.high) AS num_invalid_executions
      , count(*) AS num_measurements
      , count(*) FILTER (WHERE m.value NOT BETWEEN devs.low AND devs.high) AS num_outliers
      , count(*) FILTER (WHERE m.name NOT LIKE '%\_%' AND m.value NOT BETWEEN devs.low AND devs.high) AS num_driver_outliers
    FROM execution_devs devs
    JOIN executions ex ON ex.benchmark_run_id = devs.run_id
    JOIN execution_measurements em ON em.execution_id = ex.id
    JOIN measurements m ON m.id = em.measurement_id AND m.name = devs.name
    GROUP BY 1
)
SELECT
    m.name AS metric
  -- technically these are metrics, but treat them as labels to avoid showing a chart for this query
  , sum(s.num_executions) AS executions_label
  , sum(s.num_invalid_executions) AS invalid_executions_label
  , sum(s.num_measurements) AS measurements_label
  , sum(s.num_outliers) AS outliers_label
  , sum(s.num_driver_outliers) AS driver_outliers_label
FROM metrics m
LEFT JOIN execution_stats s ON s.metric_id = m.id
GROUP BY 1
ORDER BY 1
;
