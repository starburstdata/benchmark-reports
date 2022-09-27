-- Validate environments
-- For every environment, check for statistical outliers on every metric across all executions of every run of every benchmark.
-- The number of runs should be equal across all environments and there should be no invalid executions and no outliers.
/* TODO remaining questions:
- how to explain different number of runs between envs
- do we want to distinguish driver and cluster metrics?
*/
WITH
environments AS (
    SELECT
        env.id
      , env.name
      , array_agg(a.name || '=' || a.value ORDER BY a.name, a.value) AS attributes
    FROM environments env
    LEFT JOIN environment_attributes a ON a.environment_id = env.id AND a.name NOT IN ('startup_logs')
    GROUP BY 1, 2
)
, measurements AS (
    SELECT
        v.id
      , m.name
      , m.unit
      , v.value
      , min(a.value) FILTER (WHERE a.name = 'scope') AS scope
      , array_agg(a.name || '=' || a.value ORDER BY a.name, a.value) AS attributes
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
    GROUP BY 1, 2, 3
)
, execution_stats AS (
    SELECT
        devs.run_id
      , count(DISTINCT ex.id) AS num_executions
      , count(DISTINCT ex.id) FILTER (WHERE m.value NOT BETWEEN devs.low AND devs.high) AS num_invalid_executions
      , count(*) AS num_measurements
      , count(*) FILTER (WHERE m.value NOT BETWEEN devs.low AND devs.high) AS num_outliers
      , count(*) FILTER (WHERE m.scope = 'driver' AND m.value NOT BETWEEN devs.low AND devs.high) AS num_driver_outliers
      , array_agg(DISTINCT m.name ORDER BY m.name) FILTER (WHERE m.value NOT BETWEEN devs.low AND devs.high) AS names_outliers
      , array_agg(DISTINCT m.name ORDER BY m.name) AS names_all
    FROM execution_devs devs
    JOIN executions ex ON ex.benchmark_run_id = devs.run_id
    JOIN execution_measurements em ON em.execution_id = ex.id
    JOIN measurements m ON m.id = em.measurement_id AND m.name = devs.name
    GROUP BY 1
)
SELECT
    env.name AS environment
  , array_to_string(env.attributes, E'<br/>') AS attributes
  , runs.sequence_id AS sequence_id
  , array_agg(DISTINCT runs.status) AS statuses
  , count(DISTINCT runs.id) AS runs_num
  , sum(s.num_executions) AS executions_num
  , sum(s.num_invalid_executions) AS invalid_executions_num
  , sum(s.num_measurements) AS measurements_num
  , sum(s.num_outliers) AS outliers_num
  , sum(s.num_driver_outliers) AS driver_outliers_num
  -- TODO use this in Trino
  -- , array_sort(array_distinct(flatten(array_agg(s.names_outliers)))) AS names_outliers
  -- , array_sort(array_distinct(flatten(array_agg(s.names_all)))) AS names_ok
  , array_union_agg(s.names_outliers) AS metrics_with_outliers_label
FROM environments env
LEFT JOIN benchmark_runs runs ON runs.environment_id = env.id
LEFT JOIN execution_stats s ON s.run_id = runs.id
GROUP BY 1, 2, 3
;
