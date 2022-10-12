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
, runs AS (
    SELECT
        runs.id
      , runs.environment_id
      , runs.sequence_id
      , runs.status
      -- extract this one selected attribute because it's best as describing the whole run, even if it's not unique
      , regexp_replace(q.value, '/[^/]+$', '') AS benchmark_name
      , regexp_replace(q.value, '^.*/([^/]*?)(\.[^/.]+)?$', '\1') AS query_name
      , q.value AS full_query_name
    FROM benchmark_runs runs
    LEFT JOIN benchmark_runs_attributes q ON q.benchmark_run_id = runs.id AND q.name = 'query-names'
    AND runs.environment_id = ANY(:env_ids)
)
, measurements AS (
    SELECT
        v.id
      , substr(v.name, strpos(v.name, '-') + 1) AS name
      , v.unit
      , v.value
      -- scope can be: prestoQuery, cluster, or driver if there's no prefix
      , CASE WHEN v.name LIKE '%-%' THEN split_part(v.name, '-', 1) ELSE 'driver' END AS scope
    FROM measurements v
    GROUP BY v.id, v.name, v.unit, v.value
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
    JOIN runs ON runs.id = ex.benchmark_run_id
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
  format('<a href="envs/%s/env-details.html">%s</a>', env.id, env.name) AS environment
  , array_to_string(env.attributes, E'\n') AS attributes_label
  , runs.sequence_id AS sequence_id
  , array_agg(DISTINCT runs.status) AS statuses_label
  , count(DISTINCT runs.benchmark_name) AS benchmarks_num
  , count(DISTINCT runs.full_query_name) AS queries_num
  , count(DISTINCT runs.id) AS runs_num
  , sum(s.num_executions) AS executions_num
  , sum(s.num_invalid_executions) AS invalid_executions_num
  , sum(s.num_measurements) AS measurements_num
  , sum(s.num_outliers) AS outliers_num
  , sum(s.num_driver_outliers) AS driver_outliers_num
  , array_union_agg(s.names_outliers) AS metrics_with_outliers_label
FROM environments env
LEFT JOIN runs ON runs.environment_id = env.id
LEFT JOIN execution_stats s ON s.run_id = runs.id
WHERE env.id = ANY(:env_ids)
GROUP BY 1, 2, 3
;
