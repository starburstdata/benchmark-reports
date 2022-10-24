-- Top 5 differences
-- Find top 5 runs with same properties but from different environments with mean differences
-- of the query duration greater than the standard deviation of the other metric and 5%.
WITH
attributes AS (
    SELECT
        benchmark_run_id
      , array_agg(row(name, value) ORDER BY name, value) AS tuples
    FROM benchmark_runs_attributes
    GROUP BY 1
)
, variables AS (
    SELECT
        benchmark_run_id
      , array_agg(row(name, value) ORDER BY name, value) AS tuples
    FROM benchmark_runs_variables
    GROUP BY 1
)
, runs AS (
    SELECT
        runs.id
      , runs.environment_id
      , runs.sequence_id
      , b.value AS benchmark_name
      -- extract this one selected attribute because it's best as describing the whole run, even if it's not unique
      , regexp_replace(q.value, '^.*/([^/]*?)(\.[^/.]+)?$', '\1') AS query_name
      , attrs.tuples AS attributes
      , vars.tuples AS variables
      , attrs.tuples || vars.tuples AS properties
    FROM benchmark_runs runs
    LEFT JOIN attributes attrs ON attrs.benchmark_run_id = runs.id
    LEFT JOIN variables vars ON vars.benchmark_run_id = runs.id
    LEFT JOIN benchmark_runs_attributes b ON b.benchmark_run_id = runs.id AND b.name = 'name'
    LEFT JOIN benchmark_runs_attributes q ON q.benchmark_run_id = runs.id AND q.name = 'query-names'
    WHERE runs.status = 'ENDED'
    AND runs.environment_id = ANY(:env_ids)
)
, measurements AS (
    SELECT
        v.id
      , v.name AS metric_id
      , substr(v.name, strpos(v.name, '-') + 1) AS name
      , v.unit
      , v.value
      -- scope can be: prestoQuery, cluster, or driver if there's no prefix
      , CASE WHEN v.name LIKE '%-%' THEN split_part(v.name, '-', 1) ELSE 'driver' END AS scope
    FROM measurements v
    WHERE v.name IN ('duration')
    GROUP BY v.id, v.name, v.unit, v.value
)
, execution_devs AS (
    SELECT
        runs.id AS run_id
      , runs.benchmark_name
      , runs.query_name
      , m.metric_id
      , m.name
      , m.unit
      , m.scope
      , avg(m.value) AS mean
      , min(m.value) AS min
      , max(m.value) AS max
      , stddev(m.value) AS stddev
      , avg(m.value) - greatest(stddev(m.value), 0.05 * avg(m.value)) AS low
      , avg(m.value) + greatest(stddev(m.value), 0.05 * avg(m.value)) AS high
    FROM execution_measurements em
    JOIN executions ex ON ex.id = em.execution_id
    JOIN runs ON runs.id = ex.benchmark_run_id
    JOIN measurements m ON m.id = em.measurement_id
    GROUP BY runs.id, runs.benchmark_name, runs.query_name, m.metric_id, m.name, m.unit, m.scope
)
, diffs AS (
    SELECT
        env_left.name AS left_environment
      , env_right.name AS right_environment
      , run_left.id AS left_run_id
      , run_right.id AS right_run_id
      , ex_left.benchmark_name
      , ex_left.query_name
      , ex_left.name AS metric
      , ex_left.scope AS metric_scope
      , ex_left.unit AS unit
      -- result
      , ex_right.mean - ex_left.mean AS diff
      , 100 * (ex_right.mean - ex_left.mean) / nullif(cast(greatest(ex_right.mean, ex_left.mean) as real), 0) AS diff_pct
      -- details
      , ex_left.mean AS left_mean
      , ex_left.stddev AS left_stddev
      , 100 * ex_left.stddev / nullif(cast(ex_left.mean as real), 0) AS left_stddev_pct
      , ex_left.min AS left_min
      , ex_left.max AS left_max
      , ex_right.mean AS right_mean
      , ex_right.stddev AS right_stddev
      , 100 * ex_right.stddev / nullif(cast(ex_right.mean as real), 0) AS right_stddev_pct
      , ex_right.min AS right_min
      , ex_right.max AS right_max
    FROM runs run_left
    JOIN runs run_right ON run_left.environment_id != run_right.environment_id AND run_left.properties = run_right.properties
    JOIN execution_devs ex_left ON ex_left.run_id = run_left.id
    JOIN execution_devs ex_right ON ex_right.run_id = run_right.id AND ex_left.metric_id = ex_right.metric_id
    JOIN environments env_left ON env_left.id = run_left.environment_id
    JOIN environments env_right ON env_right.id = run_right.environment_id
    JOIN variables vars ON vars.benchmark_run_id = run_left.id
    WHERE
    env_left.name < env_right.name
    AND (ex_left.mean NOT BETWEEN ex_right.low AND ex_right.high OR ex_right.mean NOT BETWEEN ex_left.low AND ex_left.high)
)
, diffs_ranked AS (
    SELECT
        *
      , row_number() OVER (ORDER BY diff_pct DESC, left_environment, right_environment, left_run_id, right_run_id, benchmark_name, query_name, metric) AS rownum
    FROM diffs
)
SELECT
    -- not returning environments, as it is expected to run this summary between two known ones
    benchmark_name
  , query_name
  , metric
  , metric_scope
  , format_metric(diff, unit) AS diff_label
  , format_percent(diff_pct) AS diff_pct_label
  , format_metric(left_mean, unit) AS left_mean_label
  , round(left_stddev::numeric, 2) AS left_mean_err_label
  , cast(left_stddev_pct AS decimal(5,2)) AS left_err_pct_label
  , format_metric(right_mean, unit) AS right_mean_label
  , round(right_stddev::numeric, 2) AS right_mean_err_label
  , cast(right_stddev_pct AS decimal(5,2)) AS right_err_pct_label
FROM diffs_ranked
WHERE rownum < 6
ORDER BY metric, rownum
;
