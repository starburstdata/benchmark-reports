-- Differences summary
-- Histogram of difference percentage between runs with same properties but from different environments, for every environment pair and metric
-- The distribution of difference percentage should be centered around 0.
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
      , attrs.tuples AS attributes
      , vars.tuples AS variables
      , attrs.tuples || vars.tuples AS properties
    FROM benchmark_runs runs
    LEFT JOIN attributes attrs ON attrs.benchmark_run_id = runs.id
    LEFT JOIN variables vars ON vars.benchmark_run_id = runs.id
    WHERE runs.status = 'ENDED'
)
, measurements AS (
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
    GROUP BY 1, 2, 3
)
, run_pairs AS (
    SELECT
      /*  env_left.name AS left_env_name
      , env_right.name AS right_env_name
      ,*/ ex_left.name AS metric
      , ex_left.unit AS unit
      -- result
      , cast(100 * (ex_right.mean - ex_left.mean) / nullif(cast(greatest(ex_right.mean, ex_left.mean) as real), 0) AS decimal(5,2)) AS diff_pct
    FROM runs run_left
    JOIN runs run_right ON run_left.environment_id != run_right.environment_id AND run_left.properties = run_right.properties
    JOIN execution_devs ex_left ON ex_left.run_id = run_left.id
    JOIN execution_devs ex_right ON ex_right.run_id = run_right.id AND ex_left.name = ex_right.name
    JOIN environments env_left ON env_left.id = run_left.environment_id
    JOIN environments env_right ON env_right.id = run_right.environment_id
    -- don't count the same pair twice
    WHERE env_left.name < env_right.name
)
, pair_stats AS (
    SELECT
      /*  left_env_name
      , right_env_name
      ,*/ metric
      , unit
      , min(diff_pct) as min
      , max(diff_pct) as max
    FROM run_pairs
    GROUP BY /*left_env_name, right_env_name,*/ metric, unit
)
, dimensions AS (
    SELECT
      /*  left_env_name
      , right_env_name
      ,*/ metric
      , unit
    FROM run_pairs
    GROUP BY /*left_env_name, right_env_name,*/ metric, unit
)
, histogram as (
    SELECT
      /*  left_env_name
      , right_env_name
      ,*/ metric
      , unit
      , width_bucket(diff_pct, s.min, nullif(s.max, s.min), 9) AS bucket
      , numrange(min(diff_pct), max(diff_pct), '[]') AS range
      , count(*) AS freq
    FROM run_pairs r
    JOIN pair_stats s USING (/*left_env_name, right_env_name,*/ metric, unit)
    GROUP BY /*left_env_name, right_env_name,*/ metric, unit, bucket
)
SELECT
  /*  d.left_env_name
  , d.right_env_name
  ,*/ d.metric AS metric
  , d.unit AS unit
  , s.bucket AS bucket
  , range AS diff_pct_range
  , coalesce(freq, 0) AS occurrences_num
  , repeat('■', (coalesce(freq, 0)::float / max(freq) over() * 30)::int) AS bar_chart_label
FROM dimensions d
CROSS JOIN generate_series(1, 10) s(bucket)
LEFT JOIN histogram h ON (/*d.left_env_name, d.right_env_name,*/ d.metric, d.unit, s.bucket) = (/*h.left_env_name, h.right_env_name,*/ h.metric, h.unit, h.bucket)
ORDER BY /*d.left_env_name, d.right_env_name,*/ d.metric, d.unit, s.bucket
;
