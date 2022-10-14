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
    WHERE runs.status = 'ENDED' AND runs.environment_id = ANY(:env_ids)
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
    GROUP BY v.id, v.name, v.unit, v.value
)
, execution_devs AS (
    SELECT
        runs.id AS run_id
      , m.metric_id
      , m.name
      , m.scope
      , m.unit
      , avg(m.value) AS mean
      , min(m.value) AS min
      , max(m.value) AS max
      , stddev(m.value) AS stddev
    FROM execution_measurements em
    JOIN executions ex ON ex.id = em.execution_id
    JOIN benchmark_runs runs ON runs.id = ex.benchmark_run_id
    JOIN measurements m ON m.id = em.measurement_id
    WHERE runs.environment_id = ANY(:env_ids)
    GROUP BY 1, 2, 3, 4, 5
)
, run_pairs AS (
    SELECT
        ex_left.name AS metric
      , ex_left.scope
      , ex_left.unit AS unit
      -- result
      , cast(100 * (ex_right.mean - ex_left.mean) / nullif(cast(greatest(ex_right.mean, ex_left.mean) as real), 0) AS decimal(5,2)) AS diff_pct
    FROM runs run_left
    JOIN runs run_right ON run_left.environment_id != run_right.environment_id AND run_left.properties = run_right.properties
    JOIN execution_devs ex_left ON ex_left.run_id = run_left.id
    JOIN execution_devs ex_right ON ex_right.run_id = run_right.id AND ex_left.metric_id = ex_right.metric_id
    JOIN environments env_left ON env_left.id = run_left.environment_id
    JOIN environments env_right ON env_right.id = run_right.environment_id
    -- don't count the same pair twice
    WHERE env_left.name < env_right.name
)
, pair_stats AS (
    SELECT
        metric
      , scope
      , unit
      , min(diff_pct) as min
      , max(diff_pct) as max
    FROM run_pairs
    GROUP BY metric, scope, unit
)
, dimensions AS (
    SELECT
        metric
      , scope
      , unit
    FROM run_pairs
    GROUP BY metric, scope, unit
)
, histogram as (
    SELECT
        metric
      , scope
      , unit
      , width_bucket(diff_pct, s.min, nullif(s.max, s.min), 9) AS bucket
      , numrange(min(diff_pct), max(diff_pct), '[]') AS range
      , count(*) AS freq
    FROM run_pairs r
    JOIN pair_stats s USING (metric, scope, unit)
    GROUP BY metric, scope, unit, bucket
)
SELECT
    d.metric AS metric
  , d.scope AS scope_label
  , d.unit AS unit_group
  , s.bucket AS bucket
  , range AS diff_pct_range
  , coalesce(freq, 0) AS occurrences_num
  , repeat('■', (coalesce(freq, 0)::float / max(freq) over() * 30)::int) AS bar_chart_label
FROM dimensions d
CROSS JOIN generate_series(1, 10) s(bucket)
LEFT JOIN histogram h ON (d.metric, d.scope, d.unit, s.bucket) = (h.metric, h.scope, h.unit, h.bucket)
ORDER BY d.metric, d.scope, d.unit, s.bucket
;
