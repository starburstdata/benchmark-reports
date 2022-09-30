-- Run comparison
-- For every group of common properties, and a set of run properties,
-- show mean duration across all environments.
WITH
attributes AS (
    SELECT
        benchmark_run_id
      , array_agg(name || '=' || value ORDER BY name, value) AS tuples
    FROM benchmark_runs_attributes
    GROUP BY 1
)
, variables AS (
    SELECT
        benchmark_run_id
      , array_agg(name || '=' || value ORDER BY name, value) AS tuples
    FROM benchmark_runs_variables
    GROUP BY 1
)
, runs AS (
    SELECT
        runs.id
      , runs.environment_id
      , runs.sequence_id
      -- extract this one selected attribute because it's best as describing the whole run, even if it's not unique
      , q.value AS query_name
      , attrs.tuples AS attributes
      , vars.tuples AS variables
      , attrs.tuples || vars.tuples AS properties
    FROM benchmark_runs runs
    LEFT JOIN attributes attrs ON attrs.benchmark_run_id = runs.id
    LEFT JOIN variables vars ON vars.benchmark_run_id = runs.id
    LEFT JOIN benchmark_runs_attributes q ON q.benchmark_run_id = runs.id AND q.name = 'query-names'
    WHERE runs.status = 'ENDED'
    AND runs.environment_id = ANY(:env_ids)
)
, common_properties AS (
    SELECT
        run_ids
      , array_agg(name || '=' || value ORDER BY name, value) AS properties
      , row_number() OVER (ORDER BY run_ids) AS id
    FROM (
        SELECT
            name
          , value
          , array_agg(benchmark_run_id ORDER BY benchmark_run_id) AS run_ids
        FROM (
            SELECT
                a.name
              , a.value
              , a.benchmark_run_id
            FROM benchmark_runs_attributes a
            JOIN benchmark_runs r ON r.id = a.benchmark_run_id
            WHERE r.status = 'ENDED' AND r.environment_id = ANY(:env_ids)
            UNION ALL
            SELECT
                v.name
              , v.value
              , v.benchmark_run_id
            FROM benchmark_runs_variables v
            JOIN benchmark_runs r ON r.id = v.benchmark_run_id
            WHERE r.status = 'ENDED' AND r.environment_id = ANY(:env_ids)
        ) a
        GROUP BY name, value
    ) a
    -- only get groups that include all runs, because otherwise they could overlap
    -- and it's not possible to display runs grouped like this
    WHERE cardinality(run_ids) = (SELECT count(*) FROM runs)
    GROUP BY run_ids
    -- this is reduntant, but it explicitly states to only get groups with more than one item
    HAVING count(*) > 1
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
, run_devs AS (
    SELECT
        runs.id
      , runs.environment_id AS environment_id
      , runs.query_name AS query_name
      , runs.properties AS properties
      , m.metric_id
      , m.name
      , m.unit
      , avg(m.value) AS mean
      , min(m.value) AS min
      , max(m.value) AS max
      , stddev(m.value) AS stddev
    FROM execution_measurements em
    JOIN executions ex ON ex.id = em.execution_id
    JOIN runs ON runs.id = ex.benchmark_run_id
    JOIN measurements m ON m.id = em.measurement_id
    WHERE m.scope = 'driver'
    GROUP BY runs.id, runs.environment_id, runs.properties, runs.query_name, m.metric_id, m.name, m.unit
)
, diffs AS (
    SELECT
        run_devs.id
      , run_devs.query_name
      , dense_rank() OVER (PARTITION BY cp.id ORDER BY run_devs.properties) AS props_num
      , env.name AS env_name
      , run_devs.name AS metric
      , run_devs.unit AS unit
      -- result
      , run_devs.mean - lag(run_devs.mean) OVER w AS diff
      , 100 * (run_devs.mean - lag(run_devs.mean) OVER w) / nullif(cast(run_devs.mean as double precision), 0) AS diff_pct
      -- details
      , run_devs.mean
      , run_devs.stddev
      , 100 * run_devs.stddev / nullif(cast(run_devs.mean as real), 0) AS stddev_pct
      , run_devs.min
      , run_devs.max
      , array_sort(array_subtraction(run_devs.properties::text[], cp.properties::text[])) AS run_properties
      , cp.id AS group_id
      , false AS is_header
    FROM run_devs
    JOIN environments env ON env.id = run_devs.environment_id
    LEFT JOIN common_properties cp ON run_devs.id = ANY(cp.run_ids)
    WINDOW w AS (PARTITION BY run_devs.properties ORDER BY env.name)
    UNION ALL
    SELECT
        -- TODO nulls or summary?
        NULL AS id
      , NULL AS query_name
      , NULL AS props_num
      , NULL AS env_name
      , NULL AS metric
      , NULL AS unit
      , NULL AS diff
      , NULL AS diff_pct
      , NULL AS mean
      , NULL AS stddev
      , NULL AS stddev_pct
      , NULL AS min
      , NULL AS max
      , array_sort(properties::text[]) AS run_properties
      , id AS group_id
      , true AS is_header
    FROM common_properties
)
SELECT
    regexp_replace(query_name, '/[^/]+$', '') AS benchmark_name
  , regexp_replace(query_name, '^.*/([^/]*?)(\.[^/.]+)?$', '\1') AS query_name
  , props_num AS props_id
  , nullif(format('[%s](runs/%s.md)', props_num, id), '[](runs/.md)') AS run_number_label
  , env_name AS environment_pivot
  , metric
  , unit
  , format_metric(diff, unit) AS diff_label
  , format_percent(diff_pct) AS diff_pct_label
  , mean AS mean_unit
  , stddev AS mean_err
  , cast(stddev_pct AS decimal(5,2)) AS err_pct_label
  , '[' || format_metric(min, unit) || ', ' || format_metric(max, unit) || ']' AS range_label
  , run_properties AS run_properties_label
FROM diffs
ORDER BY group_id, is_header DESC, benchmark_name, query_name, run_properties, env_name
;
