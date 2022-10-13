-- Run details
-- Reads the names and values of all attributes and variables associated with a specific benchmark run.
WITH
properties AS (
    SELECT
        'attribute' AS type
      , name
      , value
    FROM benchmark_runs_attributes
    WHERE benchmark_run_id = :id
    UNION ALL
    SELECT
        'variable' AS type
      , name
      , value
    FROM benchmark_runs_variables
    WHERE benchmark_run_id = :id
)
SELECT
    name
  , value
FROM properties
ORDER BY type, name, value
;