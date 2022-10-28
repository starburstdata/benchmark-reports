-- Executions query info
SELECT
    ex.benchmark_run_id as run_id
  , ex.id as execution_id
  , q.info::jsonb AS query_info_json
FROM executions ex
JOIN query_info q ON q.id = ex.query_info_id
WHERE ex.benchmark_run_id = ANY(:ids)
ORDER BY ex.benchmark_run_id, ex.id, q.id
LIMIT 1
