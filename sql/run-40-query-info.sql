-- Executions query info
SELECT
    ex.id as execution_id
  , q.info::jsonb AS query_info_json
FROM executions ex
JOIN query_info q ON q.id = ex.query_info_id
WHERE ex.benchmark_run_id = :id
ORDER BY ex.id, q.id
LIMIT 1
