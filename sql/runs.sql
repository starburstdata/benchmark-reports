SELECT id
FROM benchmark_runs
WHERE status = 'ENDED'
AND environment_id = ANY(:env_ids)
ORDER BY id
;
