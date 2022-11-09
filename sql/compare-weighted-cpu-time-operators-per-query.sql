-- CPU time per query and operator weighted in total cpu time of a query
with base AS (
	SELECT
	  runs.id as run_id
	  , runs.name as benchmark
	  , run_vars.value as query
	  , e.id as environment_id
	  , ex.id execution_id
	  , qi.id as query_info_id
	  ,	jsonb_array_elements(qi.info::jsonb#>'{queryStats, operatorSummaries}') as operator_summary
	FROM
	  benchmark_runs runs,
	  benchmark_runs_variables run_vars,
	  executions ex,
	  query_info qi,
	  environments e
	WHERE
	  ex.benchmark_run_id=runs.id and
	  runs.id=run_vars.benchmark_run_id AND
	  runs.environment_id=e.id and
	  run_vars.name='query' and
	  qi.id=ex.query_info_id AND
	  runs.status = 'ENDED' AND
	  qi.info::jsonb->>'state' = 'FINISHED' AND
	  e.id = ANY(:env_ids)
),
operators_stats as (
	select
		environment_id,
		benchmark,
		query,
		query_info_id,
		run_id,
		operator_summary->>'operatorType' as operator_type,
		sum(duration_to_seconds(operator_summary->>'addInputCpu') + duration_to_seconds(operator_summary->>'getOutputCpu') + duration_to_seconds(operator_summary->>'finishCpu')) as operator_cpu_time
	FROM
	  base
	group by environment_id, benchmark, run_id, query, query_info_id, operator_type
)
select
	environment_id as env_id_pivot,
	operator_type,
	benchmark as benchmark_group,
	query as query_group,
	round(avg(operator_cpu_time)::numeric, 2) as avg_operator_cpu_time_num2f,
	round(stddev(operator_cpu_time)::numeric, 2) avg_operator_cpu_time_err
from
	operators_stats
group by
	environment_id, benchmark, query, operator_type
order by
	avg(operator_cpu_time) desc
