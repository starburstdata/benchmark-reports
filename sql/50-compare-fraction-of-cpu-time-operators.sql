-- CPU time distribution among operators
-- This graph presents what fraction of CPU time was consumed by particular operator, e.g. "ScanFilterAndProjectOperator, 0.68"
-- means that ScanFilterAndProjectOperator used 68% of the CPU time used by query.
-- Observing the increase of CPU time fraction for one operator could mean that the another operator was optimized and
-- its fraction was decreased (and as a side effect the fraction of the former one is higher).
with runs as (
	select
	  runs.id as run_id
	  , runs.name as benchmark
	  , run_vars.value as query
	  , e.name as environment_name
	  , ex.id execution_id
	  , qi.id as query_info_id
	  , duration_to_seconds(qi.info::jsonb->'queryStats'->>'totalCpuTime') as total_cpu_time
	  , qi.info as query_info
	from
	  benchmark_runs runs
      join benchmark_runs_variables run_vars on runs.id = run_vars.benchmark_run_id AND run_vars.name='query'
      join executions ex on ex.benchmark_run_id = runs.id
	  join query_info qi on qi.id=ex.query_info_id AND qi.info::jsonb->>'state' = 'FINISHED'
	  join environments e on runs.environment_id=e.id
	where
	  runs.status = 'ENDED' and
	  e.id = any(:env_ids)
),
operator_summaries as (
	select
		*,
		jsonb_array_elements(query_info::jsonb#>'{queryStats, operatorSummaries}') as operator_summary
	from runs
),
operators_stats as (
	select
		environment_name,
		benchmark,
		query,
		query_info_id,
		run_id,
		operator_summary->>'operatorType' as operator_type,
		sum(duration_to_seconds(operator_summary->>'addInputCpu') + duration_to_seconds(operator_summary->>'getOutputCpu') + duration_to_seconds(operator_summary->>'finishCpu')) / max(total_cpu_time) as operator_cpu_time_fraction
	from
	  operator_summaries
	group by environment_name, benchmark, run_id, query, query_info_id, operator_type
)
select
	environment_name as env_name_pivot,
	operator_type,
	round(avg(operator_cpu_time_fraction)::numeric, 2) as avg_operator_cpu_time_pct,
	stddev(operator_cpu_time_fraction) as avg_operator_cpu_time_err
from
	operators_stats
group by
	environment_name, operator_type
order by
	environment_name, avg(operator_cpu_time_fraction) desc, operator_type
