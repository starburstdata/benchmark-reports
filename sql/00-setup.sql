CREATE OR REPLACE FUNCTION array_unique (a TEXT[])
RETURNS TEXT[] AS
$$
  SELECT array(
    SELECT DISTINCT v
    FROM unnest(a) AS b(v)
    ORDER BY v
  )
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION array_union(a ANYARRAY, b ANYARRAY)
RETURNS ANYARRAY AS
$$
  SELECT array_agg(x)
  FROM (
    SELECT x
    FROM (
      SELECT unnest(a) x
      UNION
      SELECT unnest(b)
    ) u
    ORDER BY x
  ) AS u
$$ LANGUAGE SQL;

CREATE OR REPLACE AGGREGATE array_union_agg(ANYARRAY) (
  SFUNC = array_union,
  STYPE = ANYARRAY,
  INITCOND = '{}'
);

CREATE OR REPLACE FUNCTION array_subtraction(a ANYARRAY, b ANYARRAY)
RETURNS anyarray AS
$$
  SELECT ARRAY(SELECT unnest(a)
               EXCEPT
               SELECT unnest(b))
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION array_sort (ANYARRAY)
RETURNS ANYARRAY AS
$$
  SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION contains (haystack ANYARRAY, needle ANYELEMENT)
RETURNS BOOLEAN AS
$$
  SELECT needle = ANY(haystack)
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION format_metric (value DOUBLE PRECISION, unit VARCHAR)
RETURNS VARCHAR AS
$$
  SELECT CASE unit
    -- trim leading @ in case IntervalStyle is set to postgres_verbose
    WHEN 'MILLISECONDS' THEN ltrim(cast(value * interval '1 millisecond' AS varchar), '@ ')
    WHEN 'BYTES' THEN pg_size_pretty(round(value::numeric, 2))
    WHEN 'PERCENT' THEN cast(value AS decimal(18,2)) || ' %'
    WHEN 'QUERY_PER_SECOND' THEN round(value::numeric, 2) || ' qps'
    ELSE value::varchar
  END
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION format_percent(value DOUBLE PRECISION)
RETURNS VARCHAR AS
$$
  SELECT CASE
    WHEN value = 0 THEN ''
    WHEN value BETWEEN 0 AND 2 THEN '▴'
    WHEN value BETWEEN 2 AND 5 THEN '△'
    WHEN value > 5 THEN '▲'
    WHEN value BETWEEN 0 AND -2 THEN '▾'
    WHEN value BETWEEN -2 AND -5 THEN '▽'
    WHEN value < -5 THEN '▼'
  END || cast(value AS decimal(18,2))
$$ LANGUAGE SQL;
