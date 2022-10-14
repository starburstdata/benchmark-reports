#!/usr/bin/env python3

"""
Read queries from files, execute them, draw plots and tables and generate a HTML report file.
"""

import argparse
import csv
import glob
import io
import logging
import numbers
import os
import re
import subprocess
import sys
import unittest
from dataclasses import dataclass, field
from functools import cache
from os import environ, path

import git
import plotly.graph_objects as go
from jinja2 import Environment, select_autoescape, PackageLoader
from slugify import slugify
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.sql.expression import text
from testcontainers.postgres import PostgresContainer

jinja_env = Environment(loader=PackageLoader("report"), autoescape=select_autoescape())
table_template = jinja_env.get_template("table.html")
env_template = jinja_env.get_template("env.html")
run_template = jinja_env.get_template("run.html")
table_css = jinja_env.get_template("table.css")

runs_query = """
SELECT id
FROM benchmark_runs
WHERE status = 'ENDED'
AND environment_id = ANY(:env_ids)
ORDER BY id
;
"""


def main():
    parser = argparse.ArgumentParser(description="Generate a performance test report")
    parser.add_argument(
        "-d",
        "--db-url",
        default=environ.get(
            "DB_URL", "postgresql+psycopg2://postgres@localhost:5432/benchto"
        ),
        help="Database URL. Do NOT include a password here, rely on the driver to read it from a file or the environment.",
    )
    parser.add_argument(
        "-s",
        "--sql",
        default=".",
        help="Path to a directory with sql files to execute",
    )
    parser.add_argument(
        "-e",
        "--environments",
        default=environ.get("ENVIRONMENTS", "%"),
        action=SplitArgs,
        help="Names of environments to include",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="-",
        help="Filename to write the report to",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.WARNING,
        help="Print info level logs",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="test this script instead of executing it",
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    if args.test:
        sys.argv = [sys.argv[0]]
        unittest.main()
        return

    logging.debug("Connecting to the database")
    engine = create_engine(args.db_url, connect_args={})
    connection = engine.connect()

    output = sys.stdout
    basedir = None
    if args.output != "-":
        output = open(args.output, "w")
        basedir = path.abspath(path.dirname(args.output))

    print_report(connection, args.sql, args.environments, output, basedir=basedir)

    if args.output != "-":
        output.close()


class SplitArgs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(","))


def print_report(connection, sql, environments, output, basedir=None):
    """Print report for all report queries and selected environments"""
    logging.debug("Setup reports")

    sql_glob = glob.escape(sql) + "/??-*.sql"
    logging.info("Loading queries from %s", sql_glob)
    input_files = [f for f in sorted(glob.glob(sql_glob))]
    logging.debug("Query files: %s", input_files)

    logging.info("Resolving environments")
    params = {f"e{i}": name for i, name in enumerate(environments)}
    constraints = [f"name LIKE :{key}" for key in params.keys()]
    query = f"SELECT id, name FROM environments WHERE {' OR '.join(constraints)} ORDER BY name"
    rows = connection.execute(text(query), params).fetchall()
    logging.info("Report for environment names: %s", [r["name"] for r in rows])
    env_ids = [r["id"] for r in rows]

    logging.debug("Setup done, generating reports")
    reports = [read_report(f) for f in input_files]
    reports = add_figures(reports, connection, env_ids, basedir)

    logging.debug("Printing reports")
    # TODO use a template engine like Jinja2, add intro with links to data model,
    # explain why some metrics are selected (duration, mem, total cpu)
    output.write(
        f'<html><head><meta charset="utf-8" /><style>{table_css.render()}</style></head><body>'
    )
    output.write(
        '<p><a href="https://github.com/trinodb/benchto/blob/master/docs/data-model/README.md">Benchto Data Model</a>'
    )
    output.write('<div style="display: none;">')
    output.write(go.Figure().to_html(full_html=False, include_plotlyjs="cdn"))
    output.write("</div>")
    output.write("<h2>Table of Contents</h2>")
    output.write("<ol>")
    for entry in reports:
        if not entry.figures:
            # some reports might not create a figure
            continue
        output.write(f'<li><a href="#{entry.slug}">{entry.title}</a></li>')
    for entry in reports:
        if not entry.figures:
            # some reports might not create a figure
            continue

        output.write("</ol>")
        output.write(f'<h2 id="{entry.slug}">{entry.title}</h2>')
        output.write(f"<p>{entry.desc}</p>")
        output.write("<p>")
        output.write(f'<a href="{entry.file_url}">Query</a>')
        if basedir:
            file = path.join(basedir, entry.results_file)
            output.write(f', results: <a href="{file}">{entry.results_file}</a>')
        output.write("</p>")
        for fig in entry.figures:
            output.write(fig.to_html(full_html=False, include_plotlyjs=False))

    if basedir is not None:
        dump_envs_details(connection, sql, env_ids, basedir)
        dump_runs_details(connection, sql, env_ids, basedir)

    output.write("</body></html>")
    logging.debug("All done")


def dump_envs_details(connection, sql, env_ids, basedir):
    """Dump env details for all selected environments"""
    logging.debug("Listing all environments in %s", env_ids)
    env_details_sql = os.path.join(sql, "env_details.sql")
    logging.info("Loading query from %s", env_details_sql)
    env_details = read_env_details(env_details_sql)
    for env_id in env_ids:
        dump_env_details_to_file(
            os.path.join(basedir, "envs", str(env_id)),
            env_details,
            connection,
            env_id,
        )


def dump_env_details_to_file(prefix, details, connection, id):
    result = connection.execute(text(details.query), id=id)
    headers = [
        dict(value=label_from_name(key), css_class=f"align-{align_from_name(key)}")
        for key in result.keys()
    ]
    rows = [
        [
            dict(value=table_entry(row[i]), css_class=header["css_class"])
            for i, header in enumerate(headers)
        ]
        for row in result.fetchall()
    ]
    # write table to a html file
    os.makedirs(prefix, exist_ok=True)
    with open(os.path.join(prefix, details.results_file), "w") as f:
        f.write(env_template.render(headers=headers, rows=rows))


def dump_runs_details(connection, sql, env_ids, basedir):
    logging.debug("Dumping runs details for env_ids: %s", env_ids)
    runs = get_run_ids(connection, env_ids)
    sql_glob = os.path.join(glob.escape(sql), "run-*.sql")
    logging.info("Loading queries from %s", sql_glob)
    run_details_queries = [f for f in sorted(glob.glob(sql_glob))]
    logging.debug("Query files: %s", run_details_queries)
    run_details = [read_run_details(f) for f in run_details_queries]
    for run in runs:
        dump_run_details(connection, run, run_details, basedir)


def get_run_ids(connection, env_ids):
    """Get run details for all selected environments"""
    logging.debug("Listing all runs from env_ids: %s", env_ids)
    result = connection.execute(text(runs_query), env_ids=env_ids)
    return [row["id"] for row in result.fetchall()]


def dump_run_details(connection, run_id, run_details, basedir):
    for run_report in run_details:
        result = connection.execute(text(run_report.query), id=run_id)
        headers = [
            dict(value=label_from_name(key), css_class=f"align-{align_from_name(key)}")
            for key in result.keys()
        ]
        headers[-1][
            "css_class"
        ] = "align-right"  # make sure the last column is right-aligned
        rows = [
            [
                dict(value=table_entry(row[i]), css_class=header["css_class"])
                for i, header in enumerate(headers)
            ]
            for row in result.fetchall()
        ]
        run_report.contents = table_template.render(headers=headers, rows=rows)
    # Create summary index.html for a whole run
    os.makedirs(os.path.join(basedir, "runs", str(run_id)), exist_ok=True)
    with open(os.path.join(basedir, "runs", str(run_id), "index.html"), "w") as f:
        f.write(run_template.render(run_id=run_id, reports=run_details, sha=sha()))


@dataclass(init=True)
class Table:
    """HTML table for showing raw data"""

    headers: dict = field(default_factory=dict)
    rows: list = field(default_factory=list)

    def to_html(self, **kwargs):
        return table_template.render(headers=self.headers, rows=self.rows)


@dataclass
class Report:
    """Report of performance test results."""

    file: str
    file_url: str = field(init=False)
    results_file: str = field(init=False)
    query: str
    title: str
    slug: str = field(init=False)
    desc: str
    tables: list[Table] = field(default_factory=list)
    figures: list[go.Figure] = field(default_factory=list)

    def __post_init__(self):
        self.file_url = f"https://github.com/starburstdata/benchmark-reports/blob/{sha()}/{self.file}"
        self.slug = slugify(self.title)
        self.results_file = self.slug + ".csv"


@dataclass
class EnvDetails:
    """Details regarding an environment"""

    file: str
    results_file: str = field(init=False)
    query: str
    title: str
    slug: str = field(init=False)
    desc: str

    def __post_init__(self):
        self.slug = slugify(self.title)
        self.results_file = self.slug + ".html"


@dataclass
class RunDetails:
    """Details regarding a run"""

    file: str
    contents: str = field(init=False)
    query: str
    title: str
    slug: str = field(init=False)
    desc: str

    def __post_init__(self):
        self.slug = slugify(self.title)


@cache
def sha():
    try:
        repo = git.Repo(search_parent_directories=True)
    except git.InvalidGitRepositoryError:
        try:
            with open("version", "r") as f:
                return f.read()
        except FileNotFoundError:
            return "main"
    return repo.head.object.hexsha


def read_env_details(file):
    desc, query, title = read_query(file)
    return EnvDetails(file, query, title, desc)


def read_run_details(file):
    desc, query, title = read_query(file)
    return RunDetails(file, query, title, desc)


def read_report(file):
    desc, query, title = read_query(file)
    return Report(file, query, title, desc)


def read_query(file):
    """Read query and turn leading comments into a title and description"""
    query = ""
    title = ""
    desc = ""
    with open(file, "r") as f:
        for line in f:
            if line.startswith("--") and not query:
                if not title:
                    title = line.lstrip("-")
                else:
                    desc += line.lstrip("-")
                continue
            query += line
    return desc, query, title


def add_figures(reports, connection, env_ids, basedir):
    """Add figures to reports by executing queries"""
    for entry in reports:
        logging.debug("Fetching results for: %s", entry.file)
        result = connection.execute(text(entry.query), env_ids=env_ids)

        if not entry.title:
            # ignore results, this is supposed to be a setup query
            continue

        rows = result.fetchall()
        if basedir:
            # write results to a csv file
            with open(path.join(basedir, entry.results_file), "w") as f:
                writer = csv.writer(f)
                writer.writerow([name for name in result.keys()])
                writer.writerows(rows)
        # create figures
        entry.figures = figures(result.keys(), rows)
    return reports


def figures(columns, rows):
    """Figure from data rows"""

    result = []
    # TODO handle different scales - split into subplots
    # TODO if there are too many X values, split last ones into subplots until threshold
    group_by = []
    groups = set(frozenset(("", "")))
    if "unit" in columns:
        group_by = ["unit"]
        # groups are sets of tuples, because dicts are not hashable
        groups = set(
            frozenset((key, str(row[key])) for key in group_by) for row in rows
        )

    for group in sorted(groups, key=sorted):
        logging.debug("Rendering group %s", group)
        names = [name for name in columns if name not in group_by]
        group_rows = [row for row in rows if row_in_group(row, group)]
        result += add_table(names, group_rows, group)
        result += add_barchart(names, group_rows, group)
    return result


def add_table(columns, rows, group):
    # TODO link some (id?) columns
    headers = [
        dict(value=label_from_name(key), css_class=f"align-{align_from_name(key)}")
        for key in columns
    ]
    rows = [
        [
            dict(value=table_entry(row[i]), css_class=header["css_class"])
            for i, header in enumerate(headers)
        ]
        for row in rows
    ]
    fig = Table(headers=headers, rows=rows)
    return [fig]


def table_entry(item):
    if item is None:
        return None
    if isinstance(item, numbers.Number):
        return f'<pre class="numeric">{ item }</pre>'
    trimmed = str(item).strip("\n")
    if trimmed.endswith("</a>"):
        return trimmed
    if trimmed.count("\n") < 5:
        return f"<pre>{ trimmed }</pre>"
    return "<details><summary>{}</summary><pre>{}</pre></details>".format(
        trimmed[: trimmed.find("\n")], trimmed
    )


def add_barchart(columns, rows, group):
    result = []
    dimensions = [key for key in columns if is_dimension(key)]
    pivot_by = [key for key in columns if is_pivot(key)]
    metrics = [key for key in columns if is_metric(key)]
    labels = [key for key in columns if is_label(key)]
    if not metrics:
        return result
    if group == frozenset([("unit", "None")]):
        # TODO this should be used to generate groups/subplots
        logging.warning("Skipping group %s with %d rows", group, len(rows))
        return result
    # detect and create errors series
    errors = {}
    for key in metrics:
        if not key.endswith("_err"):
            continue
        # note we get the label without the _err suffix
        errors[label_from_name(key[:-4])] = [row[key] for row in rows]
    fig = go.Figure()
    pivot_sets = set(
        frozenset((key, str(row[key])) for key in pivot_by) for row in rows
    )
    for pivot_set in sorted(pivot_sets, key=sorted):
        logging.debug("Rendering pivot set %s", pivot_set)
        pivot_rows = rows
        label_prefix = ""
        if pivot_set:
            pivot_rows = [row for row in rows if row_in_group(row, pivot_set)]
            label_prefix = (
                ", ".join(
                    f"{label_from_name(name)}: {value}"
                    for name, value in sorted(pivot_set)
                )
                + " "
            )
        add_bar_trace(
            fig, pivot_rows, dimensions, metrics, errors, labels, label_prefix
        )
    # TODO using last metric as tickformat, is this correct?
    fig.update_layout(
        yaxis_tickformat=column_format(metrics[-1], group),
        barmode="group",
        title=dict(text=", ".join(f"{key}: {value}" for key, value in group)),
    )
    result.append(fig)
    return result


def row_in_group(row, group):
    for name, value in group:
        if row[name] != value:
            return False
    return True


def add_bar_trace(fig, rows, dimensions, metrics, errors, labels, label_prefix=""):
    x = []
    for row in rows:
        values = [str(row[key]) for key in dimensions]
        x.append(", ".join(values))
    text = [
        "</br>".join(f"{label_from_name(key)}: {row[key]}" for key in labels)
        for row in rows
    ]
    for metric in metrics:
        if metric.endswith("_err"):
            continue
        label = label_from_name(metric)
        error = None
        if label in errors:
            error = dict(type="data", array=errors.get(label))
        y = [row[metric] for row in rows]
        fig.add_trace(
            go.Bar(
                name=label_prefix + label_from_name(metric),
                x=x,
                y=y,
                error_y=error,
                hovertext=text,
            )
        )


def is_dimension(name):
    """Is dimension based on the column name suffix"""
    suffix = name.split("_").pop()
    # TODO handle decimals other than 2
    return suffix not in ("num2f", "num", "pct", "unit", "err", "label", "pivot")


def is_pivot(name):
    """Is pivot based on the column name suffix"""
    suffix = name.split("_").pop()
    return suffix in ("pivot")


def is_metric(name):
    """Is metric based on the column name suffix"""
    suffix = name.split("_").pop()
    # Note: missing the label suffix
    return suffix in ("num2f", "num", "pct", "unit", "err")


def is_label(name):
    """Is label based on the column name suffix"""
    suffix = name.split("_").pop()
    return suffix in ("label")


def label_from_name(name):
    """Label from column name"""
    words = name.split("_")
    # Note: missing the err suffix
    if words[-1] in ("num2f", "num", "pct", "unit", "label", "pivot"):
        words.pop()
    return " ".join(word.capitalize() for word in words)


def column_format(name, group):
    """Column format expression"""
    # reference: https://github.com/d3/d3-format/tree/v1.4.5#d3-format
    lcname = name.lower()
    words = lcname.split("_")
    suffix = words.pop()
    if suffix == "num2f":
        return ".2f"
    if suffix == "err":
        return ".2f"
    if suffix == "pct":
        return ".2%"
    if suffix == "unit":
        return format_unit(group)
    if "memory" in words or "bytes" in words:
        return ".2s"
    return ""


def format_unit(group):
    unit = ([value for name, value in group if name == "unit"] or [""]).pop()

    match unit:
        case "MILLISECONDS":
            # TODO displaing this as intervals (H:M:S.MMM) would require setting tickvals and hovertemplate
            return "g"
        case "BYTES":
            return ".2s"
        case "PERCENT":
            return ".2%"
        case "QUERY_PER_SECOND":
            return ".2f"
    return "g"


def align_from_name(name):
    """Align from column name"""
    if is_metric(name):
        return "right"
    return "left"


class TestReport(unittest.TestCase):
    def test_report(self):
        with PostgresContainer("postgres:latest") as postgres:
            self.restore(postgres.get_connection_url(), "testdata/backup.dump")
            url = make_url(postgres.get_connection_url())
            engine = create_engine(url.set(database="benchto"))

            with open("testdata/expected.html", "r") as f:
                expected = f.read()

            output = io.StringIO()
            print_report(engine.connect(), "sql", "%", output)
            actual = output.getvalue()
            # replace the parts that are expected to always change to make the diff more meaningful
            replacements = [
                (
                    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                    "00000000-1111-1111-1111-222222222222",
                ),
                (r"benchmark-reports/blob/[0-9a-f]{40}/", "benchmark-reports/blob/x/"),
            ]
            for regex, replacement in replacements:
                actual = re.sub(regex, replacement, actual)
                expected = re.sub(regex, replacement, expected)
            # only check the length, because reports contain random UUIDs, this is enough for a smoke test
            try:
                self.assertEqual(len(actual), len(expected))
            except AssertionError:
                with open("testdata/actual.html", "w") as f:
                    f.write(actual)
                self.assertEqual(actual, expected)
            output.close()

    def restore(self, url, filename):
        subprocess.run(
            [
                "pg_restore",
                "-d",
                url.replace("+psycopg2", ""),
                "--create",
                "--exit-on-error",
                "--no-owner",
                filename,
            ]
        )


if __name__ == "__main__":
    main()
