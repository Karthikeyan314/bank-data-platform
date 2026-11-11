"""Stand-ins for dbt's cross-database macros, so SQLFluff can render models offline.

They return DuckDB SQL equivalent to what dbt-duckdb generates; only the shape matters
for linting.
"""


def dateadd(datepart, interval, from_date_or_timestamp):
    return f"({from_date_or_timestamp} + ({interval}) * interval '1 {datepart}')"


def date_trunc(datepart, date):
    return f"date_trunc('{datepart}', {date})"


def datediff(first_date, second_date, datepart):
    return f"date_diff('{datepart}', {first_date}, {second_date})"


def last_day(date, datepart):
    return f"last_day({date})"
