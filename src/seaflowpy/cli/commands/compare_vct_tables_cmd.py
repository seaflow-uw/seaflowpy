"""Compare equality of VCT tables."""
import sqlite3
import click
import numpy.testing as npt
import pandas as pd


@click.command()
@click.argument('db1', nargs=1, type=click.Path())
@click.argument('db2', nargs=1, type=click.Path())
def compare_vct_tables_cmd(db1, db2):
    """Comparse VCT tables from two SQLite3 popcycle databases"""
    con1 = sqlite3.connect(db1)
    con2 = sqlite3.connect(db2)

    df1 = pd.read_sql_query("SELECT * FROM vct ORDER BY file,quantile,pop", con1)
    df2 = pd.read_sql_query("SELECT * FROM vct ORDER BY file,quantile,pop", con2)

    df1_numbers = df1.drop(["file", "quantile", "pop", "filter_id", "gating_id"], axis="columns")
    df2_numbers = df2.drop(["file", "quantile", "pop", "filter_id", "gating_id"], axis="columns")

    try:
        npt.assert_array_equal(df1["filter_id"], df2["filter_id"])
    except AssertionError:
        click.echo("filter_id columns differ")
    else:
        click.echo("filter_id columns are equal")

    try:
        npt.assert_array_equal(df1["gating_id"], df2["gating_id"])
    except AssertionError:
        click.echo("gating_id columns differ")
    else:
        click.echo("gating_id columns are equal")

    try:
        npt.assert_array_equal(df1[["file", "quantile", "pop"]], df2[["file", "quantile", "pop"]])
    except AssertionError:
        click.echo("(file, quantile, pop) columns differ")
    else:
        click.echo("(file, quantile, pop) columns are equal")

    try:
        npt.assert_array_equal(df1_numbers, df2_numbers)
    except AssertionError:
        click.echo("VCT table numbers are not exactly equal")
        try:
            npt.assert_allclose(df1_numbers, df2_numbers)
        except AssertionError:
            click.echo("VCT tables numbers are not nearly equal to within 1e-7 relative tolerance")
        else:
            click.echo("VCT tables numbers are nearly equal to within 1e-7 relative tolerance")
    else:
        click.echo("VCT table numbers are exactly equal")
