import pathlib

import pandas as pd
import pytest
from click.testing import CliRunner

from seaflowpy.cli import run

OPP_DIR = pathlib.Path("tests/testcruise_opp_one_param")
OPP_FILE_0 = OPP_DIR / "2014-07-04T00-00-00+00-00.1H.opp.parquet"
OPP_FILE_1 = OPP_DIR / "2014-07-04T01-00-00+00-00.1H.opp.parquet"


@pytest.fixture()
def runner():
    return CliRunner()


# ---------------------------------------------------------------------------
# opp sample-hourly
# ---------------------------------------------------------------------------

class TestSampleHourly:
    def test_basic(self, runner, tmp_path):
        """Each input file produces an output file with the same name."""
        result = runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            str(OPP_FILE_0), str(OPP_FILE_1),
        ])
        assert result.exit_code == 0, result.output
        assert (tmp_path / OPP_FILE_0.name).exists()
        assert (tmp_path / OPP_FILE_1.name).exists()

    def test_count_respected(self, runner, tmp_path):
        """Output has at most count rows per file."""
        result = runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            "-c", "5",
            str(OPP_FILE_0), str(OPP_FILE_1),
        ])
        assert result.exit_code == 0, result.output
        df0 = pd.read_parquet(tmp_path / OPP_FILE_0.name)
        df1 = pd.read_parquet(tmp_path / OPP_FILE_1.name)
        assert len(df0) <= 5
        assert len(df1) <= 5

    def test_seed_reproducible(self, runner, tmp_path):
        """Same seed produces identical output on repeated runs."""
        args = [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            "-c", "10",
            "-s", "42",
            str(OPP_FILE_0),
        ]
        runner.invoke(run, args)
        df_first = pd.read_parquet(tmp_path / OPP_FILE_0.name).reset_index(drop=True)

        # Delete output so the second run re-samples (different file_id set check
        # would skip; removing ensures a fresh sample)
        (tmp_path / OPP_FILE_0.name).unlink()
        runner.invoke(run, args)
        df_second = pd.read_parquet(tmp_path / OPP_FILE_0.name).reset_index(drop=True)

        pd.testing.assert_frame_equal(df_first.sort_values(list(df_first.columns)).reset_index(drop=True),
                                      df_second.sort_values(list(df_second.columns)).reset_index(drop=True))

    def test_output_columns_preserved(self, runner, tmp_path):
        """Output parquet has the same columns as the source."""
        runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            str(OPP_FILE_0),
        ])
        src = pd.read_parquet(OPP_FILE_0)
        out = pd.read_parquet(tmp_path / OPP_FILE_0.name)
        assert list(out.columns) == list(src.columns)

    def test_min_date_filters_files(self, runner, tmp_path):
        """--min-date excludes files whose timestamp is before the cutoff."""
        result = runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            "--min-date", "2014-07-04T01:00:00+00:00",
            str(OPP_FILE_0), str(OPP_FILE_1),
        ])
        assert result.exit_code == 0, result.output
        assert not (tmp_path / OPP_FILE_0.name).exists()
        assert (tmp_path / OPP_FILE_1.name).exists()

    def test_max_date_filters_files(self, runner, tmp_path):
        """--max-date excludes files whose timestamp is after the cutoff."""
        result = runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            "--max-date", "2014-07-04T00:00:00+00:00",
            str(OPP_FILE_0), str(OPP_FILE_1),
        ])
        assert result.exit_code == 0, result.output
        assert (tmp_path / OPP_FILE_0.name).exists()
        assert not (tmp_path / OPP_FILE_1.name).exists()

    def test_smart_cache_skips_unchanged_file(self, runner, tmp_path):
        """A second run skips files whose file_id set has not changed."""
        args = [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            str(OPP_FILE_0),
        ]
        runner.invoke(run, args)
        mtime_before = (tmp_path / OPP_FILE_0.name).stat().st_mtime

        runner.invoke(run, args)
        mtime_after = (tmp_path / OPP_FILE_0.name).stat().st_mtime

        assert mtime_before == mtime_after  # file was not rewritten

    def test_smart_cache_resamples_when_source_changes(self, runner, tmp_path):
        """A second run resamples when the source file has new file_ids."""
        args = [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            str(OPP_FILE_0),
        ]
        runner.invoke(run, args)
        mtime_before = (tmp_path / OPP_FILE_0.name).stat().st_mtime

        # Write a modified source with an extra fake file_id row
        src_df = pd.read_parquet(OPP_FILE_0)
        extra = src_df.iloc[:1].copy()
        extra["file_id"] = "fake/new-file-id"
        modified_src = pathlib.Path(tmp_path) / "src" / OPP_FILE_0.name
        modified_src.parent.mkdir()
        pd.concat([src_df, extra], ignore_index=True).to_parquet(modified_src)

        runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            str(modified_src),
        ])
        mtime_after = (tmp_path / OPP_FILE_0.name).stat().st_mtime

        assert mtime_after > mtime_before  # file was rewritten

    def test_directory_input(self, runner, tmp_path):
        """Passing a directory finds all .opp.parquet files within it."""
        result = runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            str(OPP_DIR),
        ])
        assert result.exit_code == 0, result.output
        assert (tmp_path / OPP_FILE_0.name).exists()
        assert (tmp_path / OPP_FILE_1.name).exists()

    def test_no_files(self, runner, tmp_path):
        """No files produces no output and exits cleanly."""
        result = runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
        ])
        assert result.exit_code == 0, result.output
        assert list(tmp_path.iterdir()) == []

    def test_stderr_summary(self, runner, tmp_path):
        """stderr contains the expected summary lines."""
        result = runner.invoke(run, [
            "opp", "sample-hourly",
            "-o", str(tmp_path),
            str(OPP_FILE_0), str(OPP_FILE_1),
        ])
        assert "2 input files" in result.output
        assert "2 files within time window" in result.output
        assert "2 files sampled" in result.output
