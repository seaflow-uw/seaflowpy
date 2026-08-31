#!/usr/bin/env python3
"""Find cruises whose gating or filter plan starts after the first sample.

For each cruise, compares the earliest start_date in a plan file from
seaflow-gating (cruises/<cruise>/<cruise>.gating_params.gating_plan.tsv) and
seaflow-multifilter (cruises/<cruise>/<cruise>.filter_params.filter_plan.tsv)
against the earliest DATE in the matching curated/<cruise>_<instrument-id>.sfl
from seaflow-sfl. A plan starting late leaves the leading samples unprocessed.

Any repo not supplied as a local checkout is shallow-cloned to a temporary
directory for the run. Exits 1 if anything was found, 2 on a setup error.
"""

import argparse
import csv
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import NoReturn

# __doc__ is None under `python -OO`, which strips docstrings.
SUMMARY = __doc__.splitlines()[0] if __doc__ else "Check SeaFlow plan start dates."
GITHUB_URL = "https://github.com/seaflow-uw/%s.git"
SFL_REPO = "seaflow-sfl"

# Plan kind -> (label, repo name, plan file suffix). Keys are --only values.
PLAN_REPOS = {
    "gating": ("gating plan", "seaflow-gating", "gating_params.gating_plan.tsv"),
    "filter": ("filter plan", "seaflow-multifilter", "filter_params.filter_plan.tsv"),
}


def parse_timestamp(text):
    """Parse an RFC3339 timestamp into an aware UTC datetime.

    Raises ValueError if the string isn't a valid timestamp.
    """
    s = text.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"  # fromisoformat only accepts Z from Python 3.11
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        # SeaFlow timestamps without an offset are UTC.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def earliest_date(path, column):
    """Scan one TSV column for the earliest timestamp.

    Returns (earliest datetime or None, list of messages for invalid values).
    """
    earliest, invalid = None, []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if reader.fieldnames is None or column not in reader.fieldnames:
            return None, [f"no {column} column"]
        for row in reader:
            value = (row.get(column) or "").strip()
            if not value:
                continue
            try:
                dt = parse_timestamp(value)
            except ValueError:
                invalid.append(f"invalid {column}: {value!r}")
                continue
            if earliest is None or dt < earliest:
                earliest = dt
    return earliest, invalid


def clone(repo, dest):
    """Shallow-clone a seaflow-uw repo into dest."""
    print(f"Cloning {repo}...", file=sys.stderr)
    subprocess.run(
        ["git", "clone", "--quiet", "--depth", "1", GITHUB_URL % repo, str(dest)],
        check=True,
    )
    return dest


def repo_dir(repo, given, tmp):
    """Return a local checkout of repo, cloning into tmp if none was given."""
    if given:
        if not given.is_dir():
            die(f"no such directory: {given}")
        return given
    try:
        return clone(repo, tmp / repo)
    except (subprocess.CalledProcessError, OSError) as err:
        die(f"could not clone {repo}: {err}")


def find_sfl(curated, cruise):
    """Return curated SFL paths for a cruise: <cruise>_<instrument-id>.sfl."""
    pattern = re.compile(re.escape(cruise) + r"_\d+\.sfl")
    return sorted(p for p in curated.iterdir() if pattern.fullmatch(p.name))


def plural(count, word):
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


def note(cruise, source, message):
    """Format one note, naming the cruise and the file the value came from."""
    return f"  {cruise:<16} {source}: {message}"


def check(cruises_dir, curated, label, suffix):
    """Compare every cruise's plan against its SFL. Returns report lists."""
    late, missing, invalid = [], [], []
    cruises = sorted(p.name for p in cruises_dir.iterdir() if p.is_dir())

    for cruise in cruises:
        plan = cruises_dir / cruise / f"{cruise}.{suffix}"
        if not plan.exists():
            missing.append(f"{cruise}   no {label} at cruises/{cruise}/{plan.name}")
            continue

        plan_source = f"{label} cruises/{cruise}/{plan.name}"
        plan_start, bad = earliest_date(plan, "start_date")
        invalid.extend(note(cruise, plan_source, msg) for msg in bad)
        if plan_start is None:
            invalid.append(
                note(cruise, plan_source, "no usable start_date, cruise skipped")
            )
            continue

        sfl_paths = find_sfl(curated, cruise)
        if not sfl_paths:
            missing.append(f"{cruise}   (looked for curated/{cruise}_<id>.sfl)")
            continue

        sfl = sfl_paths[0]
        sfl_source = f"SFL curated/{sfl.name}"
        first_sample, bad = earliest_date(sfl, "DATE")
        invalid.extend(note(cruise, sfl_source, msg) for msg in bad)
        if first_sample is None:
            invalid.append(note(cruise, sfl_source, "no usable DATE, cruise skipped"))
            continue

        if plan_start > first_sample:
            late.append((cruise, plan_start, first_sample))

    return cruises, late, missing, invalid


def report(label, cruises, late, missing, invalid):
    """Print one repo's findings; returns the exit code for that repo."""
    if late:
        print(f"Late {label} start ({plural(len(late), 'cruise')}):")
        for cruise, plan_start, first_sample in late:
            print(
                f"  {cruise:<16} plan {plan_start.isoformat()}"
                f"   first sample {first_sample.isoformat()}"
                f"   late by {plan_start - first_sample}"
            )
        print()

    if missing:
        print(f"Missing files ({len(missing)}):")
        for line in missing:
            print(f"  {line}")
        print()

    if invalid:
        print(f"Invalid timestamps ({len(invalid)}):")
        for line in invalid:
            print(line)
        print()

    print(f"Checked {plural(len(cruises), 'cruise')}, "
          f"{plural(len(late), 'late start')}.")
    return 1 if (late or missing or invalid) else 0


def die(message) -> NoReturn:
    """Print an error and exit; never returns, so callers can't get None back."""
    print(f"error: {message}", file=sys.stderr)
    sys.exit(2)


def main():
    parser = argparse.ArgumentParser(
        description=SUMMARY,
        epilog="Any repo not given as a local checkout is cloned for the run.",
    )
    parser.add_argument(
        "--only",
        choices=sorted(PLAN_REPOS),
        help="check only gating plans or only filter plans (default: both)",
    )
    parser.add_argument(
        "--gating-dir", type=Path, help="existing seaflow-gating checkout"
    )
    parser.add_argument(
        "--filter-dir", type=Path, help="existing seaflow-multifilter checkout"
    )
    parser.add_argument(
        "--sfl-dir",
        type=Path,
        help="existing seaflow-sfl checkout (repo root or its curated/ dir)",
    )
    args = parser.parse_args()

    kinds = [args.only] if args.only else list(PLAN_REPOS)
    given = {"gating": args.gating_dir, "filter": args.filter_dir}

    with tempfile.TemporaryDirectory(prefix="seaflow-plan-check-") as tmpdir:
        tmp = Path(tmpdir)

        sfl_root = repo_dir(SFL_REPO, args.sfl_dir, tmp)
        curated = sfl_root / "curated"
        if not curated.is_dir():
            curated = sfl_root  # --sfl-dir may point straight at curated/
        if not any(curated.glob("*.sfl")):
            die(f"no curated SFL files found in {curated}")

        status = 0
        for index, kind in enumerate(kinds):
            label, repo, suffix = PLAN_REPOS[kind]
            cruises_dir = repo_dir(repo, given[kind], tmp) / "cruises"
            if not cruises_dir.is_dir():
                die(f"no cruises directory at {cruises_dir}")
            if index:
                print()
            print(f"=== {repo}: {label}s ===")
            status |= report(label, *check(cruises_dir, curated, label, suffix))
        return status


if __name__ == "__main__":
    sys.exit(main())
