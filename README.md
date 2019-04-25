# Seaflowpy

A Python package for SeaFlow flow cytometer data.

## Requirements

See `environment.yaml` for requirements

## Install

This will clone the repo and create a new conda environment `seaflowpy`.

```sh
git clone https://github.com/armbrustlab/seaflowpy
cd seaflowpy
# Edit this file to create an environment with a different name
conda env create -f environment.yml
conda activate seaflowpy
# Confirm the seaflowpy command-line tool is accessible
seaflowpy version
```

### Integration with R

To call `seaflowpy` from R, update the PATH environment variable in
`~/.Renviron`. For example:

```sh
PATH=${PATH}:${HOME}/miniconda3/envs/seaflowpy/bin
```

## Testing

Seaflowpy uses `pytest` for testing. Tests can be run from this directory as
`pytest` to test the installed version of the package, or run `tox` to install
the source into a temporary virtual environment for testing.

## Command-line interface

All `seaflowpy` CLI tools are accessible from the `seaflowpy` executable.
Run `seaflowpy --help` to begin exploring the CLI usage documentation.

### SFL validation workflow

SFL validation sub-commands are available under the `seaflowpy sfl` command.
The usage details for each command can be accessed as `seaflowpy sfl <cmd> -h`.

#### `seaflowpy sfl convert-gga`

Converts GGA coordinate values to decimal degree. Otherwise the file is
unchanged.

#### `seaflowpy sfl db`

Import an SFL file into a popcycle database file.
The database file will be created if needed.
Various checks are run before import (the same as `seaflowpy sfl validate`).
If errors are encountered a summary is printed and import is aborted.

#### `seaflowpy sfl dedup`

Remove lines in an SFL file with duplicate "FILE" values.
Because it's impossible to know which of the duplicated SFL entries
corresponds to which EVT file, all duplicate rows are removed.
A unique list of removed files is printed to STDERR.

#### `seaflowpy sfl manifest`

Compare EVT files listed in an SFL file with EVT files on-disk
or in cloud object storage.
This can serve as a quick sanity check for the internal consistency of a
SeaFlow cruise data folder.
NB, it's normal for one file to be missing from the SFL file
or EVT day of year folder around midnight.

#### `seaflowpy sfl print`

Print a standard version of an SFL file with only the necessary columns.
The correct day of year folder will be added to "FILE" column values if not
present. "DATE" column will be created if not present from "FILE" column values
(only applies to new-style datestamped file names).
Any other required columns which are missing will be created with "NA" values.

#### `seaflowpy sfl validate`

Validate key values in an SFL file. The following checks are performed:

* all required columns are present
* "FILE" column values have day of year folders, are in the proper format,
in chronological order, and are unique
* "DATE" column values are in the proper format, represent valid date and times,
and are UTC
* "LAT" and "LON" coordinate column values are valid decimal degree values

Because some of these errors can affect every row of the file
(e.g. out of order files), only the first error of each type is printed.
To get a full printout of all errors run the command with `--verbose`.
