# Seaflowpy

A Python package for SeaFlow flow cytometer data.

## Install

### Command-line tool as single-file download

Single file executables of the `seaflowpy` command-line tool
for MacOS and Linux can be downloaded from the project's github
[release page](https://github.com/armbrustlab/seaflowpy/releases).

### Docker

Docker image are available from Docker Hub at `ctberthiaume/seaflowpy`.

```
docker run -it ctberthiaume/seaflowpy seaflowpy version
```

The Docker build file is in this repo at `/Dockerfile`.

### PyPI

```
pip install seaflowpy
```

### Source

This will clone the repo and create a new virtual environment `seaflowpy`.
`venv` can be replaced with `virtualenv`, `conda`, etc.

```sh
git clone https://github.com/armbrustlab/seaflowpy
cd seaflowpy
python3 -m venv seaflowpy
source seaflowpy/bin/activate
pip3 install -r requirements.txt
pip3 install .
# Confirm the seaflowpy command-line tool is accessible
seaflowpy version
deactivate
```

## Integration with R

To call `seaflowpy` from R, update the PATH environment variable in
`~/.Renviron`. For example:

```sh
PATH=${PATH}:${HOME}/venvs/seaflowpy/bin
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
