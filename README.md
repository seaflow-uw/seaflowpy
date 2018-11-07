A Python package for SeaFlow flow cytometer data.

### Requirements
seaflowpy has been tested against Python 3.6 and 3.7.

### Install
It's good practice to install packages in a conda environment or virtualenv virtual environment. Here we'll assume we're installing into a conda environment.

```sh
git clone https://github.com/armbrustlab/seaflowpy
cd seaflowpy
# Create a conda environment with only pip installed
conda create -n seaflowpy pip
conda activate seaflowpy
pip install .
# Confirm the seaflowpy command-line tool is accessible
seaflowpy version
```

Experienced users of conda might have expected pandas and numpy to be installed through conda. In this case we're pulling pandas and numpy from PyPI with pip. This is for performance reasons. The default numpy package in conda makes use of the MKL (Math Kernel Library for Intel processors). While this generally results in a performance improvements with numpy, given the way that seaflowpy handles parallel processing MKL ends up significantly degrading multicore performance. To be clear this is most likely not a problem with numpy + MKL, just with the way that seaflowpy interacts with numpy + MKL in their default configuration. Future work will be exploring ways to configure numpy + MKL to play nice with seaflowpy multicore workloads.

### Testing
It is recommended to run tests using `tox`. Install `tox` with `pip`, then from the `seaflowpy` source directory just run `tox`. This will install `seaflowpy` into a temporary virtual environment and run `pytest` tests against that installation.

It's also possible to run `pytest` from the source directory to test directly against the version of `seaflowpy` installed in the current environment.

### Command-line interface
All `seaflowpy` CLI tools are accessible from the `seaflowpy` executable. Run `seaflowpy --help` to begin exploring the CLI usage documentation.

#### SFL validation workflow
SFL validation sub-commands are available under the `seaflowpy sfl` command. The usage details for each command can be accessed as `seaflowpy sfl <cmd> -h`.

##### `seaflowpy sfl convert-gga`
Converts GGA coordinate values to decimal degree. Otherwise the file is unchanged.

##### `seaflowpy sfl db`
Import an SFL file into a popcycle database file. The database file will be created if needed. Various checks are run (the same as `seaflowpy sfl validate`) before import. If errors are encountered a summary is printed and import is aborted.

##### `seaflowpy sfl dedup`
Remove lines in an SFL file with duplicate "FILE" values. Because it's impossible to know which of the duplicated SFL entries corresponds to which EVT file, all duplicate rows are removed. A unique list of removed files is printed to STDERR.

##### `seaflowpy sfl manifest`
Compare EVT files listed in an SFL file with EVT files on-disk or in cloud object storage. This can serve as a quick sanity check for the internal consistency of a SeaFlow cruise data folder. Note, it's normal for one file to be missing from the SFL or EVT day of year folder around midnight.

##### `seaflowpy sfl print`
Print a standard version of an SFL file with only the necessary columns. The correct day of year folder will be added to "FILE" column values if not present. "DATE" column will be created if not present from "FILE" column values (only applies to new-style datestamped file names). Any other required columns which are missing will be created with "NA" values.

##### `seaflowpy sfl validate`
Validate key values in an SFL file. The following checks are performed:
* all required columns are present
* "FILE" column values have day of year folder prefixes, are in the proper format, in chronological order, and are unique
* "DATE" column values are in the proper format, represent valid date and times, and are UTC
* "LAT" and "LON" coordinate column values are valid decimal degree values

Because some of these errors can affect every row of the file (e.g. out of order files), only the first error of each type is printed by default. To get a full printout of all errors run the command with `--verbose`.
