A Python library for SeaFlow data.

## Installation

### Prerequisites
[Install Anaconda](https://www.continuum.io/downloads) for Python 2.7, or make sure you have a build environment setup to install numpy.

### Install

```sh
git clone https://github.com/armbrustlab/seaflowpy
cd seaflowpy
python setup.py install
```

This installs the `seaflowpy` Python package as well as the following command-line scripts:

* `seaflowpy_filter` - filter EVT data
* `seaflowpy_classify` - classify OPP data
* `seaflowpy_importsfl` - import SFL files to a SQLite3 database
* `seaflowpy_exportsflstat` - export SFL and population tables from a SQLite3 database
* `seaflowpy_sds2sfl` - convert a legacy SDS file to SFL format


### Test
You can test that `seaflowpy` is working properly on your system with

```sh
python setup.py test
```

## seaflowpy package

See [https://github.com/armbrustlab/seaflowpy/blob/master/seaflowpy_example.ipynb](https://github.com/armbrustlab/seaflowpy/blob/master/seaflowpy_example.ipynb) for an iPython notebook walkthrough on using the seaflowpy package to

* read EVT/OPP files
* attach population annotations from VCT files
* access the underlying pandas DataFrame created for each file
* classify particles by population

## Filtering with seaflowpy_filter
`seaflowpy_filter` will filter a set of EVT files and save aggregate statistics and filter parameters into a new `popcycle` SQLite3 database file. If `opp_dir` is specified, the filtered particle data will be saved in a directory whose structure mirrors the original EVT directory.

### Example

Filter a directory of EVT files using 2 cores, show progress every 20%.

```
$ seaflowpy_filter -p 2 -r 20 -c SCOPE_1 -e ./SCOPE_1 -d ./SCOPE_1.db -o ./SCOPE_1_opp
```

## Population classifcation with seaflowpy_classify
`seaflowpy_classify` will classify OPP (focused) particles by population. Aggregate population statistics are saved to the `vct` table of a SQLite3 database file. Population annotation files (VCT) are saved to an output directory that mirrors the input OPP file directory.

The gating polygons used to define each population should have already been defined using the R `popcycle` library and saved to the database. In some cases different gating parameters should be used for different sections of a cruise. This can be accomplished by saving multiple gating entries in the database, and then running `seaflowpy_classify` multiple times with different gating paramter IDs for different groups of files. See options `--gating_id`, `--start`, and `--end`.

### Example

Classify OPP files using gates saved with ID 9cc16cb6-1d95-486b-9197-d5cd56f5d63d. Display progress every 10%.

```
$ seaflowpy_classify -c SCOPE_1 -d SCOPE_1.db -o SCOPE_1_opp/ -v SCOPE_1_vct \
-p 1 -r 20 -g 9cc16cb6-1d95-486b-9197-d5cd56f5d63d
```

If we want to limit classification to a contiguous subset of files, say between `SCOPE_1_opp/2014_342/2014-12-08T23-25-19+00-00.opp.gz` and `SCOPE_1_opp/2014_342/2014-12-08T23-43-21+00-00.opp.gz`, we can specify a `--start` and an `--end`. Note that files in the OPP directory are sorted chronologically before `--start` and `--end` filtering is applied.

```
$ seaflowpy_classify -c SCOPE_1.1 -d SCOPE_1.db -o SCOPE_1_opp/ -v SCOPE_1_vctpy \
-p 1 -g 9cc16cb6-1d95-486b-9197-d5cd56f5d63d \
-s SCOPE_1_opp/2014_342/2014-12-08T23-25-19+00-00.opp.gz \
-e SCOPE_1_opp/2014_342/2014-12-08T23-43-21+00-00.opp.gz
```

## SFL file import with seaflowpy_importsfl

`seaflowpy_importsfl` will import SFL files into the `sfl` table of a seaflow SQLite3 database.

## Export SFL and stat population data tables

`seaflowpy_exportsflstat` will export the `sfl` and `stat` tables from a seaflow SQLite3 database as CSV files.

## Convert a legacy SDS file to SFL

`seaflowpy_sds2sfl` converts an old SDS file to the current SFL format. A SeaFlow instrument serial number must be provided to convert stream pressure to flow rate.
