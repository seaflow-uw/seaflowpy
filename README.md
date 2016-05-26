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

## Filtering with seaflowpy_filter
`seaflowpy_filter` will filter a set of EVT files and save aggregate statistics and filter parameters into a new `popcycle` SQLite3 database file. If `opp_dir` is specified, the filtered particle data will be saved in a directory whose structure mirrors the original EVT directory.


### Usage
```
usage: seaflowpy_filter [-h] [-e DIR] [-s] [-b NAME] [-l N] -d FILE [-o DIR]
                        -c NAME [--notch1 N] [--notch2 N] [--width N]
                        [--origin N] [--offset N] [-p N] [-r N] [--version]

A program to filter EVT data (version 0.2.5)

optional arguments:
  -h, --help            show this help message and exit
  -e DIR, --evt_dir DIR
                        EVT directory path (required unless --s3) (default:
                        None)
  -s, --s3              Read EVT files from s3://S3_BUCKET/CRUISE where CRUISE
                        is provided by --cruise (required unless --evt_dir)
                        (default: False)
  -b NAME, --s3_bucket NAME
                        S3 bucket name (optional) (default:
                        armbrustlab.seaflow)
  -l N, --limit N       Limit how many files to process. Useful for testing.
                        (optional) (default: None)
  -d FILE, --db FILE    SQLite3 db file. (required) (default: None)
  -o DIR, --opp_dir DIR
                        Directory in which to save LabView binary formatted
                        files of focused particles (OPP). Will be created if
                        does not exist. (optional) (default: None)
  -c NAME, --cruise NAME
                        Cruise name (required) (default: None)
  --notch1 N            Notch 1 (optional) (default: None)
  --notch2 N            Notch 2 (optional) (default: None)
  --width N             Width (optional) (default: 0.5)
  --origin N            Origin (optional) (default: None)
  --offset N            Offset (optional) (default: 0.0)
  -p N, --process_count N
                        Number of processes to use in filtering (optional)
                        (default: 1)
  -r N, --resolution N  Progress update resolution by % (optional) (default:
                        10.0)
  --version             show program's version number and exit
```

### Example

Filter a directory of EVT files using 2 cores, show progress every 20%.

```
$ seaflowpy_filter -p 2 -r 20 -c SCOPE_1 -e ./SCOPE_1 -d ./SCOPE_1.db -o ./SCOPE_1_opp

Defined parameters:
{ 'cruise': 'SCOPE_1',
  'db': './SCOPE_1.db',
  'evt_dir': './SCOPE_1',
  'offset': 0.0,
  'opp_dir': './SCOPE_1_opp',
  'process_count': 2,
  'resolution': 20.0,
  's3': False,
  's3_bucket': 'armbrustlab.seaflow',
  'version': '0.2.5',
  'width': 0.5}

Filtering 19 EVT files. Progress every 20% (approximately)
File: 4/19 (21.05%) Particles this block: 239808 / 7767424 (0.030874) elapsed: 4.28s
File: 8/19 (42.11%) Particles this block: 73848 / 7569677 (0.009756) elapsed: 6.94s
File: 12/19 (63.16%) Particles this block: 79958 / 8676480 (0.009215) elapsed: 11.66s
File: 16/19 (84.21%) Particles this block: 76657 / 8504704 (0.009013) elapsed: 14.90s
File: 19/19 (100.00%) Particles this block: 49036 / 5427072 (0.009035) elapsed: 17.41s

Input EVT files = 19
Parsed EVT files = 19
EVT particles = 37945357 (2179345.94 p/s)
OPP particles = 519307 (29825.77 p/s)
OPP/EVT ratio = 0.013686
Filtering completed in 17.41 seconds
```

## Population classifcation with seaflowpy_classify
`seaflowpy_classify` will classify OPP (focused) particles by population. Aggregate population statistics are saved to the `vct` table of a SQLite3 database file. Population annotation files (VCT) are saved to an output directory that mirrors the input OPP file directory.

The gating polygons used to define each population should have already been defined using the R `popcycle` library and saved to the database. In some cases different gating parameters should be used for different sections of a cruise. This can be accomplished by saving multiple gating entries in the database, and then running `seaflowpy_classify` multiple times with different gating paramter IDs for different groups of files. See options `--gating_id`, `--start`, and `--end`.

### Usage

```
usage: seaflowpy_classify [-h] -o DIR [-s FILE] [-e FILE] [-l N] -g ID -c NAME
                          -d FILE [-v DIR] [-p N] [-r N] [--version]

A program to classify OPP data (version 0.2.5)

optional arguments:
  -h, --help            show this help message and exit
  -o DIR, --opp_dir DIR
                        OPP directory path. (required) (default: None)
  -s FILE, --start FILE
                        First file to classify. (optional) (default: None)
  -e FILE, --end FILE   Last file to classify. (optional) (default: None)
  -l N, --limit N       Limit how many files to process. Useful for testing.
                        (optional) (default: None)
  -g ID, --gating_id ID
                        ID for gating parameters to use (required) (default:
                        None)
  -c NAME, --cruise NAME
                        Cruise name (required) (default: None)
  -d FILE, --db FILE    SQLite3 db file. (required) (default: None)
  -v DIR, --vct_dir DIR
                        Directory in which to save VCT csv files of particle
                        population annotations. Will be created if does not
                        exist. (required) (default: None)
  -p N, --process_count N
                        Number of processes to use in filtering. (optional)
                        (default: 1)
  -r N, --resolution N  Progress update resolution by % (optional) (default:
                        10.0)
  --version             show program's version number and exit
```

### Example

Classify OPP files using gates saved with ID 9cc16cb6-1d95-486b-9197-d5cd56f5d63d. Display progress every 10%.

```
$ seaflowpy_classify -c SCOPE_1 -d SCOPE_1.db -o SCOPE_1_opp/ -v SCOPE_1_vct \
-p 1 -r 20 -g 9cc16cb6-1d95-486b-9197-d5cd56f5d63d

Defined parameters:
{ 'cruise': 'SCOPE_1',
  'db': 'SCOPE_1.db',
  'gating_id': '9cc16cb6-1d95-486b-9197-d5cd56f5d63d',
  'opp_dir': 'SCOPE_1_opp/',
  'process_count': 1,
  'resolution': 20.0,
  'vct_dir': 'SCOPE_1_vct',
  'version': '0.2.5'}

Classifying 19 OPP files. Progress every 20% (approximately)
File: 4/19 (21.05%) elapsed: 1.39s
File: 8/19 (42.11%) elapsed: 1.83s
File: 12/19 (63.16%) elapsed: 2.25s
File: 16/19 (84.21%) elapsed: 2.84s
File: 19/19 (100.00%) elapsed: 3.15s

Classified 19 OPP files in 3.15 seconds
```

If we want to limit classification to a contiguous subset of files, say between `SCOPE_1_opp/2014_342/2014-12-08T23-25-19+00-00.opp.gz` and `SCOPE_1_opp/2014_342/2014-12-08T23-43-21+00-00.opp.gz`, we can specify a `--start` and an `--end`. Note that files in the OPP directory are sorted chronologically before `--start` and `--end` filtering is applied.

```
$ seaflowpy_classify -c SCOPE_1.1 -d SCOPE_1.db -o SCOPE_1_opp/ -v SCOPE_1_vctpy \
-p 1 -g 9cc16cb6-1d95-486b-9197-d5cd56f5d63d \
-s SCOPE_1_opp/2014_342/2014-12-08T23-25-19+00-00.opp.gz \
-e SCOPE_1_opp/2014_342/2014-12-08T23-43-21+00-00.opp.gz

Defined parameters:
{ 'cruise': 'SCOPE_1.1',
  'db': 'SCOPE_1.db',
  'end': 'SCOPE_1_opp/2014_342/2014-12-08T23-43-21+00-00.opp.gz',
  'gating_id': '9cc16cb6-1d95-486b-9197-d5cd56f5d63d',
  'opp_dir': 'SCOPE_1_opp/',
  'process_count': 1,
  'resolution': 10.0,
  'start': 'SCOPE_1_opp/2014_342/2014-12-08T23-25-19+00-00.opp.gz',
  'vct_dir': 'SCOPE_1_vctpy'}

Classifying 7 OPP files. Progress every 10% (approximately)
File: 1/7 (14.29%) elapsed: 0.15s
File: 2/7 (28.57%) elapsed: 0.45s
File: 3/7 (42.86%) elapsed: 0.60s
File: 4/7 (57.14%) elapsed: 0.70s
File: 5/7 (71.43%) elapsed: 0.80s
File: 6/7 (85.71%) elapsed: 0.90s
File: 7/7 (100.00%) elapsed: 1.01s

Classified 7 OPP files in 1.01 seconds
```

## SFL file import with seaflowpy_importsfl

`seaflowpy_importsfl` will import SFL files into the `sfl` table of a seaflow SQLite3 database.

### Usage
```
usage: seaflowpy_importsfl [-h] -c CRUISE [-g] [-w] (-e EVT_DIR | -s SFL) -d
                           DB [--version]

A program to insert SFL file data into a popcycle sqlite3 database (version
0.2.5)

optional arguments:
  -h, --help            show this help message and exit
  -c CRUISE, --cruise CRUISE
                        cruise name, e.g. CMOP_3
  -g, --gga             lat/lon input is in GGA format. Convert to decimal
                        degree.
  -w, --west            Some ships don't provide E/W designations for
                        longitude. Use this flag if this is the case and all
                        longitudes should be West (negative).
  -e EVT_DIR, --evt_dir EVT_DIR
                        EVT data directory if specific SFL file not provided,
                        e.g ~/SeaFlow/datafiles/evt/
  -s SFL, --sfl SFL     SFL file if EVT directory is not provided.
  -d DB, --db DB        sqlite3 database file, e.g.
                        ~/popcycle/sqlite/popcycle.db. Will be created with
                        just an sfl table if doesn't exist.
  --version             show program's version number and exit
```

## Export SFL and stat population data tables

`seaflowpy_exportsflstat` will export the `sfl` and `stat` tables from a seaflow SQLite3 database as CSV files.

### Usage
```
usage: seaflowpy_exportsflstat [-h] [--version] db {sfl,stats}

Export sfl table or stat table/view as CSV (version 0.2.5)

positional arguments:
  db           Popcycle SQLite3 DB file
  {sfl,stats}  Name of table/view to output

optional arguments:
  -h, --help   show this help message and exit
  --version    show program's version number and exit
```

## Convert a legacy SDS file to SFL

`seaflowpy_sds2sfl` converts an old SDS file to the current SFL format. A SeaFlow instrument serial number must be provided to convert stream pressure to flow rate.

### Usage
```
seaflowpy_sds2sfl -h
usage: seaflowpy_sds2sfl [-h] --sds SDS --sfl SFL --serial SERIAL [--version]

Convert old Seaflow SDS file format to SFL, with STREAM PRESSURE converted to
FLOW RATE with user supplied ratio (version 0.2.5)

optional arguments:
  -h, --help       show this help message and exit
  --sds SDS        Input SDS file
  --sfl SFL        Output SFL file.
  --serial SERIAL  Seaflow instrument serial number
  --version        show program's version number and exit
```
