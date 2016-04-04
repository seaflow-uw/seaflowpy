# seaflowpy
A Python library for working with SeaFlow data.

## Installation

### Prerequisites
[Install Anaconda](https://www.continuum.io/downloads) for Python 2.7, or make sure you have a build environment setup to install numpy.

### Install

```sh
git clone https://github.com/armbrustlab/seaflowpy
cd seaflowpy
python setup.py install
```

You can now import a python package called `seaflowpy` and a new executable `filterevt` should be available in your path.

### Test
You can test that `seaflowpy` is working properly on your system with

```sh
python setup.py test
```

## seaflowpy package

See [https://github.com/armbrustlab/seaflowpy/blob/master/seaflowpy_example.ipynb](https://github.com/armbrustlab/seaflowpy/blob/master/seaflowpy_example.ipynb) for an iPython notebook walkthrough of using the seaflowpy package to read EVT files.

## Command-line filtering with filterevt
filterevt will filter a set of EVT files and save aggregate statistics and filter parameters into a new `popcycle` SQLite3 database file. If `opp_dir` is specified the filtered particle data will be saved in a directory whose structure mirrors the original EVT directory.



### Usage
```
usage: filterevt [-h] (--files FILES [FILES ...] | --evt_dir EVT_DIR | --s3)
                 --db DB [--opp_dir OPP_DIR] --cruise CRUISE [--notch1 NOTCH1]
                 [--notch2 NOTCH2] [--width WIDTH] [--origin ORIGIN]
                 [--offset OFFSET] [--cpus CPUS] [--progress PROGRESS]
                 [--limit LIMIT] [--s3_bucket S3_BUCKET]

Filter EVT data.

optional arguments:
  -h, --help            show this help message and exit
  --files FILES [FILES ...]
                        EVT file paths. - to read from stdin. (required unless
                        --evt_dir or --s3) (default: None)
  --evt_dir EVT_DIR     EVT directory path (required unless --files or --s3)
                        (default: None)
  --s3                  Read EVT files from s3://S3_BUCKET/CRUISE where
                        cruise is provided by --cruise (required unless
                        --files or --evt_dir) (default: False)
  --db DB               SQLite3 db file. (required) (default: None)
  --opp_dir OPP_DIR     Directory in which to save LabView binary formatted
                        files of focused particles (OPP). Will be created if
                        does not exist. (optional) (default: None)
  --cruise CRUISE       Cruise name (required) (default: None)
  --notch1 NOTCH1       Notch 1 (optional) (default: None)
  --notch2 NOTCH2       Notch 2 (optional) (default: None)
  --width WIDTH         Width (optional) (default: 0.5)
  --origin ORIGIN       Origin (optional) (default: None)
  --offset OFFSET       Offset (optional) (default: 0.0)
  --cpus CPUS           Number of CPU cores to use in filtering (optional)
                        (default: 1)
  --progress PROGRESS   Progress update % resolution (optional) (default:
                        10.0)
  --limit LIMIT         Limit how many files to process. Useful for testing.
                        (optional) (default: None)
  --s3_bucket S3_BUCKET
                        S3 bucket name (optional) (default:
                        armbrustlab.seaflow)
```

### Examples

Filter a directory of EVT files using 2 cores, show progress every 5%.

```
$ filterevt --cpus 2 --progress 5 --cruise SCOPE_1 --evt_dir ./SCOPE_1 \
--db ./SCOPE_1.db --opp_dir ./SCOPE_1_opp

Defined parameters:
{ 'cpus': 2,
  'cruise': 'SCOPE_1',
  'db': './SCOPE_1.db',
  'evt_dir': './SCOPE_1',
  'offset': 0.0,
  'opp_dir': './SCOPE_1_opp',
  'progress': 5.0,
  's3': False,
  's3_bucket': 'armbrustlab.seaflow',
  'width': 0.5}

Filtering 19 EVT files. Progress every 5% (approximately)
File: 1/19 (5.26%) Particles this block: 86246 / 2132096 (0.040451) elapsed: 1.66s
File: 2/19 (10.53%) Particles this block: 86240 / 2132352 (0.040444) elapsed: 1.67s
File: 3/19 (15.79%) Particles this block: 45672 / 1310464 (0.034852) elapsed: 2.62s
...

Input EVT files = 19
Parsed EVT files = 19
EVT particles = 37945357 (2840899.89 p/s)
OPP particles = 519307 (38879.57 p/s)
OPP/EVT ratio = 0.013686
Filtering completed in 13.36 seconds
```
