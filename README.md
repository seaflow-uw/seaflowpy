# seaflowpy

## filterevt.py

### Installation
[Install Anaconda](https://www.continuum.io/downloads) for Python 2.7.

### Usage
```
usage: filterevt.py [-h]
                    (--files FILES [FILES ...] | --evt_dir EVT_DIR | --s3)
                    [--cpus CPUS] --db DB --cruise CRUISE [--notch1 NOTCH1]
                    [--notch2 NOTCH2] [--width WIDTH] [--origin ORIGIN]
                    [--offset OFFSET] [--no_index] [--no_opp] [--gz]
                    [--progress PROGRESS] [--limit LIMIT]

Filter EVT data.

optional arguments:
  -h, --help            show this help message and exit
  --files FILES [FILES ...]
                        EVT file paths. - to read from stdin. (required unless
                        --evt_dir or --s3) (default: None)
  --evt_dir EVT_DIR     EVT directory path (required unless --files or --s3)
                        (default: None)
  --s3                  Read EVT files from s3://seaflowdata/CRUISE where
                        cruise is provided by --cruise (required unless
                        --files or --evt_dir) (default: False)
  --cpus CPUS           Number of CPU cores to use in filtering (optional)
                        (default: 1)
  --db DB               SQLite3 db file. If this file is to be compressed
                        (i.e. --no_gz is not set), an extension of ".gz" will
                        automatically be added to the path given here.
                        (required) (default: None)
  --cruise CRUISE       Cruise name (required) (default: None)
  --notch1 NOTCH1       Notch 1 (optional) (default: None)
  --notch2 NOTCH2       Notch 2 (optional) (default: None)
  --width WIDTH         Width (optional) (default: 0.5)
  --origin ORIGIN       Origin (optional) (default: None)
  --offset OFFSET       Offset (optional) (default: 0.0)
  --no_index            Don't create SQLite3 indexes (optional) (default:
                        False)
  --no_opp              Don't save data to opp table (optional) (default:
                        False)
  --gz                  gzip compress SQLite3 db file (optional) (default:
                        False)
  --progress PROGRESS   Progress update % resolution (optional) (default:
                        10.0)
  --limit LIMIT         Limit how many files to process. Useful for testing.
                        (optional) (default: None)
```
