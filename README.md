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

### Examples

Filter a directory of 19 EVT files using 2 cores, show progress every 5%.

```
$ filterevt.py --cpus 2 --progress 5 --cruise SCOPE_1 --evt_dir ./SCOPE_1 --db ./SCOPE_1.db

Defined parameters:
{ 'cpus': 2,
  'cruise': 'SCOPE_1',
  'db': './SCOPE_1.db',
  'evt_dir': 'SCOPE_1',
  'gz': False,
  'no_index': False,
  'no_opp': False,
  'offset': 0.0,
  'progress': 5.0,
  's3': False,
  'width': 0.5}

Filtering 19 EVT files. Progress every 5% (approximately)
File: 1/19 (5.26%) Particles this block: 86240 / 2132352 (0.040444) elapsed: 4.54s
File: 2/19 (10.53%) Particles this block: 86246 / 2132096 (0.040451) elapsed: 8.25s
File: 3/19 (15.79%) Particles this block: 45672 / 1310464 (0.034852) elapsed: 10.24s
File: 4/19 (21.05%) Particles this block: 21650 / 2192512 (0.009875) elapsed: 11.32s
File: 5/19 (26.32%) Particles this block: 22739 / 2205312 (0.010311) elapsed: 12.39s
File: 6/19 (31.58%) Particles this block: 10532 / 1030797 (0.010217) elapsed: 13.02s
File: 7/19 (36.84%) Particles this block: 21153 / 2180608 (0.009701) elapsed: 14.18s
File: 8/19 (42.11%) Particles this block: 19424 / 2152960 (0.009022) elapsed: 15.05s
File: 9/19 (47.37%) Particles this block: 18956 / 2187904 (0.008664) elapsed: 15.99s
File: 10/19 (52.63%) Particles this block: 19873 / 2171008 (0.009154) elapsed: 17.00s
File: 11/19 (57.89%) Particles this block: 20497 / 2151424 (0.009527) elapsed: 18.88s
File: 12/19 (63.16%) Particles this block: 20632 / 2166144 (0.009525) elapsed: 18.96s
File: 13/19 (68.42%) Particles this block: 19230 / 2122112 (0.009062) elapsed: 20.73s
File: 14/19 (73.68%) Particles this block: 19455 / 2132736 (0.009122) elapsed: 21.72s
File: 15/19 (78.95%) Particles this block: 18934 / 2125952 (0.008906) elapsed: 22.92s
File: 16/19 (84.21%) Particles this block: 19038 / 2123904 (0.008964) elapsed: 23.88s
File: 17/19 (89.47%) Particles this block: 18891 / 2124672 (0.008891) elapsed: 24.77s
File: 18/19 (94.74%) Particles this block: 19616 / 2137344 (0.009178) elapsed: 25.62s
File: 19/19 (100.00%) Particles this block: 10529 / 1165056 (0.009037) elapsed: 26.09s

Input EVT files = 19
Parsed EVT files = 19
EVT particles = 37945357 (1454290.59 p/s)
OPP particles = 519307 (19902.92 p/s)
OPP/EVT ratio = 0.013686
Filtering completed in 26.09 seconds

Creating DB indexes
Index creation completed in 8.09 seconds
```

Filter a directory of 19 EVT files using 2 cores, show progress every 5%, but don't save OPP data. This is a fast way to see the effects of filtering if actual focused particle data is not needed. Notice it only took 6.5 seconds versus 34 seconds if OPP data was saved.

```
$ filterevt.py --cpus 2 --progress 5 --cruise SCOPE_1 --no_opp --evt_dir ./SCOPE_1 --db ./SCOPE_1.db

Defined parameters:
{ 'cpus': 2,
  'cruise': 'SCOPE_1',
  'db': './SCOPE_1.db',
  'evt_dir': 'SCOPE_1',
  'gz': False,
  'no_index': False,
  'no_opp': True,
  'offset': 0.0,
  'progress': 5.0,
  's3': False,
  'width': 0.5}

Filtering 19 EVT files. Progress every 5% (approximately)
File: 1/19 (5.26%) Particles this block: 86240 / 2132352 (0.040444) elapsed: 0.72s
File: 2/19 (10.53%) Particles this block: 86246 / 2132096 (0.040451) elapsed: 0.78s
File: 3/19 (15.79%) Particles this block: 45672 / 1310464 (0.034852) elapsed: 1.15s
File: 4/19 (21.05%) Particles this block: 21650 / 2192512 (0.009875) elapsed: 1.46s
File: 5/19 (26.32%) Particles this block: 22739 / 2205312 (0.010311) elapsed: 1.83s
File: 6/19 (31.58%) Particles this block: 10532 / 1030797 (0.010217) elapsed: 1.84s
File: 7/19 (36.84%) Particles this block: 19424 / 2152960 (0.009022) elapsed: 2.62s
File: 8/19 (42.11%) Particles this block: 21153 / 2180608 (0.009701) elapsed: 2.64s
File: 9/19 (47.37%) Particles this block: 18956 / 2187904 (0.008664) elapsed: 3.39s
File: 10/19 (52.63%) Particles this block: 19873 / 2171008 (0.009154) elapsed: 3.46s
File: 11/19 (57.89%) Particles this block: 20632 / 2166144 (0.009525) elapsed: 4.08s
File: 12/19 (63.16%) Particles this block: 20497 / 2151424 (0.009527) elapsed: 4.22s
File: 13/19 (68.42%) Particles this block: 19455 / 2132736 (0.009122) elapsed: 4.79s
File: 14/19 (73.68%) Particles this block: 19230 / 2122112 (0.009062) elapsed: 4.92s
File: 15/19 (78.95%) Particles this block: 18934 / 2125952 (0.008906) elapsed: 5.48s
File: 16/19 (84.21%) Particles this block: 19038 / 2123904 (0.008964) elapsed: 5.63s
File: 17/19 (89.47%) Particles this block: 18891 / 2124672 (0.008891) elapsed: 6.15s
File: 18/19 (94.74%) Particles this block: 19616 / 2137344 (0.009178) elapsed: 6.29s
File: 19/19 (100.00%) Particles this block: 10529 / 1165056 (0.009037) elapsed: 6.46s

Input EVT files = 19
Parsed EVT files = 19
EVT particles = 37945357 (5874116.89 p/s)
OPP particles = 519307 (80391.13 p/s)
OPP/EVT ratio = 0.013686
Filtering completed in 6.46 seconds

Creating DB indexes
Index creation completed in 0.01 seconds
```

Filter the first 10 files in an EVT directory

```
$ filterevt.py --cruise SCOPE_1 --evt_dir ./SCOPE_1 --db ./SCOPE_1.db --limit 10

Defined parameters:
{ 'cpus': 1,
  'cruise': 'SCOPE_1',
  'db': 'SCOPE_1.db',
  'evt_dir': 'SCOPE_1',
  'gz': False,
  'limit': 10,
  'no_index': False,
  'no_opp': False,
  'offset': 0.0,
  'progress': 10.0,
  's3': False,
  'width': 0.5}

Filtering 10 EVT files. Progress every 10% (approximately)
File: 1/10 (10.00%) Particles this block: 86240 / 2132352 (0.040444) elapsed: 3.94s
File: 2/10 (20.00%) Particles this block: 86246 / 2132096 (0.040451) elapsed: 7.89s
File: 3/10 (30.00%) Particles this block: 45672 / 1310464 (0.034852) elapsed: 10.05s
File: 4/10 (40.00%) Particles this block: 21650 / 2192512 (0.009875) elapsed: 11.50s
File: 5/10 (50.00%) Particles this block: 22739 / 2205312 (0.010311) elapsed: 12.92s
File: 6/10 (60.00%) Particles this block: 10532 / 1030797 (0.010217) elapsed: 13.60s
File: 7/10 (70.00%) Particles this block: 21153 / 2180608 (0.009701) elapsed: 15.42s
File: 8/10 (80.00%) Particles this block: 19424 / 2152960 (0.009022) elapsed: 16.93s
File: 9/10 (90.00%) Particles this block: 18956 / 2187904 (0.008664) elapsed: 18.44s
File: 10/10 (100.00%) Particles this block: 19873 / 2171008 (0.009154) elapsed: 20.28s

Input EVT files = 10
Parsed EVT files = 10
EVT particles = 19696013 (971005.39 p/s)
OPP particles = 352485 (17377.37 p/s)
OPP/EVT ratio = 0.017896
Filtering completed in 20.28 seconds

Creating DB indexes
Index creation completed in 5.96 seconds
```

Filter 2 EVT files.

```
$ filterevt.py --cruise SCOPE_1 --files ./SCOPE_1/2014_342/2014-12-08T22-53-34+00-00 \
./SCOPE_1/2014_342/2014-12-08T23-07-01+00-00 --db ./SCOPE_1.db

Defined parameters:
{ 'cpus': 1,
  'cruise': 'SCOPE_1',
  'db': 'SCOPE_1.db',
  'files': [ './SCOPE_1/2014_342/2014-12-08T22-53-34+00-00',
             './SCOPE_1/2014_342/2014-12-08T23-07-01+00-00'],
  'gz': False,
  'no_index': False,
  'no_opp': False,
  'offset': 0.0,
  'progress': 10.0,
  's3': False,
  'width': 0.5}

Filtering 2 EVT files. Progress every 10% (approximately)
File: 1/2 (50.00%) Particles this block: 86240 / 2132352 (0.040444) elapsed: 3.91s
File: 2/2 (100.00%) Particles this block: 22739 / 2205312 (0.010311) elapsed: 5.40s

Input EVT files = 2
Parsed EVT files = 2
EVT particles = 4337664 (803137.13 p/s)
OPP particles = 108979 (20177.93 p/s)
OPP/EVT ratio = 0.025124
Filtering completed in 5.40 seconds

Creating DB indexes
Index creation completed in 1.16 seconds
```

Filter a list of EVT files fed to STDIN by `find`.

```
$ find SCOPE_1 -name '*T23-2*00-00' | filterevt.py --cruise SCOPE_1 --files - --db ./SCOPE_1.db

Defined parameters:
{ 'cpus': 1,
  'cruise': 'SCOPE_1',
  'db': 'SCOPE_1.db',
  'files': ['-'],
  'gz': False,
  'no_index': False,
  'no_opp': False,
  'offset': 0.0,
  'progress': 10.0,
  's3': False,
  'width': 0.5}

Filtering 3 EVT files. Progress every 10% (approximately)
File: 1/3 (33.33%) Particles this block: 21153 / 2180608 (0.009701) elapsed: 1.33s
File: 2/3 (66.67%) Particles this block: 19424 / 2152960 (0.009022) elapsed: 2.63s
File: 3/3 (100.00%) Particles this block: 18956 / 2187904 (0.008664) elapsed: 3.89s

Input EVT files = 3
Parsed EVT files = 3
EVT particles = 6521472 (1675265.06 p/s)
OPP particles = 59533 (15293.10 p/s)
OPP/EVT ratio = 0.009129
Filtering completed in 3.89 seconds

Creating DB indexes
Index creation completed in 0.58 seconds
```
