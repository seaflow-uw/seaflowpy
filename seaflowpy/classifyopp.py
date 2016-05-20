import db
import evt
import errors
import os
import sys
import time
from itertools import imap
from multiprocessing import Pool


def classify_opp_files(files, cruise, gating_id, dbpath, vct_dir, process_count=1,
                       every=10.0, multiprocessing_flag=True):
    """Classify a list of OPP files.

    Arguments:
        files - paths to OPP files to filter
        cruise - cruise name
        gating_id - id for entry in gating table
        dbpath = SQLite3 db path
        vct_dir = Directory for output binary OPP files

    Keyword arguments:
        process_count - number of worker processes to use
        every - Percent progress output resolution
        multiprocessing_flag = Use multiprocessing?
    """
    # Get classification polygons
    pop_polys = db.get_poly(dbpath, gating_id)

    # Construct worker inputs
    inputs = []
    for f in files:
        inputs.append([f, cruise, gating_id, pop_polys, dbpath, vct_dir])

    if multiprocessing_flag:
        # Create a pool of N worker processes
        pool = Pool(process_count)
        def mapper(worker, task_list):
            return pool.imap_unordered(worker, task_list)
    else:
        def mapper(worker, task_list):
            return imap(worker, task_list)

    print ""
    print "Classifying %i OPP files. Progress every %i%% (approximately)" % \
        (len(files), every)

    t0 = time.time()

    last = 0  # Last progress milestone in increments of every

    # Filter particles in parallel with process pool
    for i, res in enumerate(mapper(do_work, inputs)):
        # Print progress periodically
        perc = float(i + 1) / len(files) * 100  # Percent completed
        # Round down to closest every%
        milestone = int(perc / every) * every
        if milestone > last:
            now = time.time()
            msg = "File: %i/%i (%.2f%%)" % (i + 1, len(files), perc)
            msg += " elapsed: %.2fs" % (now - t0,)
            print msg
            sys.stdout.flush()
            last = milestone

    t1 = time.time()
    delta = t1 - t0

    print ""
    print "Classified %i OPP files in %.2f seconds" % (len(files), delta)


def do_work(args):
    """multiprocessing pool worker function"""
    try:
        return classify_one_file(*args)
    except KeyboardInterrupt as e:
        pass


def classify_one_file(opp_file, cruise, gating_id, poly, dbpath, vct_dir):
    """Classify one OPP file, save to sqlite3 and csv"""
    try:
        opp = evt.EVT(path=opp_file, transform=True,
                      columns=["fsc_small", "fsc_perp", "pe", "chl_small"])
    except errors.EVTFileError as e:
        print "Could not parse file %s: %s" % (opp_file, repr(e))
    except Exception as e:
        print "Unexpected error for file %s: %s" % (opp_file, repr(e))

    else:
        opp.classify(poly)
        opp.write_vct_csv(vct_dir)
        opp.save_vct_to_db(cruise, gating_id, dbpath)
