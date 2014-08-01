#!/usr/bin/env python

# simple data gathering client for lustre performance data
# multithreaded
# tested with lustre 1.8 and python 2.4
# Holger Berger 2014

# Refactored by Uwe Schilling 1.aug 2014

# ---------- global imports --------------
import sys
import xmlrpclib
import time
import socket

# ---------- parital imports --------------
from threading import Thread, Lock

# ---------- project imports --------------
sys.path.append("MySQL")
from data_inserter import Logfile

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------

# Objects
db = Logfile()
iolock = Lock()

# Constants
SLEEP = 60   # > 10 sec
TIMEOUT = 30  # has to be < SLEEP
FILEVERSION = "1.0"

# Variables
first = True

# Data
rpcs = {}
types = {}
nids = {}
oldnids = {}
hostnames = {}
threads = {}
timings = {}
bws = {}
reqs = {}
# -----------------------------------------------------------------------------


def worker(srv):
    global oldnids, first, timings, bws, reqs

    t1 = time.time()

    try:
        r = rpcs[srv].get_sample()
    except:
        print >>sys.stderr, "failed to connect to server:", srv
        timings[srv] = time.time() - t1
        return
    timings[srv] = time.time() - t1

    if len(r) == 0:
        return
    nids[srv] = r[0].split(";")
    if first or nids[srv] != oldnids[srv]:
        iolock.acquire()
# --------- switch to db here ---------
        t_insert = time.time()
        line = str("#" +
                   FILEVERSION + ";" +
                   hostnames[srv] + ";" +
                   nids[srv][0] + ";" +
                   ";".join(nids[srv][1:]) + "\n")
        db.readHead(line)
        print "  time to insert header [sec]:", (time.time() - t_insert)
        iolock.release()
    oldnids[srv] = nids[srv]
    t_insert = time.time()
    for ost in r[1:]:
        l = []
        sp = ost.split(";")
        for i in sp:
            if type(i) == list:
                l.append(",".join(map(str, i)))
            else:
                l.append(i)
        iolock.acquire()
# --------- switch to db here ---------
        line = str(hostnames[srv] + ";" +
                   str(int(sample_start_time)) + ";" +
                   ";" .join(map(str, l)) + "\n")
        db.readData(line)
        iolock.release()
        vs = sp[1].split(',')
        if len(vs) == 1:
            reqs[srv] = reqs.setdefault(srv, 0) + int(sp[1])
        else:
            (wb, rb) = bws.setdefault(srv, (0, 0))
            bws[srv] = (wb + int(vs[1]), rb + int(vs[3]))
    print "  time to insert data   [sec]:", (time.time() - t_insert)
#------------------------------------------------------------------------------


def connect_to(srv):
    try:
        rpcs[srv] = xmlrpclib.ServerProxy('http://' + srv + ':8000')
        types[srv] = rpcs[srv].get_type()
        hostnames[srv] = rpcs[srv].get_hostname()
        print "connected to %s running a %s" % (hostnames[srv], types[srv])
    except socket.error:
        print >>sys.stderr, "could not connect to ", srv
#------------------------------------------------------------------------------


def print_stats():
    minT = ("", 10000)
    maxT = ("", 0)
    avg = 0
    for (s, v) in timings.iteritems():
        if v < minT[1]:
            minT = (s, v)
        if v > maxT[1]:
            maxT = (s, v)
        avg += float(v)
    print "transfer times - max: %s %3.3fs" % maxT, "- min: %s %3.3fs" % minT, "- avg: %3.3fs" % (avg / len(timings))
    for mdt in reqs:
        print "  metadata requests  %s: %6.1f/s" % (mdt, reqs[mdt] / float(SLEEP))
        reqs[mdt] = 0
    trbs = 0
    twbs = 0
    for oss in bws:
        print "  oss data bandwidth %s: read %7.1f MB/s - write %7.1f MB/s" % (
            oss, bws[oss][0] / (1024.0 * 1024.0 * float(SLEEP)),
            bws[oss][1] / (1024.0 * 1024.0 * float(SLEEP)))
        trbs += bws[oss][0]
        twbs += bws[oss][1]
        bws[oss] = (0, 0)
    print "  === total bandwidth === : read %7.1f MB/s - write %7.1f MB/s" % (
        trbs / (1024.0 * 1024.0 * float(SLEEP)),
        twbs / (1024.0 * 1024.0 * float(SLEEP)))
#------------------------------------------------------------------------------

socket.setdefaulttimeout(TIMEOUT)

if __name__ == '__main__':

    # Args
    servers = sys.argv[1:]

    for srv in servers:
        connect_to(srv)

    # main loop
    while True:
        # Time mesure for sync
        sample_start_time = time.time()

        # Start workers
        for srv in servers:
            threads[srv] = Thread(target=worker, args=(srv,))
            threads[srv].start()

        # Collect data
        for srv in servers:
            threads[srv].join()

        # Time mesure
        sample_end_time = time.time()

        # Flag to handle first collection different
        first = False

        # Print Stats
        print "%3.3fs for sample collection," % (sample_end_time - sample_start_time),
        print_stats()

        # Sleep and try to ceep the samples in sync
        time.sleep(SLEEP - ((sample_end_time - sample_start_time) % SLEEP))
