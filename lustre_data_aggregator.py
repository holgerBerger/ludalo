#!/usr/bin/env python

# simple data gathering client for lustre performance data
# multithreaded
# tested with lustre 1.8 and python 2.4
# Holger Berger 2014

import sys
sys.path.append("/MySQL")

import xmlrpclib, time, socket
import sys, signal, os
from threading import Thread, Lock
from data_inserter import Logfile

SLEEP = 60   # > 10 sec
TIMEOUT = 30  # has to be < SLEEP
FILEVERSION = "1.0"

servers = sys.argv[1:]

out = open("data_file", "w")
db = Logfile()

rpcs = {}
types = {}
nids = {}
oldnids = {}
hostnames = {}
threads = {}

iolock = Lock()

first = True
timings = {}
bws = {}
reqs = {}


def signalhandler(signum, frame):
    global out, first, iolock
    if signum == signal.SIGUSR1:
        iolock.acquire()
        out.close()
        print "switching file to", "data_file_" + str(int(time.time()))
        os.rename("data_file", "data_file_" + str(int(time.time())))
        out = open("data_file", "w")
        iolock.release()
        first = True


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
        #out.write("#" + FILEVERSION + ";" + hostnames[srv] + ";")
        #out.write(nids[srv][0] + ";")
        #out.write(";".join(nids[srv][1:]) + "\n")
        line = ("#" + FILEVERSION + ";" + hostnames[srv] + ";" + nids[srv][0] + ";" + ";".join(nids[srv][1:]) + "\n")
        db.readHead(line)
        iolock.release()
    oldnids[srv] = nids[srv]
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
        #out.write(hostnames[srv] + ";" + str(int(sample)) + ";" + ";" .join(map(str, l))+"\n")
        line = (hostnames[srv] + ";" + str(int(sample)) + ";" + ";" .join(map(str, l))+"\n")
        db.readData(line)
        iolock.release()
        vs = sp[1].split(',')
        if len(vs) == 1:
            reqs[srv] = reqs.setdefault(srv, 0) + int(sp[1])
        else:
            (wb, rb) = bws.setdefault(srv, (0, 0))
            bws[srv] = (wb + int(vs[1]), rb + int(vs[3]))

#------------------------------------------------------------------------------

socket.setdefaulttimeout(TIMEOUT)

for srv in servers:
    rpcs[srv] = xmlrpclib.ServerProxy('http://' + srv + ':8000')
    types[srv] = rpcs[srv].get_type()
    hostnames[srv] = rpcs[srv].get_hostname()
    print "connected to %s running a %s" % (hostnames[srv], types[srv])

signal.signal(signal.SIGUSR1, signalhandler)

while True:
    sample = time.time()

    for srv in servers:
        threads[srv] = Thread(target=worker, args=(srv,))
        threads[srv].start()

    for srv in servers:
        threads[srv].join()

    e = time.time()
    # set this here, to have a good chance to get it right during sleep
    first = False
    print "%3.3fs for sample collection," % (e - sample),
    minT = ("", 10000)
    maxT = ("", 0)
    avg = 0
    for (s, v) in timings.iteritems():
        if v < minT[1]:
            minT = (s, v)
        if v > maxT[1]:
            maxT = (s, v)
        avg += float(v)
    print "transfer times - max: %s %3.3fs" % maxT, "- min: %s %3.3fs" % minT, "- avg: %3.3fs" % (avg/len(timings))
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

    time.sleep(SLEEP - ((e - sample) % SLEEP))
