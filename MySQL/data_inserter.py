#!/usr/bin/env python


# inserts data into a sqlite database
# Holger Berger 2014
#
# TODO
#  - insert OST and MDS values as well
#  - add job data/user mapping
#
# fist argument is hosts file for name mapping, following arguments is files
#
# addition form Uwe Schilling 2014:
# add the posibility to insert direct to the db without any file.
#
#
# supported db at this moment, sqlite, mysql, psql
#

import sys
import atexit
import os.path
import time
from MySQLObject import MySQLObject
import curses
import hashlib


class Logfile:

    def __init__(self, filename=None, hostfile=None):

        self.filename = filename
        self.myDB = MySQLObject()
        if filename:
            self.filesize = os.path.getsize(filename)

        if hostfile:
            self.readhostfile(hostfile)

    def eta(self, secs):
        if secs < 60:
            return "%2.2d sec       " % secs
        else:
            return "%d min %2.2d sec" % (secs / 60, secs % 60)

    def readHead(self, line):
        sp = line[:-1].split(";")  # ignore the line break at the end
        server = sp[1]
        # timestamp = sp[2]
        stype = sp[3]  # mdt or ost

        # add server and type to the db
        self.myDB.insert_server(server, stype)

        # add nids to the database
        for nid in sp[5:]:
            self.insert_nid_server(server, nid)
#------------------------------------------------------------------------------

    def readData(self, line):
        sp = line[:-1].split(";")  # ignore the line break at the end
    # form a hash digest of the line and check if line is already in database,
    # if so, do not add line again to avoid bloat and wrong results for
    # sums over data
        hexdigest = hashlib.sha224(line).hexdigest()
        if self.myDB.has_hash(hexdigest):
            # print "line collision"
            return

        server = sp[0]
        timestamp = sp[1]
        source = sp[2]
        value_tupel = sp[3]  # values for ost

        self.myDB.insert_timestamp(timestamp)
        # source = univ_1-OST0001
        fs = source.split('-')[0]
        self.myDB.insert_source(source, fs)

        # add ost global
        self.insert_ost_global(source, value_tupel, timestamp, server)
        self.insert_nids(server, timestamp, source, sp[4:])
#------------------------------------------------------------------------------

    def read(self):
        ''' action is HERE'''
        f = open(self.filename, "r")
        counter = 0
        acounter = 0
        starttime = time.time()

        # 1.0;hmds1;time;mdt;reqs;
        # 1.0;hoss3;time;ost;rio,rb,wio,wb;
        for line in f:
            if line.startswith("#"):  # this is a head line
                self.readHead(line)
            else:
#--------------------- if not headline ----------------------------------------
                self.readData(line)

#--------------------- progress bar -------------------------------------------
            if self.filename:
                counter += 1
                if counter % 10 == 0:
                    duration = (time.time() - starttime)
                    fraction = (float(f.tell()) / float(self.filesize))
                    endtime = duration * (1.0 / fraction) - duration
                # printString = str("\rinserted %d records / %d%% ETA = %s"
                #                 %(counter,int(fraction*100.0), self.eta(endtime)))
                    printString = str("\rinserted %9d records [%s] ETA = %s"
                                      % (counter, "|" * int(fraction * 20.0) +
                                         "\\|/-"[acounter % 4] +
                                         "-" * (19 - int(fraction * 20.0)), self.eta(endtime)))
                    print printString,
                    sys.stdout.flush()
                    acounter += 1
#------------------------------------------------------------------------------
        endtime = time.time()
        print "used %s to insert data." % self.eta(endtime - starttime)

#------------------------------------------------------------------------------

    def readhostfile(self, hostfile):
        try:
            f = open(hostfile, "r")
        except:
            return
        for l in f:
            if not l.startswith('#'):
                sp = l[:-1].split()
                if len(sp) == 0:
                    continue
                ip = sp[0]
                name = sp[1]
                self.myDB.hostfilemap[ip] = name
        print "read", len(self.myDB.hostfilemap), "host mappings"
        f.close()
#------------------------------------------------------------------------------

    def insert_ost_global(self, source, d_tuple, timestamp, server):
        self.myDB.insert_ost_global(source, d_tuple, timestamp, server)

    def insert_nid_server(self, server, one_nid):
        self.myDB.add_nid_server(server, one_nid)

    def insert_nid(self, server, timeStamp, source, nidvals_Tup, nidID):
        ''' methode to insert only one nid value tuple '''
        self.myDB.add_nid_values(server, timeStamp, source, nidvals_Tup, nidID)

    def insert_nids(self, server, timestamp, source, nidvals):
        stype = self.myDB.servertype[server]
        il_ost = []
        il_mdt = []

        for i in range(len(nidvals)):
            nidid = self.myDB.globalnidmap[
                self.myDB.per_server_nids[server][i]]
            timeid = self.myDB.timestamps[timestamp]
            sourceid = self.myDB.sources[source]

            if nidvals[i] != "":
                if stype == 'ost':
                    temp = [timeid, sourceid, nidid]
                    temp.extend(nidvals[i].split(','))
                    il_ost.append(temp)

                if stype == 'mdt':
                    il_mdt.append((timeid, sourceid, nidid, nidvals[i]))

        self.myDB.insert_ost_samples(il_ost)
        self.myDB.insert_mdt_samples(il_mdt)


def cleanup():
    print curses.tigetstr("cnorm")

if __name__ == "__main__":

    try:
        curses.setupterm()
        print curses.tigetstr("civis"),
        atexit.register(cleanup)
    except:
        pass

    if len(sys.argv) <= 2 or sys.argv[1] in ["-h", "--help"]:
        print "usage: %s hostmapping logfile ..." % sys.argv[0]
        sys.exit(0)

    hostfile = sys.argv[1]

    for filename in sys.argv[2:]:
        o = Logfile(filename, hostfile)
        o.read()
        print "optimizing DB...",
        sys.stdout.flush()
        print " done"
        o.myDB.closeConnection()
