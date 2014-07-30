#!/usr/bin/env python

# helper to import torque accounting data into DB

# 01/23/2014 01:06:06;E;566796.intern2;user=XXXX group=XXXX jobname=STDIN
# queue=smulti ctime=1390432092 qtime=1390432092 etime=1390432092
# start=1390432137 owner=XXXX
# exec_host=n081701/0+n081701/1+n081701/2+n081701/3+n081701/4+n081701/5+n081701/6+n081701/7+n081002/0+n081002/1+n081002/2+n081002/3+n081002/4+n081002/5+n081002/6+n081002/7
# Resource_List.ncpus=1 Resource_List.neednodes=2:sb:ppn=8
# Resource_List.nodect=2 Resource_List.nodes=2:sb:ppn=8
# Resource_List.walltime=08:00:00 session=8899 end=1390435566
# Exit_status=0 resources_used.cput=00:00:06 resources_used.mem=103024kb
# resources_used.vmem=439356kb resources_used.walltime=00:57:08

import sys
import atexit
import curses
import os.path
import time

# sys.path.append("SQLite")
#import lustre_jobs_sqlite as lustre_jobs_db
# sys.path.append("PSQL")
#import lustre_jobs_PSQL as lustre_jobs_db
sys.path.append("MySQL")
import lustre_jobs_MySQL as lustre_jobs_db


def cleanup():
    print curses.tigetstr("cnorm")

curses.setupterm()
print curses.tigetstr("civis")
atexit.register(cleanup)

if len(sys.argv) <= 1 or sys.argv[1] in ["-h", "--help"]:
    print "usage: %s torqueacctfile ..." % sys.argv[0]
    sys.exit(0)

for filename in sys.argv[1:]:
    f = open(filename, "r")
    filesize = os.path.getsize(filename)
    counter = 0

    db = lustre_jobs_db.DB('sqlite_new.db')

    db.create_tables()

    for l in f:
        sp = l[:-1].split(";")
        if sp[1] == "E":
            datestr = sp[0]
            end = int(time.mktime(time.strptime(datestr, "%m/%d/%Y %H:%M:%S")))
            jobid = sp[2]
            fi = sp[3].split()
            for i in fi:
                if i.startswith("start"):
                    start = i.split('=')[1]
                if i.startswith("user"):
                    owner = i.split('=')[1]
                if i.startswith("exec_host"):
                    l = []
                    # exec_host=n030602/0+n030602/1+n030602/2+n030602/3+n030602/4+n030602/5+n030602/6+n030602/7+n030502/0+n030502/1+n030502/2+n030502/3+n030502/4+n030502/5+n030502/6+n030502/7+n030501/0+n030501/1+n030501/2+n030501/3+n030501/4+n030501/5+n030501/6+n030501/7+n023202/0+n023202/1+n023202/2+n023202/3+n023202/4+n023202/5+n023202/6+n023202/7+n023201/0+n023201/1+n023201/2+n023201/3+n023201/4+n023201/5+n023201/6+n023201/7
                    for n in [x.split('/')[0] for x in i.split('=')[1].split("+")]:
                        if n not in l:
                            l.append(n)
                    hosts = ",".join(l)
                    db.insert_job(jobid, start, end, owner, hosts, "")
                    counter += 1
                    if counter % 10 == 0:
                        print "read %d records / %d%% from %s\r" % (counter, int(float(f.tell()) / float(filesize) * 100.0), filename),

    print "read %d records / %d%% from %s" % (counter, int(float(f.tell()) / float(filesize) * 100.0), filename)
    f.close()

    db.close()
