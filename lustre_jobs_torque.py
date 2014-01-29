#!/usr/bin/env python

# helper to import torque accounting data into DB

# 01/23/2014 01:06:06;E;566796.intern2;user=XXXX group=XXXX jobname=STDIN queue=smulti ctime=1390432092 qtime=1390432092 etime=1390432092 start=1390432137 owner=XXXX exec_host=n081701/0+n081701/1+n081701/2+n081701/3+n081701/4+n081701/5+n081701/6+n081701/7+n081002/0+n081002/1+n081002/2+n081002/3+n081002/4+n081002/5+n081002/6+n081002/7 Resource_List.ncpus=1 Resource_List.neednodes=2:sb:ppn=8 Resource_List.nodect=2 Resource_List.nodes=2:sb:ppn=8 Resource_List.walltime=08:00:00 session=8899 end=1390435566 Exit_status=0 resources_used.cput=00:00:06 resources_used.mem=103024kb resources_used.vmem=439356kb resources_used.walltime=00:57:08

import sys
import sqlite3
import lustre_jobs_sqlite

f = open(sys.argv[1], "r")

conn = sqlite3.connect('sqlite.db')
cursor = conn.cursor()

lustre_jobs_sqlite.create_tables(cursor)

for l in f:
  sp = l[:-1].split(";")
  if sp[1] == "E":
    jobid=sp[2]
    fi = sp[3].split()
    for i in fi:
      if i.startswith("etime"):
        end=i.split('=')[1]
      if i.startswith("start"):
        start=i.split('=')[1]
      if i.startswith("user"):
        owner=i.split('=')[1]
      if i.startswith("exec_host"):
        l=[]
        for n in [x.split('/')[0] for x in  i.split('=')[1].split("+")]:
          if n not in l:
            l.append(n)
        hosts=",".join(l)
        lustre_jobs_sqlite.insert_job(cursor, jobid, start, end, owner, hosts, "")

conn.commit()
conn.close()
