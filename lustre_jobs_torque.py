#!/usr/bin/env python

# 01/23/2014 00:53:29;S;566798.intern2;user=ifftgs group=iff12266 jobname=lack-200_fd300_f5_u10_t.cas.gz.sh queue=single ctime=1390434772 qtime=1390434772 etime=1390434772 start=1390434809 owner=ifftgs@cl3fr1 exec_host=n133501/0 Resource_List.ncpus=1 Resource_List.neednodes=1:mem256gb Resource_List.nodect=1 Resource_List.nodes=1:mem256gb Resource_List.walltime=12:00:00 01/23/2014 01:06:06;E;566796.intern2;user=hpcchris group=s17063 jobname=STDIN queue=smulti ctime=1390432092 qtime=1390432092 etime=1390432092 start=1390432137 owner=hpcchris@cl3fr2 exec_host=n081701/0+n081701/1+n081701/2+n081701/3+n081701/4+n081701/5+n081701/6+n081701/7+n081002/0+n081002/1+n081002/2+n081002/3+n081002/4+n081002/5+n081002/6+n081002/7 Resource_List.ncpus=1 Resource_List.neednodes=2:sb:ppn=8 Resource_List.nodect=2 Resource_List.nodes=2:sb:ppn=8 Resource_List.walltime=08:00:00 session=8899 end=1390435566 Exit_status=0 resources_used.cput=00:00:06 resources_used.mem=103024kb resources_used.vmem=439356kb resources_used.walltime=00:57:08

import sys

f = open(sys.argv[1], "r")

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
        hosts=",".join(list(set([x.split('/')[0] for x in  i.split('=')[1].split("+")])))
        print  jobid, start, end, owner, hosts