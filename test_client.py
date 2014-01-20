#!/usr/bin/env python

import xmlrpclib,time
import sys

SLEEP = 5

servers = sys.argv[1:]

out = open("logfile","w")

rpcs = {}
types = {}
nids = {}
oldnids = {}
hostnames = {}

for srv in servers:
  rpcs[srv] = xmlrpclib.ServerProxy('http://'+srv+':8000')
  types[srv] = rpcs[srv].get_type()
  hostnames[srv] = rpcs[srv].get_hostname()
  print "connected to %s running a %s" % (hostnames[srv],types[srv])

first = True

while True:
  sample=time.time()
  for srv in servers:
    r=rpcs[srv].get_sample()
    if len(r)==0:
      continue
    nids[srv] = r[0]
    if first or nids[srv] != oldnids[srv]:
      out.write("#;"+hostnames[srv]+";")
      out.write(";".join(nids[srv][0]))
      out.write(";".join(nids[srv][1:10])+"\n")  # FIXME
    oldnids[srv] = nids[srv]
    for ost in r[1:]:
      l = []
      for i in ost:
        if type(i) == list:
          l.append(",".join(map(str,i)))
        else:
          l.append(i)
      out.write(hostnames[srv]+";"+str(int(sample))+";"+";".join(map(str,l))+"\n")
  e=time.time()
  print "sample duraction",e-sample
  time.sleep(SLEEP-(e-sample))
  first = False
