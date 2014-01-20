#!/usr/bin/env python

# simple data gathering client for lustre performance data
# multithreaded
# tested with lustre 1.8 and python 2.4
# Holger Berger 2014

import xmlrpclib,time
import sys
from threading import Thread,Lock

SLEEP = 60

servers = sys.argv[1:]

out = open("logfile","w")

rpcs = {}
types = {}
nids = {}
oldnids = {}
hostnames = {}
threads = {}

iolock = Lock()

first = True

def worker(srv):
    global oldnids
    r=rpcs[srv].get_sample()
    if len(r)==0:
      return
    nids[srv] = r[0].split(";")
    if first or nids[srv] != oldnids[srv]:
      iolock.acquire()
      out.write("#;"+hostnames[srv]+";")
      out.write(";".join(nids[srv][0]))
      out.write(";".join(nids[srv][1:10])+"\n")  # FIXME
      iolock.release()
    oldnids[srv] = nids[srv]
    for ost in r[1:]:
      l = []
      for i in ost.split(";"):
        if type(i) == list:
          l.append(",".join(map(str,i)))
        else:
          l.append(i)
      iolock.acquire()
      out.write(hostnames[srv]+";"+str(int(sample))+";"+";".join(map(str,l))+"\n")
      iolock.release()


for srv in servers:
  rpcs[srv] = xmlrpclib.ServerProxy('http://'+srv+':8000')
  types[srv] = rpcs[srv].get_type()
  hostnames[srv] = rpcs[srv].get_hostname()
  print "connected to %s running a %s" % (hostnames[srv],types[srv])


while True:
  sample=time.time()

  for srv in servers:
    threads[srv] = Thread( target = worker, args = (srv,)) 
    threads[srv].start()

  for srv in servers:
    threads[srv].join()

  e=time.time()
  print e-sample,"sec for sample collection"
  time.sleep(SLEEP-(e-sample))
  first = False
