#!/usr/bin/env python

# data collector for OSS
#
# tested with lustre 1.8.4
#
# collect nr of IO requests and size of IO every SLEEP seconds
# and write to a csv file.
# CSV contains in each sample the values for sample-last sample, to deltas
# samples are tuples of 4 values #read io request,read bytes,#write io requests, written bytes
# epoch;name of ost; sample for ost; sampel for 1. nid; sample for 2. nid; ...
# legend starts with # and can change over time, as nid list changes. The legend contains the names of the nids
# sorted as the samples are.
# nid list changes and ost number changes should be covered. Empty (no activity) nid samples are empty, and look like ;;
# empty OST samples (= no IO) are left out

# Holger Berger 2014


# get new sampel after SLEEP seconds
SLEEP = 60

# update NID list after NIDTIMEOUT seconds, can be longer than SLEEP
# in case of short SLEEP, that reduces number of directory lookups
# which is (#OSTS * #NIDS) and can be a factor in case of large #NIDS
NIDTIMEOUT = 60

import os,time,sys,getopt

def get_osts():
    return [ x for x in os.listdir("/proc/fs/lustre/obdfilter/") if 'OST' in x ]

def get_ost_data(ost):
    f=open("/proc/fs/lustre/obdfilter/"+ost+"/stats","r")
    rio=0; wio=0; rb=0; wb=0
    for l in f:
      if l.startswith("read_bytes"):
        s=l.split()
        rb=int(s[6])
        rio=int(s[1])
      if l.startswith("write_bytes"):
        s=l.split()
        wb=int(s[6])
        wio=int(s[1])
    f.close()
    return (rio,rb,wio,wb)

def get_nid_data(ost, nid):
  """get nid data, return 0 for non existing nid"""
  rio=0; wio=0; rb=0; wb=0
  try:
    f=open("/proc/fs/lustre/obdfilter/"+ost+"/exports/"+nid+"/stats","r")
    for l in f:
      if l.startswith("read_bytes"):
        s=l.split()
        rb=int(s[6])
        rio=int(s[1])
      if l.startswith("write_bytes"):
        s=l.split()
        wb=int(s[6])
        wio=int(s[1])
    f.close()
  except:
    pass
  return (rio,rb,wio,wb)
  
def dumptuple(data, old):
  """write out a tuple, write delta and nothing if only zeros"""
  l = [ str(data[i]-old[i]) for i in (0,1,2,3)] 
  if l != ["0","0","0","0"]:
    sys.stdout.write(",".join(l)+";")
  else:
    sys.stdout.write(";")

def collect():
  old = {} 
  current = {}
  nids = {}

  oldosts = []
  ostlist = []

  allnids = set()
  first = True

  while True:

    stime = time.time()
    if get_osts() != oldosts:
      ostlist = get_osts()
      # in case OST list changes (like failover), get nid list as well
      nidtime = 0

    # if nid timeout is over, get nid list, nid list is union of all nid lists
    # of OSTs, and has to be sorted when data is collected and written, to allow
    # one legend for all OSTs
    if nidtime+NIDTIMEOUT < time.time():
      nidtime = time.time()
      oldnids = allnids
      allnids = set()
      for ost in ostlist:
        nids[ost] = [ x for x in os.listdir("/proc/fs/lustre/obdfilter/"+ost+"/exports") if 'o2ib' in x ]
        allnids = allnids | set(nids[ost])
      if sorted(oldnids) != sorted(allnids):
        sys.stdout.write("#time;ost;rio;rb;wio;wb;")
        for nid in sorted(allnids):
          sys.stdout.write(nid+";")
        print

    # this time is the time written to each OST sample line of that
    # iteration. It is not the exact time, but makes it easyer to 
    # recognize same sample interval.
    sample = time.time()
    for ost in sorted(ostlist):
      current[ost] = get_ost_data(ost)
      for nid in nids[ost]:
        current[ost+"/"+nid] = get_nid_data(ost,nid)
      if not first:
        if current[ost][0]-old[ost][0]>0 or current[ost][2]-old[ost][2]>0:
          print str(sample)+";"+ost+";",
          dumptuple(current[ost],old[ost])
          for nid in sorted(allnids):
            dumptuple(current[ost+"/"+nid], old[ost+"/"+nid])
          print
    first = False
    old = current
    current = {}
    oldosts = ostlist[:]
    etime = time.time()
 
    # wait, skip the time it took to do the sample
    time.sleep(SLEEP-(etime-stime))

collect()
    





