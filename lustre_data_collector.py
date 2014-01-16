#!/usr/bin/env python

# data collector for OSS
#
# tested with lustre 1.8.4
# has to work with python 2.4 for redhat 5.x based OSS
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

from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import SocketServer
import signal
import os,time,sys,getopt

# get new sample after SLEEP seconds
SLEEP = 60

# update NID list after NIDTIMEOUT seconds, can be longer than SLEEP
# in case of short SLEEP, that reduces number of directory lookups
# which is (#OSTS * #NIDS) and can be a factor in case of large #NIDS
NIDTIMEOUT = 60


class OSS_RPCSERVER:

  def __init__(self):
    self.first = True
    self.old = {}

  def get_osts(self):
    return [ x for x in os.listdir("/proc/fs/lustre/obdfilter/") if 'OST' in x ]

  def __get_ost_data(self, ost):
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

  def __get_nid_data(self, ost, nid):
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
    
  def __dumptuple(self, data, old):
    """write out a tuple, write delta and nothing if only zeros"""
    l = [ str(data[i]-old[i]) for i in (0,1,2,3)] 
    if l != ["0","0","0","0"]:
      return l
    else:
      return ()


  def get_sample(self):
    current = {}
    nids = {}

    oldosts = []
    ostlist = []

    allnids = set()

    result=[]

    ostlist = self.get_osts()

    # form explanation comment and return as first element in list of results
    explanation=[]
    allnids = set()
    for ost in ostlist:
      nids[ost] = [ x for x in os.listdir("/proc/fs/lustre/obdfilter/"+ost+"/exports") if 'o2ib' in x ]
      allnids = allnids | set(nids[ost])
    explanation.append("#time;ost;rio;rb;wio;wb;".split(";"))
    for nid in sorted(allnids):
      explanation.append(nid)
    result.append(explanation)

    anydata = False
    # form ost statistics lines and append to results
    for ost in sorted(ostlist):
      current[ost] = self.__get_ost_data(ost)
      ostline = []
      for nid in nids[ost]:
        current[ost+"/"+nid] = self.__get_nid_data(ost,nid)
      if not self.first:
        if current[ost][0]-self.old[ost][0]>0 or current[ost][2]-self.old[ost][2]>0:
          anydata = True
          ostline.append(ost)
          ostline.append(self.__dumptuple(current[ost],self.old[ost]))
          for nid in sorted(allnids):
            ostline.append(self.__dumptuple(current[ost+"/"+nid], self.old[ost+"/"+nid]))
          result.append(ostline)
    self.first = False
    self.old = current
    current = {}
    oldosts = ostlist[:]

    if anydata:
      return result
    else:
      return []


# Threaded mix-in
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
  pass

def signalhandler(signum, frame):
  '''Signalhandler to exit broker'''
  if (signum != signal.SIGUSR1):
    sys.exit()


def rpc_server():
  '''main loop, provides XML-RPC server'''
  signal.signal(signal.SIGTERM, signalhandler)
  signal.signal(signal.SIGUSR1, signalhandler)
  signal.signal(signal.SIGINT, signalhandler)

  oss_rpcserver = OSS_RPCSERVER()

  server = AsyncXMLRPCServer(('', 8000), logRequests=False)
  server.register_introspection_functions()
  server.register_instance(oss_rpcserver)

  # we add a endless loop here to handle interrupts, we want to be able to quit, other signals should be resumed
  while (True):
    try:
      print "serve_forever"
      server.serve_forever()
    except SystemExit:
      break
    except:
      pass


rpc_server()
  
