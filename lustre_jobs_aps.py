#!/usr/bin/env python


# import apsscheduler logs data into database
# first argument is passwd file to be able to map UIDs to usernames, following arguments are logfiles

# parsed lines look like
#2013-12-21 00:02:03: Bound apid 0 resId 4072 pagg 0xba600000680 batchId '1179908'
#2013-12-21 00:02:04: Placed apid 3615838 resId 4072 pagg 0xba600000680 uid 15952 flags 0x22001 numCmds 1 cmd0 'tfs' nids: 3-5,12,26,38,52-53,56,78,87,94,105,118,1547,1572-1573,1576,1578,1581,1583,1589,1591-1592,1596-1597,1604,1618,1623,1738,1819-1820,1826-1830,1832-1835,1845-1846,1855-1860,2469,2472-2475,2477-2479,2513,2710,2853,2856,2858-2859,2880 (nid0 3)
#2013-12-21 00:02:04: apid 3615838 pTag 125 nttGran/ents 1/64 cookie 0x4f230000
#2013-12-21 00:09:08: type release uid 0 gid 0 apid 3615838 pagg 0 resId 4072 numCmds 0
#2013-12-21 00:09:08: Released apid 3615838 resId 4072 pagg 0xba600000680 claim


# FIXME does not yet contain handling of jobs not having start AND end in the single file

import sys,atexit,curses
import os.path
import time

sys.path.append("MySQL")
import lustre_jobs_MySQL as lustre_jobs_db
#sys.path.append("PSQL")
#import lustre_jobs_PSQL as lustre_jobs_db

def read_pw(filename, usermap):
  f = open(filename, "r")
  for l in f:
    sp = l.split(':')
    usermap[sp[2]] = sp[0] 
  print "read",len(usermap),"uid mappings"

def getvalue(l, key):
  try:
    p = l.index(key)
  except ValueError:
    return None
  return l[p+1]


def cleanup():
  print curses.tigetstr("cnorm")


if __name__ == "__main__":

  curses.setupterm()
  print curses.tigetstr("civis")
  atexit.register(cleanup)

  if len(sys.argv)<=2 or sys.argv[1] in ["-h", "--help"]:
    print "usage: %s passwdfile apsschedlog ..." % sys.argv[0]
    sys.exit(0)

  usermap = {}
  read_pw(sys.argv[1], usermap)

  db = lustre_jobs_db.DB('sqlite_new.db')

  db.create_tables()

  restojob = {}   # map resid to job
  jobs = {}

  for filename in sys.argv[2:]:
    f = open(filename, "r")
    filesize = os.path.getsize(filename)
    counter=0


    for l in f:
      if "Bound apid" in l:
        sp = l[:-1].split()
        jobid=getvalue(sp, "batchId")[1:-1]
        resid=getvalue(sp, "resId")
        restojob[resid] = jobid
        jobs[jobid] = {'jobid':jobid}
      if "Placed apid" in l:
        sp = l[:-1].split()
        sstart=sp[0]+" "+sp[1][:-1]
        start = int(time.mktime(time.strptime(sstart,"%Y-%m-%d %H:%M:%S")))
        resid=getvalue(sp, "resId")
        uid=getvalue(sp, "uid")
        cmd=getvalue(sp, "cmd0")[1:-1]
        nids=getvalue(sp, "nids:")
        try:
          jobs[restojob[resid]]['start'] = start
          jobs[restojob[resid]]['cmd'] = cmd
          try:
            jobs[restojob[resid]]['owner'] = usermap[uid]
          except KeyError:
            print "unknown userid", uid
            jobs[restojob[resid]]['owner'] = uid
          jobs[restojob[resid]]['nids'] = nids
        except KeyError:
          print "job without binding",resid," "*40
      if "Released apid" in l:   
        sp = l[:-1].split()
        send=sp[0]+" "+sp[1][:-1]
        end = int(time.mktime(time.strptime(send,"%Y-%m-%d %H:%M:%S")))
        resid=getvalue(sp, "resId")
        try:
          jobs[restojob[resid]]['end'] = end
        except KeyError:
          print "job without start",resid," "*40
        else:
          #print jobs[restojob[resid]] 
          if not 'start' in jobs[restojob[resid]]:
            print "job not placed",resid," "*40
          else:
            db.insert_job(**jobs[restojob[resid]])
          counter+=1
          if counter%10 == 0:
            print "read %d records / %d%% from %s\r"%(counter,int(float(f.tell())/float(filesize)*100.0), filename),

    print "read %d records / %d%% from %s"%(counter,int(float(f.tell())/float(filesize)*100.0), filename)
    f.close()

  db.close()
