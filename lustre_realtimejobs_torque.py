#!/usr/bin/env python

import time
import select
import _inotify

sys.path.append("MySQL")
import lustre_jobs_MySQL as lustre_jobs_db


ROOTDIR="/var/spool/torque/server_priv/accounting"

class Logfile:
    def __init__(self, prefix, path, filename):
        ''' path: directory where the file is
            filename: name of file to read
            prefix: filename has to start with this prefix
        '''
        self.path = path
        self.prefix = prefix
        self.filename = self.path+"/"+filename
        self.f = open(self.filename,"r")
        self.read_from_last_pos_to_end()

        self.db = lustre_jobs_db.DB()

    def read_from_last_pos_to_end(self):
        '''read from file from current position to current end, build lists for inserts and updates
        and do batch execution'''
        jobstarts = {}
        jobends = {}
        b=self.f.read()
        for l in b.split("\n"):
            sp = l[:-1].split(";")
            if len(sp)>1 and sp[1] in ["S","E"]:
              datestr=sp[0]
              if sp[1] == "S":
                end=-1
              else:
                end=int(time.mktime(time.strptime(datestr,"%m/%d/%Y %H:%M:%S")))
              jobid=sp[2]
              fi = sp[3].split()
              for i in fi:
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
                  if sp[1] == "S":
                    jobstarts[jobid] = (jobid, start, end, owner, hosts, "") 
                  else:
                    jobends[jobid] = (jobid, start, end, owner, hosts, "")
        inserts = []
        updates = []
        for i in jobstarts:
            if i not in jobends:
                inserts.append(jobstarts[i])
            else:
                inserts.append(jobends[i])
        for i in jobends:
            if i not in jobstarts:
                updates.append(jobends[i])
       
        # insert into DB - executemany is hard to achieve, as we need to insert users as well
        for j in inserts:
            self.db.insert_job(j)
        for j in updates:
            self.db.update_job(j)

    def switch_file(self,filename):
        todayfile = time.strftime("%Y%m%d")
        if filename.startswith(self.prefix) and todayfile in filename:
            self.read_from_last_pos_to_end()
            self.f.close()
            self.filename = self.path+"/"+filename
            self.f = open(self.filename, "r")
        #print "new file", self.filename

    def action(self, e):
        if e["mask"] & _inotify.CREATE:
            self.switch_file(e["name"])
        if e["mask"] & _inotify.MODIFY:
            self.read_from_last_pos_to_end()
        

def mainloop():

  fd = _inotify.create()
  wddir = _inotify.add(fd, "watchdir", _inotify.CREATE | _inotify.MODIFY) 

  todayfile = time.strftime("%Y%m%d")
  todayfile = "20140320"
  lf = Logfile("","watchdir",todayfile)

  while True:
    # blocking wait
    _inotify.read_event(fd, lf.action)
    time.sleep(0.1)
    

if __name__ == "__main__":
  mainloop() 
