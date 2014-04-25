#!/usr/bin/env python

import sys
import time,pwd
import select
import _inotify

sys.path.append("MySQL")
import lustre_jobs_MySQL as lustre_jobs_db


# ROOTDIR="/var/spool/torque/server_priv/accounting"
ROOTDIR="/home/berger/Lustre/testdata/watch"

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
        self.db = lustre_jobs_db.DB()
        self.read_from_last_pos_to_end()

    def getvalue(self, l, key):
        try:
            p = l.index(key)
        except ValueError:
            return None
        return l[p+1]

    def read_from_last_pos_to_end(self):
        '''read from file from current position to current end, build lists for inserts and updates
        and do batch execution'''
        
        resToJob = {}   # map resid to job
        jobs = {}

        jobstarts = {}
        jobends = {}
        b=self.f.read()
        ### aps #######
        for l in b.split("\n"):
            if "Bound apid" in l:
                sp = l[:-1].split()
                jobid=self.getvalue(sp, "batchId")[1:-1]
                resid=self.getvalue(sp, "resId")
                resToJob[resid] = jobid
                jobs[jobid] = {'jobid':jobid}

            if "Placed apid" in l:
                sp = l[:-1].split()
                sstart=sp[0]+" "+sp[1][:-1]
                start = int(time.mktime(time.strptime(sstart,"%Y-%m-%d %H:%M:%S")))
                resid=self.getvalue(sp, "resId")
                uid=self.getvalue(sp, "uid")
                cmd=self.getvalue(sp, "cmd0")[1:-1]
                nids=self.getvalue(sp, "nids:")
                try:
                    jobs[resToJob[resid]]['start'] = start
                    jobs[resToJob[resid]]['cmd'] = cmd
                    try:
                        # jobs[resToJob[resid]]['owner'] = usermap[uid]
                        jobs[resToJob[resid]]['owner'] = pwd.getpwuid(int(uid)).pw_name
                    except KeyError:
                        print "unknown userid", uid
                        jobs[resToJob[resid]]['owner'] = uid
                    jobs[resToJob[resid]]['nids'] = nids
                    jobid = resToJob[resid]
                    jobstarts[jobid] = (jobid, jobs[jobid]['start'], -1, jobs[jobid]['owner'], jobs[jobid]['nids'], jobs[jobid]['cmd'])
                except KeyError:
                    print "job without binding",resid

            if "Released apid" in l:   
                sp = l[:-1].split()
                send=sp[0]+" "+sp[1][:-1]
                end = int(time.mktime(time.strptime(send,"%Y-%m-%d %H:%M:%S")))
                resid=self.getvalue(sp, "resId")
                try:
                    jobs[resToJob[resid]]['end'] = end
                except KeyError:
                    print "job without start",resid
                else:
                    #print jobs[resToJob[resid]] 
                    if not 'start' in jobs[resToJob[resid]]:
                        print "job not placed",resid
                    else:
                        # db.insert_job(**jobs[resToJob[resid]])
                        jobid = resToJob[resid]
                        jobends[jobid] = (jobid, jobs[jobid]['start'], jobs[jobid]['end'], jobs[jobid]['owner'], jobs[jobid]['nids'], jobs[jobid]['cmd'])

        ### aps end ######
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
            print "insert", j
            self.db.insert_job(*j)
        for j in updates:
            print  "update",j
            self.db.update_job(*j)

    def switch_file(self,filename):
        todayfile = time.strftime("%Y%m%d")
        if filename.startswith(self.prefix) and todayfile in filename:
            self.read_from_last_pos_to_end()
            self.f.close()
            self.filename = self.path+"/"+filename
            self.f = open(self.filename, "r")
        # print "new file", self.filename

    def action(self, e):
        if e["mask"] & _inotify.CREATE:
            self.switch_file(e["name"])
        if e["mask"] & _inotify.MODIFY:
            self.read_from_last_pos_to_end()
        

def mainloop():

  fd = _inotify.create()
  wddir = _inotify.add(fd, ROOTDIR, _inotify.CREATE | _inotify.MODIFY) 

  todayfile = "apsched"+time.strftime("%Y%m%d")
  # todayfile = "apsched20131221"
  lf = Logfile("apsched",ROOTDIR,todayfile)

  while True:
    # blocking wait
    _inotify.read_event(fd, lf.action)
    time.sleep(0.1)
    

if __name__ == "__main__":
  mainloop() 
