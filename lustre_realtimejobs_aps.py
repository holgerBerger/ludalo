#!/usr/bin/env python

import sys
import time
import pwd
import _inotify
import anydbm

# new time format
import dateutil.parser
import calendar

sys.path.append("MySQL")
import MySQLObject 

# ROOTDIR="/var/spool/torque/server_priv/accounting"
ROOTDIR = "/home/berger/Lustre/testdata/watch"


class Logfile:
    def __init__(self, prefix, path, filename):
        ''' path: directory where the file is
            filename: name of file to read
            prefix: filename has to start with this prefix
        '''
        self.path = path
        self.prefix = prefix
        self.filename = self.path + "/" + filename
        self.f = open(self.filename, "r")
        self.db = MySQLObject.MySQLObject()

        # map resid to job - make it persistent as Bound line is only line containing jobid
        self.resToJob = anydbm.open('resToJob', 'c')
        print "init:",self.resToJob

        self.read_from_last_pos_to_end()

    def getvalue(self, l, key):
        # get values for specified key from list l of form: key value key value
        try:
            p = l.index(key)
        except ValueError:
            return None
        return l[p + 1]

    def read_from_last_pos_to_end(self):
        '''read from file from current position to current end,
        build lists for inserts and updates and do batch execution'''

        jobs = {}

        jobstarts = {}
        jobends = {}
        b = self.f.read()
        ### aps #######
        for l in b.split("\n"):
            if "Bound apid" in l:
                sp = l[:-1].split()
                jobid = self.getvalue(sp, "batchId")[1:-1]
                resid = self.getvalue(sp, "resId")
                # Cray resid is used in logfile to identfy jobs, this "Bound apid" line is the only line containing batchId
                # so later on we will map resid to batchId as batchId is used in database, to make it easyer to map
                # database data to existing jobs
                self.resToJob[resid] = jobid + self.db.batchpostfix    # we add batchserver here as cray log files do not contain it!!!
                self.resToJob.sync()
                #jobs[jobid] = {'jobid': jobid}

            if "Placed apid" in l:
                sp = l[:-1].split()
                # OLD direct logfiel format 2014-01-27 00:01:19:
                #  sstart = sp[0] + " " + sp[1][:-1]
                #  start = int( time.mktime( time.strptime(sstart, "%Y-%m-%d %H:%M:%S") ) )
                # NEW time format after syslog 2014-06-12T16:01:59.829416+02:00
                start = calendar.timegm(dateutil.parser.parse(sp[0]).utctimetuple())
                resid = self.getvalue(sp, "resId")
                uid = self.getvalue(sp, "uid")
                cmd = self.getvalue(sp, "cmd0")[1:-1]
                nids = self.getvalue(sp, "nids:")
                try:
                    # FIXME need way to handel usermapping when machine does not have user db
                    try:
                        # jobs[resToJob[resid]]['owner'] = usermap[uid]
                        owner = pwd.getpwuid(int(uid)).pw_name
                    except KeyError:
                        print "unknown userid", uid
                        owner = uid
                    jobid = self.resToJob[resid]
                    self.db.insert_job(jobid, start, -1, owner, nids, cmd)
                except KeyError:
                    print "job without binding", resid

            if "Released apid" in l:
                sp = l[:-1].split()
                # OLD format
                # send = sp[0] + " " + sp[1][:-1]
                # end = int(time.mktime(time.strptime(send, "%Y-%m-%d %H:%M:%S")))
                # NEW format
                end = calendar.timegm(dateutil.parser.parse(sp[0]).utctimetuple())
                resid = self.getvalue(sp, "resId")
                try:
                    jobid = self.resToJob[resid]
                    self.db.update_job(jobid, -1, end, "", "", "")
                except KeyError:
                    print "job without binding", resid
                # be nice and shrink the DB
                try:
                    del self.resToJob[resid]
                except KeyError:
                    pass # we give a ...

        self.db.commit()

    def switch_file(self, filename):
        todayfile = time.strftime("%Y%m%d")
        if filename.startswith(self.prefix) and todayfile in filename:
            self.read_from_last_pos_to_end()
            self.f.close()
            self.filename = self.path + "/" + filename
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

    todayfile = "apsched" + time.strftime("%Y%m%d")
    # todayfile = "apsched20131221"
    lf = Logfile("apsched", ROOTDIR, todayfile)

    while True:
    # blocking wait
        _inotify.read_event(fd, lf.action)
        time.sleep(0.1)

if __name__ == "__main__":
    mainloop()
