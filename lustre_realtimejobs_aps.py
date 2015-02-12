#!/usr/bin/env python

""" @brief      Collect Job data from
    @details    This graps the job information form aps and push it
                to the databas.
    @author     Holger Berger
"""

import time
import pwd
import _inotify
import anydbm

# new time format
import dateutil.parser
import calendar

import lib.database as database

# ROOTDIR="/var/spool/torque/server_priv/accounting"
ROOTDIR = "/home/berger/Lustre/testdata/watch"
FILEPREFIX = "apssched"


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
        dbconfig = database.DatabaseConfigurator('db.conf')
        self.db = dbconfig.getNewDB_Mongo_Conn()

        # map resid to job - make it persistent as Bound line is only line
        # containing jobid
        self.resToJob = anydbm.open('resToJob', 'c')
        # print "init:",self.resToJob
        self.usermap = {}
        self.readusermap()

        self.read_from_last_pos_to_end()

    def readusermap(self):
        '''read mapping file in format of /etc/passwd'''
        try:
            f = open("/etc/passwd", "r")
        except Exception, e:
            raise e
            exit()

        for l in f:
            sp = l.split(":")
            # self.usermap(sp[2]) = sp[0]  # this is not the funktion you are
            # looking for...
            self.usermap[sp[2]] = sp[0]
        f.close()
        self.usermapage = time.time()

    def mapuser(self, uid):
        ''' map uid to username, read mapping file at most every 5 minutes, if a user is not known
            try to read mapping file, if still not known, return uid'''
        # read only every 5 minutes in case of unknown users
        if uid not in self.usermap and time.time() > self.usermapage + 300:
            self.readusermap()
        if uid not in self.usermap:
            return uid
        else:
            return self.usermap[uid]

    def getvalue(self, l, key):
        ''' get values for specified key from list l of form: key value key value'''
        try:
            p = l.index(key)
        except ValueError:
            return None
        return l[p + 1]

    def read_from_last_pos_to_end(self):
        '''read from file from current position to current end,
        build lists for inserts and updates and do batch execution'''

#        b = self.f.read()
# aps #######
#        for l in b.split("\n"):
        # code to ignore incomplete lines, ignore garbage and seek back to last end of line
        # in case of single incomplete line, it is supposed to work as well
        lines = self.f.readlines()
        if len(lines) == 0:
            return
        if lines[-1][-1] != '\n':
            # seek back to last end of line
            self.f.seek(0 - len(lines[-1]), 1)
            del lines[-1]
        # aps #######
        for l in lines:
            if "Bound apid" in l:
                sp = l[:-1].split()
                jobid = self.getvalue(sp, "batchId")[1:-1]
                resid = self.getvalue(sp, "resId")
                # Cray resid is used in logfile to identfy jobs, this "Bound apid" line is the only line containing batchId
                # so later on we will map resid to batchId as batchId is used in database, to make it easyer to map
                # database data to existing jobs
                # we add batchserver here as cray log files do not contain
                # it!!!
                self.resToJob[resid] = jobid + self.db.batchpostfix
                self.resToJob.sync()
                #jobs[jobid] = {'jobid': jobid}

            if "Placed apid" in l:
                sp = l[:-1].split()
                # OLD direct logfiel format 2014-01-27 00:01:19:
                #  sstart = sp[0] + " " + sp[1][:-1]
                #  start = int( time.mktime( time.strptime(sstart, "%Y-%m-%d %H:%M:%S") ) )
                # NEW time format after syslog 2014-06-12T16:01:59.829416+02:00
                start = calendar.timegm(
                    dateutil.parser.parse(sp[0]).utctimetuple())
                resid = self.getvalue(sp, "resId")
                uid = self.getvalue(sp, "uid")
                cmd = self.getvalue(sp, "cmd0")[1:-1]
                nids = self.getvalue(sp, "nids:")
                try:
                    try:
                        owner = pwd.getpwuid(int(uid)).pw_name
                    except KeyError:
                        # in case /etc/passwd does not contain user, we check
                        # file mapping
                        owner = self.mapuser(uid)
                    jobid = self.resToJob[resid]
                    self.db.insert_jobData(jobid, start, -1, owner, nids, cmd)
                    print "jobstart:", jobid, "owner:", owner
                except KeyError:
                    print "job without binding", resid

            if "Released apid" in l:
                sp = l[:-1].split()
                # OLD format
                # send = sp[0] + " " + sp[1][:-1]
                # end = int(time.mktime(time.strptime(send, "%Y-%m-%d %H:%M:%S")))
                # NEW format
                end = calendar.timegm(
                    dateutil.parser.parse(sp[0]).utctimetuple())
                resid = self.getvalue(sp, "resId")
                try:
                    jobid = self.resToJob[resid]
                    self.db.update_jobData(jobid, -1, end, "", "", "")
                except KeyError:
                    print "job without binding", resid
                # be nice and shrink the DB
                try:
                    del self.resToJob[resid]
                except KeyError:
                    pass  # we give a ...

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
    # wddir = _inotify.add(fd, ROOTDIR, _inotify.CREATE | _inotify.MODIFY)
    # try without asingment
    _inotify.add(fd, ROOTDIR, _inotify.CREATE | _inotify.MODIFY)

    todayfile = FILEPREFIX + time.strftime("%Y%m%d")
    # todayfile = "apsched20131221"
    lf = Logfile(FILEPREFIX, ROOTDIR, todayfile)

    while True:
    # blocking wait
        _inotify.read_event(fd, lf.action)
        time.sleep(0.1)

if __name__ == "__main__":
    mainloop()
