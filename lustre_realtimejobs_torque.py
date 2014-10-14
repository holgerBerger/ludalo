#!/usr/bin/env python

import time
import _inotify
import lib.database as database


ROOTDIR = "/var/spool/torque/server_priv/accounting"


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
        self.read_from_last_pos_to_end()

    def read_from_last_pos_to_end(self):
        '''read from file from current position to current end, build lists for inserts and updates
        and do batch execution'''
        jobstarts = {}
        jobends = {}
        b = self.f.read()
        for l in b.split("\n"):
            sp = l[:-1].split(";")
            # A ABORT / D delete do not always produce E record
            if len(sp) > 1 and sp[1] in ["S", "E", "A", "D"]:
                jobid = sp[2]
                datestr = sp[0]
                if sp[1] == "S":
                    fi = sp[3].split()
                    for i in fi:
                        if i.startswith("start"):
                            start = int(i.split('=')[1])
                        if i.startswith("user"):
                            owner = i.split('=')[1]
                        if i.startswith("exec_host"):
                            l = []
                            for n in [x.split('/')[0] for x in i.split('=')[1].split("+")]:
                                if n not in l:
                                    l.append(n)
                            hosts = ",".join(l)
                    end = -1
                    jobstarts[jobid] = (jobid, start, end, owner, hosts, "")
                    print "jobstart:", jobid, "owner:", owner, len(l), "nodes"
                else:
                    end = int(
                        time.mktime(time.strptime(datestr, "%m/%d/%Y %H:%M:%S")))
                    start = -1
                    owner = ""
                    hosts = ""
                    jobends[jobid] = (jobid, start, end, owner, hosts, "")
                    print "jobend:", jobid

        inserts = []
        updates = []
        # if S and E come together, merge them into one insert
        for i in jobstarts:
            if i not in jobends:
                inserts.append(jobstarts[i])
            else:
                tmp = list(jobstarts[i])
                tmp[2] = jobends[i][2]
                inserts.append(tuple(tmp))
        for i in jobends:
            if i not in jobstarts:
                updates.append(jobends[i])

        # insert into DB - executemany is hard to achieve, as we need to insert
        # users as well
        for j in inserts:
            self.db.insert_jobData(*j)
        for j in updates:
            self.db.update_jobData(*j)
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

    todayfile = time.strftime("%Y%m%d")
    # todayfile = "20140320"
    lf = Logfile("", ROOTDIR, todayfile)

    while True:
    # blocking wait
        _inotify.read_event(fd, lf.action)
        time.sleep(0.1)


if __name__ == "__main__":
    mainloop()
