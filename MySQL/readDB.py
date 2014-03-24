#!/usr/bin/env python
'''
Created on 18.02.2014

@author: uwe
'''

import sys
import time
import datetime
import MySQLdb
from threading import Thread, Lock
from multiprocessing.pool import Pool
from ConfigParser import ConfigParser

sys.path.append("../Analysis")
from plotGraph import plotGraph


class readDB(object):

    def __init__(self, dbFile):
        '''
        Constructor
        '''
        self.DB_VERSION = 2
        self.dbFile = dbFile
        self.config = ConfigParser()
        try:
            self.config.readfp(open("db.conf"))
        except IOError:
            print "no db.conf file found."
            sys.exit()
        self.dbname = self.config.get("database", "name")
        self.dbpassword = self.config.get("database", "password")
        self.dbhost = self.config.get("database", "host")
        self.dbuser = self.config.get("database", "user")
        self.conn = MySQLdb.connect(passwd=self.dbpassword, db=self.dbname, host=self.dbhost, user=self.dbuser)
        self.c = self.conn.cursor()
        if not self.check_version():
            self.c.execute(''' select
                                    version
                                from
                                    version
                                order by id desc
                                limit 1 ''')

            v = self.c.fetchone()
            if not v:
                print 'please regenerate database!!!'
                sys.exit(1)
            version = v[0]
            print ('\nThere is something wrong with the Database\n' +
                       'DB version is ' + str(version) +
                       ''' but expect version ''' +
                       str(self.DB_VERSION))
            sys.exit(0)

#------------------------------------------------------------------------------
    def check_version(self):
        self.c.execute(''' select
                                version
                            from
                                version
                            order by id desc
                            limit 1 ''')
        version = self.c.fetchone()
        if version:
            if version[0] == self.DB_VERSION:
                return True
            else:
                return False
        else:
            return False

#------------------------------------------------------------------------------

    def read_write_sum_to_Nid(self, start, end, nidName):
        self.c.execute('''  select
                                samples_ost.rb,
                                samples_ost.wb,
                                timestamps.timestamp,
                                nids.nid,
                                samples_ost.rio,
                                samples_ost.wio
                            from
                                samples_ost,
                                nids,
                                timestamps
                            where
                                nids.id = samples_ost.nid
                                    and timestamps.id = samples_ost.timestamp
                                    and timestamps.timestamp between %s and %s
                                    and nids.nid = %s''',
                                    (start, end, nidName))
        tmp = self.c.fetchall()

        timeMapRB = {}
        timeMapWB = {}
        timeMapRIO = {}
        timeMapWIO = {}
        self.c.execute('''
                            select
                                timestamp
                            from
                                timestamps
                            where
                                timestamp between %s and %s''',
                                (start, end))
        tmp_time = self.c.fetchall()
        for timeStamp in tmp_time:
            timeMapRB[timeStamp[0]] = 0
            timeMapWB[timeStamp[0]] = 0
            timeMapRIO[timeStamp[0]] = 0
            timeMapWIO[timeStamp[0]] = 0

        nidList = set()
        for item in tmp:
            read = item[0]
            write = item[1]
            timestamp = item[2]
            nid = item[3]
            rio = item[4]
            wio = item[5]
            if timestamp not in timeMapRB or timeMapWB or timeMapRIO or timeMapWIO:
                timeMapRB[timestamp] = 0
                timeMapWB[timestamp] = 0
                timeMapRIO[timestamp] = 0
                timeMapWIO[timestamp] = 0
            timeMapRB[timestamp] += read
            timeMapWB[timestamp] += write
            timeMapRIO[timestamp] += rio
            timeMapWIO[timestamp] += wio
            nidList.add(nid)

        return (start, end, timeMapRB, timeMapWB,
                    timeMapRIO, timeMapWIO, nidList)

#------------------------------------------------------------------------------
    def get_sum_nids_to_job(self, jobID):
        start_end = self.get_job_start_end(jobID)
        if start_end:
            start = start_end[0] - 60
            end = start_end[1] + 60
            if not (end - start < 900):
                #nids = self.get_nid_to_Job(jobID)   # moved here for performance
                #print 'find nids'
                #colReturn = []
                #for nid in nids:
                #    colReturn.append(self.read_write_sum_to_Nid(start, end, nid))
                #return colReturn

                # Test of the job join
                #(start, end, timeMapRB, timeMapWB, timeMapRIO, timeMapWIO, nidList)
                query = '''select
                                sum(samples_ost.rb),
                                sum(samples_ost.wb),
                                sum(samples_ost.rio),
                                sum(samples_ost.wio),
                                timestamps.timestamp,
                                nids.nid
                            from
                                nids
                                    join
                                nodelist ON nids.id = nodelist.nid
                                    join
                                jobs ON jobs.id = nodelist.job
                                    and jobs.id = %s
                                    join
                                samples_ost ON nids.id = samples_ost.nid
                                    join
                                timestamps ON timestamps.id = samples_ost.timestamp
                                    and timestamps.timestamp between %s and %s
                            group by nids.nid , timestamps.timestamp'''
                self.c.execute(query, (jobID, start, end,))
                query_result = self.c.fetchall()

                self.c.execute('''
                    select
                        timestamp
                    from
                        timestamps
                    where
                        timestamp between %s and %s''',
                        (start, end))
                tmp_time = self.c.fetchall()

                nidMap = {}
                for row in query_result:
                    rb_sum = row[0]
                    wb_sum = row[1]
                    rio_sum = row[2]
                    wio_sum = row[3]
                    timestamp = row[4]
                    nid = row[5]
                    value_tuple = nidMap.get(nid, None)

                    if not value_tuple:
                        timeMapRB = {}
                        timeMapWB = {}
                        timeMapRIO = {}
                        timeMapWIO = {}
                        # init with 0
                        for timeStamp in tmp_time:
                            timeMapRB[timeStamp[0]] = 0
                            timeMapWB[timeStamp[0]] = 0
                            timeMapRIO[timeStamp[0]] = 0
                            timeMapWIO[timeStamp[0]] = 0

                        timeMapRB[timestamp] = rb_sum
                        timeMapWB[timestamp] = wb_sum
                        timeMapRIO[timestamp] = rio_sum
                        timeMapWIO[timestamp] = wio_sum

                        nidMap[nid] = (timeMapRB, timeMapWB, timeMapRIO, timeMapWIO)
                    else:
                        timeMapRB = value_tuple[0]
                        timeMapWB = value_tuple[1]
                        timeMapRIO = value_tuple[2]
                        timeMapWIO = value_tuple[3]

                        timeMapRB[timestamp] = rb_sum
                        timeMapWB[timestamp] = wb_sum
                        timeMapRIO[timestamp] = rio_sum
                        timeMapWIO[timestamp] = wio_sum

                        nidMap[nid] = (timeMapRB, timeMapWB, timeMapRIO, timeMapWIO)
                colReturn = []
                for nid in nidMap.keys():
        #(start, end, timeMapRB, timeMapWB, timeMapRIO, timeMapWIO, nidList)
                    value_tuple = nidMap[nid]
                    timeMapRB = value_tuple[0]
                    timeMapWB = value_tuple[1]
                    timeMapRIO = value_tuple[2]
                    timeMapWIO = value_tuple[3]
                    colReturn.append((start, end, timeMapRB, timeMapWB, timeMapRIO, timeMapWIO))

                return colReturn

            else:
                return None
        else:
            return None
#------------------------------------------------------------------------------

    def get_job_start_end(self, jobID):
        self.c.execute('''
                        select t_start, t_end
                        from jobs
                        where id = %s ''', (jobID,))
        start_end = self.c.fetchall()

        self.c.execute('''
                        select timestamp from timestamps
                        order by timestamp desc
                        limit 1 ''')
        samples_max = self.c.fetchall()
        self.c.execute('''
                        select timestamp from timestamps
                        order by timestamp
                        limit 1 ''')
        samples_min = self.c.fetchall()

        # out of sample range
        if not (start_end[0][0] < samples_min[0][0] or start_end[0][1] > samples_max[0][0]):
            if start_end:
                return start_end[0]
            else:
                return None
        else:
            return None
#------------------------------------------------------------------------------

    def get_nid_to_Job(self, jobID):
        self.c.execute('''
                        select nids.nid
                        From jobs, nids, nodelist
                        where nids.id = nodelist.nid
                        and jobs.id = nodelist.job
                        and jobs.id = %s;
                        ''', (jobID,))
        nids = self.c.fetchall()
        nidReturn = []
        for nid in nids:
            nidReturn.append(nid[0])

        return nidReturn

#------------------------------------------------------------------------------

    def get_All_Users_r_w(self, houers, rw_in_MB=1):
        ''' give a user lists back with all users witch read and write is
            grader then rw_in_MB with in the last houers eg.
            get_All_Users_r_w(12, rw_in_MB=100) -> all users witch io is grader
            then 100MB in the last 12 houers. '''
        timestamp_end = time.time()
        timestamp_start = timestamp_end - (houers * 60 * 60)

        volume = rw_in_MB * 1000 * 1000

        p1 = self.getAll_Nid_IDs_Between(timestamp_start, timestamp_end, volume)
        userList = set()
        for p in p1:
            tmp = self.getAll_Jobs_to_Nid_ID_Between(timestamp_start, timestamp_end, p)
            if tmp:
                userList.add(self.get_User_To_Job(tmp[0][2])[0][0])
        return userList
#------------------------------------------------------------------------------

    def get_User_To_Job(self, jobID):
        self.c.execute('''
                       select users.username
                       from users, jobs
                       where users.id = jobs.owner
                       and jobid = %s ''', (jobID,))
        user = self.c.fetchall()
        return user
#------------------------------------------------------------------------------

    def getAll_Jobs_to_Nid_ID_Between(self, timeStamp_start, timeStamp_end, nidID):

        self.c.execute('''
            select owner, nodelist.nid, jobs.jobid, jobs.t_start, jobs.t_end
            from jobs, nodelist
            where nodelist.job = jobs.id
            and nodelist.nid = %s
            and jobs.end > %s
            and jobs.start < %s''', (nidID, timeStamp_start, timeStamp_end))
        job_list = self.c.fetchall()
        return job_list
#------------------------------------------------------------------------------

    def get_hi_lo_TimestampsID_Between(self, timeStamp_start, timeStamp_end):
        self.c.execute(''' SELECT * FROM timestamps
                            WHERE timestamp BETWEEN %s AND %s
                            order by timestamp desc
                            limit 1
                            ''', (timeStamp_start, timeStamp_end))
        t_end = self.c.fetchone()

        self.c.execute(''' SELECT * FROM timestamps
                            WHERE timestamp BETWEEN %s AND %s
                            order by timestamp
                            limit 1
                            ''', (timeStamp_start, timeStamp_end))
        t_start = self.c.fetchone()

        if not t_start and not t_end:
            return None
        else:
            return (t_start[0], t_end[0])
#------------------------------------------------------------------------------

    def getAll_Nid_IDs_Between(self, timeStamp_start, timeStamp_end, threshold_b=0):
        ''' get all nids between two timestamps  if thershold only nids with
            more rb or wb between this timestamps'''

        timestamp = self.get_hi_lo_TimestampsID_Between(timeStamp_start, timeStamp_end)

        if timestamp:
            timeStamp_end_id = timestamp[1]
            timeStamp_start_id = timestamp[0]

            self.c.execute(''' SELECT nid, rb, wb FROM samples_ost
                                WHERE timestamp BETWEEN %s AND %s
                                ''', (timeStamp_start_id, timeStamp_end_id))
            c = self.c.fetchall()
            nidDictrb = {}
            nidDictwb = {}
            for row in c:
                nid = row[0]
                rb = row[1]
                wb = row[2]

                if nid not in nidDictrb:
                    nidDictrb[nid] = 0

                if nid not in nidDictwb:
                    nidDictwb[nid] = 0

                nidDictrb[nid] += rb
                nidDictwb[nid] += wb

            for key in nidDictrb.keys():
                if nidDictrb[key] < threshold_b:
                    del(nidDictrb[key])

            for key in nidDictwb.keys():
                if nidDictwb[key] < threshold_b:
                    del(nidDictwb[key])

            tmp = nidDictrb.keys()
            collReturn = nidDictwb.keys()

            collReturn = set(collReturn)
            tmp = set(tmp)

            collReturn = collReturn | tmp

            return list(collReturn)
        else:
            return None
#------------------------------------------------------------------------------

    def getTimeStamp(self, year, month, day, houer, minute):
        ''' convert from year day month to time stamp '''
        dateTimeInput = datetime.datetime(year, month, day, houer, minute)
        timeStamp = time.mktime(dateTimeInput.timetuple())
        timeStamp = int(timeStamp)
        return timeStamp
#------------------------------------------------------------------------------

    def timeStampToDate(self, timeStamp):
        ''' converts form time stamp to year day month '''
        return datetime.datetime.fromtimestamp(
                                float(timeStamp)).strftime('%Y-%m-%d %H:%M:%S')
#------------------------------------------------------------------------------

    def explainJob(self, jobID):
        query = ''' select
                        jobs.jobid,
                        jobs.t_start,
                        jobs.t_end,
                        users.username,
                        jobs.nodelist
                    from
                        jobs,
                        users
                    where
                        jobs.id = %s
                        and users.id = jobs.owner'''
        self.c.execute(query, (jobID,))
        head = self.c.description
        informations = zip(zip(*head)[0], self.c.fetchall()[0])
        print informations[0][0], informations[0][1], informations[3][0], informations[3][1]
        print 'Duration:', (informations[2][1] - informations[1][1]) / 60, 'min'
        number_of_nodes = len(informations[4][1].split(','))
        print 'Number of Nodes:', number_of_nodes
#------------------------------------------------------------------------------


def print_job(job):

    db = readDB('sqlite_new.db')

    check_job = db.c.execute('''
                    select * from jobs where id = %s
                     ''', (job,))

    if not check_job:
        print 'No such job: ', job
        sys.exit(0)

    sum_nid = db.get_sum_nids_to_job(job)

    if sum_nid:
        jobid = job

        db.c.execute('''
                select jobs.jobid, users.username , jobs.nodelist
                from jobs, users
                where jobs.id = %s
                and users.id = jobs.owner''', (jobid,))
        job_info = db.c.fetchone()

        db.c.execute('''select nid from nodelist where job = %s''', (jobid,))
        nids = db.c.fetchall()
        title = 'Job_' + str(job_info[0]) + '_NoN_' + str(len(nids)) + '__Owner_' + str(job_info[1])
        List_of_lists = []
        read_sum = []
        write_sum = []
        io_sum = []
        for nid in sum_nid:
        #(start, end, timeMapRB, timeMapWB, timeMapRIO, timeMapWIO, nidList)
            start = nid[0]
            end = nid[1]
            readDic = nid[2]
            writeDic = nid[3]
            rioDic = nid[4]
            wioDic = nid[5]

            readY = []
            writeY = []
            ioread = []
            iowrite = []
            writeX = sorted(writeDic.keys())
            readX = sorted(readDic.keys())

            for timeStamp in readX:
                readY.append(float(-readDic[timeStamp]) / (60 * 1000000))
                writeY.append(float(writeDic[timeStamp]) / (60 * 1000000))

                read_sum.append(readDic[timeStamp])
                write_sum.append(writeDic[timeStamp])
                io_sum.append(rioDic[timeStamp] + wioDic[timeStamp])

                ioread.append(-rioDic[timeStamp])
                iowrite.append(wioDic[timeStamp])

            if readX and readY and writeY and writeX:
                List_of_lists.append(readX)
                List_of_lists.append(readY)

                List_of_lists.append(writeX)
                List_of_lists.append(writeY)
        print 'Plot: ', title
        write_sum_b = sum(write_sum)
        read_sum_b = sum(read_sum)
        io_sum_b = sum(io_sum)
        query = ''' UPDATE jobs
                    SET r_sum = %s, w_sum = %s, reqs_sum = %s
                    where jobs.id = %s  '''
        db.c.execute(query, (read_sum_b, write_sum_b, io_sum_b, job))
        plotGraph(List_of_lists, title)

if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    db = readDB('sqlite_db.db')
    db.c.execute('''select id, jobid from jobs''')
    jobs = db.c.fetchall()
    valid_jobs = []

    print '# of jobs: ' + str(len(jobs))
    db.conn.commit()

    for job in jobs:
        start_end = db.get_job_start_end(job[0])
        if start_end:
            start = start_end[0]
            end = start_end[1]
            if not (end - start < 900):
                valid_jobs.append(job[0])

    print '# of valid jobs: ' + str(len(valid_jobs))

    pool = Pool()
    pool.map(print_job, valid_jobs)

    db.conn.commit()

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in:", str(time_end - time_start), "sec"
