#!/usr/bin/env python
'''
Created on 18.02.2014

@author: uwe
'''

import sys
sys.path.append("../Analysis")
import time
import datetime
import MySQLdb
from threading import Thread, Lock
from multiprocessing.pool import Pool
from ConfigParser import ConfigParser
from User import User
from Job import Job
import argparse
import numpy as np

from plotGraph import plotGraph, plotJob


class readDB(object):

    def __init__(self):
        '''
        Constructor
        '''
        self.DB_VERSION = 4
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
        self.conn = MySQLdb.connect(passwd=self.dbpassword,
                                    db=self.dbname,
                                    host=self.dbhost,
                                    user=self.dbuser)
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
                       str(self.DB_VERSION) + '\n')
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
    #  read_write_sum_to_Nid
#------------------------------------------------------------------------------
    def get_sum_nids_to_job(self, jobID):
        #depricated.
        start_end = self.get_job_start_end(jobID)
        if start_end:
            start = start_end[0] - 60
            endTime = start_end[1]
            if endTime < 0:
                end = time.time()
            else:
                end = endTime + 60
        #(start, end, timeMapRB, timeMapWB, timeMapRIO, timeMapWIO, nidList)
            query = '''
                    select
                        sum(samples_ost.rb),
                        sum(samples_ost.wb),
                        sum(samples_ost.rio),
                        sum(samples_ost.wio),
                        timestamps.c_timestamp,
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
                        timestamps ON timestamps.id = samples_ost.timestamp_id
                            and timestamps.c_timestamp between %s and %s
                    group by nids.nid , timestamps.c_timestamp'''
            self.c.execute(query, (jobID, start, end,))
            query_result = self.c.fetchall()

            self.c.execute('''
                select
                    c_timestamp
                from
                    timestamps
                where
                    c_timestamp between %s and %s''',
                    (start, end))
            tmp_time = self.c.fetchall()
            # Build nidMap
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
                    for timeStamps in tmp_time:
                        timeMapRB[timeStamps[0]] = 0
                        timeMapWB[timeStamps[0]] = 0
                        timeMapRIO[timeStamps[0]] = 0
                        timeMapWIO[timeStamps[0]] = 0

                    timeMapRB[timestamp] = rb_sum
                    timeMapWB[timestamp] = wb_sum
                    timeMapRIO[timestamp] = rio_sum
                    timeMapWIO[timestamp] = wio_sum

                    nidMap[nid] = (timeMapRB,
                                   timeMapWB,
                                   timeMapRIO,
                                   timeMapWIO)
                else:
                    timeMapRB = value_tuple[0]
                    timeMapWB = value_tuple[1]
                    timeMapRIO = value_tuple[2]
                    timeMapWIO = value_tuple[3]

                    timeMapRB[timestamp] = rb_sum
                    timeMapWB[timestamp] = wb_sum
                    timeMapRIO[timestamp] = rio_sum
                    timeMapWIO[timestamp] = wio_sum

                    nidMap[nid] = (timeMapRB,
                                   timeMapWB,
                                   timeMapRIO,
                                  timeMapWIO)
# build return collection
#(start, end, timeMapRB, timeMapWB, timeMapRIO, timeMapWIO, nidList, nidName)
            colReturn = []
            for nid in nidMap.keys():
                value_tuple = nidMap[nid]
                timeMapRB = value_tuple[0]
                timeMapWB = value_tuple[1]
                timeMapRIO = value_tuple[2]
                timeMapWIO = value_tuple[3]
                nidName = nid
                colReturn.append((start,
                                  endTime,
                                  timeMapRB,
                                  timeMapWB,
                                  timeMapRIO,
                                  timeMapWIO,
                                  nidName))

            print 'return(get_sum_nids_to_job) ', len(colReturn)
            return colReturn
        else:
            print 'start end time error'
            return None
#------------------------------------------------------------------------------

    def get_job_start_end(self, jobID):
        self.c.execute('''
                        select t_start, t_end
                        from jobs
                        where id = %s ''', (jobID,))
        start_end = self.c.fetchall()

        self.c.execute('''
                        select c_timestamp from timestamps
                        order by c_timestamp desc
                        limit 1 ''')
        #samples_max = self.c.fetchall()
        self.c.execute('''
                        select c_timestamp from timestamps
                        order by c_timestamp
                        limit 1 ''')
        samples_min = self.c.fetchall()

        if start_end:
            if start_end[0][0] > samples_min[0][0]:
                return start_end[0]
            else:
                start_end = (samples_min[0][0], start_end[0][1])
                return start_end
        else:
            print "Error by getting Job Start or End."
            exit()

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

        p1 = self.getAll_Nid_IDs_Between(timestamp_start,
                                         timestamp_end,
                                         volume)
        userList = set()
        for p in p1:
            tmp = self.getAll_Jobs_to_Nid_ID_Between(timestamp_start,
                                                     timestamp_end, p)
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

    def getAll_Jobs_to_Nid_ID_Between(self, timeStamp_start,
                                        timeStamp_end, nidID):

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
                            WHERE c_timestamp BETWEEN %s AND %s
                            order by c_timestamp desc
                            limit 1
                            ''', (timeStamp_start, timeStamp_end))
        t_end = self.c.fetchone()

        self.c.execute(''' SELECT * FROM timestamps
                            WHERE c_timestamp BETWEEN %s AND %s
                            order by c_timestamp
                            limit 1
                            ''', (timeStamp_start, timeStamp_end))
        t_start = self.c.fetchone()

        if not t_start and not t_end:
            return None
        else:
            return (t_start[0], t_end[0])
#------------------------------------------------------------------------------

    def getAll_Nid_IDs_Between(self, timeStamp_start,
                                timeStamp_end, threshold_b=0):
        ''' get all nids between two timestamps  if thershold only nids with
            more rb or wb between this timestamps'''

        timestamp = self.get_hi_lo_TimestampsID_Between(timeStamp_start,
                                                        timeStamp_end)

        if timestamp:
            timeStamp_end_id = timestamp[1]
            timeStamp_start_id = timestamp[0]

            self.c.execute(''' SELECT nid, rb, wb FROM samples_ost
                                WHERE timestamp_id BETWEEN %s AND %s
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
        print str(
                  str(informations[0][0]) + ' ' +
                  str(informations[0][1]) + ' ' +
                  str(informations[3][0]) + ' ' +
                  str(informations[3][1]))
        if informations[2][1] < 0:
            t_end = time.time()
            print str('Duration [running job]: ' +
               str(int(t_end - informations[1][1]) / 60) +
               'min')
        else:
            t_end = informations[2][1]
            print str('Duration: ' +
               str(int(t_end - informations[1][1]) / 60) +
               'min')

        number_of_nodes = len(informations[4][1].split(','))
        print 'Number of Nodes:', number_of_nodes
#------------------------------------------------------------------------------

    def print_Filesystem(self, window, fs='univ_1'):
        # used in main
        # getting all informations out of the database
        query = ('''
                select
                  timestamps.c_timestamp, sum(wb), sum(wio), sum(rb), sum(rio)
                from
                    ost_values,
                    targets,
                    filesystems,
                    timestamps
                where
                    ost_values.target = targets.id
                        and targets.fsid = filesystems.id
                        and ost_values.timestamp_id = timestamps.id
                        and filesystems.filesystem = %s
                        and c_timestamp between
                            unix_timestamp()-%s and unix_timestamp()
                group by timestamps.c_timestamp
                order by timestamps.c_timestamp''')
        values_np = self.query_to_npArray(query, (fs, int(window)))

        query = ''' select
                        c_timestamp
                    from
                        timestamps
                    where
                        c_timestamp
                            between
                                unix_timestamp()-%s and unix_timestamp()
                                '''
        allTimestamps = self.query_to_npArray(query, int(window))

        values_np = self.np_fillAndSort(values_np, allTimestamps)

        list_of_list = []
        list_of_list.append(values_np[:, 0])  # time
        list_of_list.append(values_np[:, 1] / (60 * 1000000))  # wb
        list_of_list.append(values_np[:, 0])  # time
        list_of_list.append(0 - (values_np[:, 3] / (60 * 1000000)))  # rb
        plotGraph(list_of_list, fs, 21)

        list_of_list = []
        list_of_list.append(values_np[:, 0])  # time
        list_of_list.append(0 - values_np[:, 4] / 60)  # rio per sec
        list_of_list.append(values_np[:, 0])  # time
        list_of_list.append(values_np[:, 2] / 60)  # wio per sec
        plotGraph(list_of_list, fs + 'io', 21)
        print 'done'
        exit()
#------------------------------------------------------------------------------

    def print_user(self, user):
        db.c.execute('''select count(*) form users; ''')
        userCounter = db.c.fetchone()
        print userCounter[0], ' users total'

        testUser = 'xhcmarku'  # 13 Jobs 38.54 Days Jobs runtime
        db.c.execute('''
                select
                    jobs.id
                from
                    jobs,
                    users
                where
                    jobs.owner = users.id
                and
                    users.username = %s;''', testUser)
        rows = db.c.fetchall()
        jobID_list = []
        for row in rows:
            jobID_list.append(row[0])

        user = User(testUser)
        testjob = Job(jobID_list[0])
        # (start, end, timeMapRB, timeMapWB, timeMapRIO, timeMapWIO, nidName)
        job_valuse = db.get_sum_nids_to_job(jobID_list[0])
        timeMapRB = job_valuse[2]
        timeMapWB = job_valuse[3]
        timeMapRIO = job_valuse[4]
        timeMapWIO = job_valuse[5]
        nidName = job_valuse[6]
        testjob.add_Values(timeMapRB, timeMapWB,
                            timeMapRIO, timeMapWIO, nidName)
        user.addJob(testjob)
#------------------------------------------------------------------------------

    def query_to_npArray(self, query, options=None):
        # used in print fs
        ''' execute the query with the given options and returns
            a numpy matrix of the output
        '''
        self.c.execute(query, options)

        # fetchall() returns a nested tuple (one tuple for each table row)
        results = self.c.fetchall()
        #print results

        # 'num_rows' needed to reshape the 1D NumPy array returend
        # by 'fromiter' in other words, to restore original dimensions
        # of the results set
        num_rows = int(self.c.rowcount)

        # recast this nested tuple to a python list and flatten it
        # so it's a proper iterable:
        x = map(list, list(results))              # change the type
        x = sum(x, [])                            # flatten

        # D is a 1D NumPy array
        D = np.fromiter(iter=x, dtype=np.float_, count=-1)

        # 'restore' the original dimensions of the result set:
        D = D.reshape(num_rows, -1)
        return D
#------------------------------------------------------------------------------

    def np_fillAndSort(self, values_np, allTimestamps_np):
        ''' fill the values_np with missing time stamps and sort the
            new array by the times tamps
        '''
        rt = np.array(values_np[:, 0])  # get the timestamps
        for t in allTimestamps_np:
            if not np.any(values_np[rt == t]):
                # if timestamp not in matix apend empty entry
                values_np = np.concatenate((values_np,
                                            np.array([[t[0], 0, 0, 0, 0]])))
        # a = the matrix a sortet by the first axis
        return values_np[values_np[:, 0].argsort()]


def print_job(job):
    db = readDB()
    db.c.execute('''
                    select * from jobs where jobid = %s
                    ''', (str(job),))

    check_job = db.c.fetchone()
    if not check_job:
        print 'No such job: ', job
        sys.exit(0)

    job = check_job[0]
    db.explainJob(job)
    jobObject = Job(job)

    start_end = db.get_job_start_end(job)

    if start_end:
        start = start_end[0] - 60
        endTime = start_end[1]
        if endTime < 0:
            end = time.time()
        else:
            end = endTime + 60

        query = '''
                select
                    timestamps.c_timestamp,
                    sum(samples_ost.wb),
                    sum(samples_ost.wio),
                    sum(samples_ost.rb),
                    sum(samples_ost.rio)
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
                    timestamps ON timestamps.id = samples_ost.timestamp_id
                        and timestamps.c_timestamp between %s and %s
                group by timestamps.c_timestamp'''  # by nids.nid ,

        values_np = db.query_to_npArray(query, (job, start, end,))

        query = ''' select
                        c_timestamp
                    from
                        timestamps
                    where
                        c_timestamp
                            between
                                %s and %s
                                '''
        allTimestamps = db.query_to_npArray(query, (start, end,))

        values_np = db.np_fillAndSort(values_np, allTimestamps)
        values_np = np.nan_to_num(values_np)
        print values_np
        db.c.execute('''select nid from nodelist where job = %s''', (job,))
        nids = db.c.fetchall()

        db.c.execute('''
                select jobs.jobid, users.username , jobs.nodelist
                from jobs, users
                where jobs.id = %s
                and users.id = jobs.owner''', (job,))
        job_info = db.c.fetchone()

        title = ('Job_' + str(job_info[0]) +
                 '_NoN_' + str(len(nids)) +
                 '__Owner_' + str(job_info[1]))
        jobObject.setTitle(title)
        print 'Plot: ', title

        timestamps = values_np[:, 0]
        wbs = values_np[:, 1] / (60 * 1000000)
        wio = ((values_np[:, 1] / 1000) / (values_np[:, 2])) / 60

        rbs = values_np[:, 3] / (60 * 1000000)
        rio = ((values_np[:, 3] / 1000) / (values_np[:, 4])) / 60

        plotJob(timestamps, rbs, rio, wbs, wio, title)
        print 'done'
        exit()

        list_of_list = []
        list_of_list.append(values_np[:, 0])  # time
        list_of_list.append(values_np[:, 1] / (60 * 1000000))  # wb
        list_of_list.append(values_np[:, 0])  # time
        list_of_list.append(0 - (values_np[:, 3] / (60 * 1000000)))  # rb
        plotGraph(list_of_list, title, 21)

        list_of_list = []
        list_of_list.append(values_np[:, 0])  # time
        list_of_list.append(((values_np[:, 3] / 1000) / (values_np[:, 4])) / 60)  # rio per sec
        list_of_list.append(values_np[:, 0])  # time
        list_of_list.append(((values_np[:, 1] / 1000) / (values_np[:, 2])) / 60)  # wio per sec
        plotGraph(list_of_list, title + 'io', 21)
        print 'done'
        exit()


if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    db = readDB()

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user",
                        help="print one specific user", type=str)
    parser.add_argument("-fs", "--filesystem",
                        help="print one file system", type=str)
    parser.add_argument("-j", "--job",
                        help="print one specific job", type=str)
    parser.add_argument("-w", "--window",
                        help='''specify a time window [in hours],
                                    default = 5 days''', type=str)
    args = parser.parse_args()
    if args.filesystem:
        print 'fs=', args.filesystem
        if not args.window:
            window = 432000
        else:
            window = int(args.window) * 3600  # hours to seconds
        db.print_Filesystem(window, args.filesystem)
        #exit()
    elif args.user:
        print 'user=', args.user
        print 'Not implemented yet'
        db.print_user(args.user)
        #exit()
    elif args.job:
        print 'job=', args.job
        print_job(args.job)
        #exit()
    else:
        parser.print_help()
        exit()

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in:", str(time_end - time_start), "sec"


def print_all_jobs_test():
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


def print_all_filesystems_test():
    print 'univ_1'
    db.print_Filesystem('univ_1')

    print 'univ_2'
    db.print_Filesystem('univ_2')

    print 'ind_1'
    db.print_Filesystem('ind_1')

    print 'ind_2'
    db.print_Filesystem('ind_2')

    print 'res_1'
    db.print_Filesystem('res_1')
