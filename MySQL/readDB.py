#!/usr/bin/env python
'''
Created on 18.02.2014

@author: uwe
'''

import sys
import time
import datetime
import MySQLdb
from multiprocessing.pool import Pool
from ConfigParser import ConfigParser
import argparse
import numpy as np


# Owen imports
sys.path.append("/home/uwe/projects/ludalo/Analysis")

#from User import User
from Job import Job
from fft_series import get_fingerprint
from plotGraph import plotJob


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

        self.verbose = True

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

    def get_job_start_end(self, jobID):
        self.c.execute('''
                        select t_start, t_end
                        from jobs
                        where id = %s ''', (jobID,))
        start_end = self.c.fetchall()

        job_start = start_end[0]
        if start_end[1] < 0:
            job_end = time.time() - 60
        else:
            job_end = start_end[1]

        self.c.execute('''
                        select c_timestamp from timestamps
                        order by c_timestamp desc
                        limit 1 ''')
        samples_max = self.c.fetchall()

        self.c.execute('''
                        select c_timestamp from timestamps
                        order by c_timestamp
                        limit 1 ''')
        samples_min = self.c.fetchall()

        if start_end:
            # job begins befor samples
            if job_start < samples_min:
                return None
            # job ends after last sample
            elif job_end > samples_max:
                return None
            # job is in sample range
            else:
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

        timestamps = values_np[:, 0]
        wbs = values_np[:, 1]
        wbs_per_second = wbs / 60
        wbs_kb_per_s = wbs_per_second / 1024
        wbs_mb_per_s = wbs_kb_per_s / 1024
        wio = values_np[:, 2]
        wio_volume_in_kb = np.nan_to_num((wbs / wio) / 1024)

        rbs = values_np[:, 3]
        rbs_per_second = rbs / 60
        rbs_kb_per_s = rbs_per_second / 1024
        rbs_mb_per_s = rbs_kb_per_s / 1024
        rio = values_np[:, 4]
        rio_volume_in_kb = np.nan_to_num((rbs / rio) / 1024)

        path = '/var/www/ludalo-web/calc/' + str(fs)
        plotJob(timestamps,
                    rbs_mb_per_s, rio_volume_in_kb,
                    wbs_mb_per_s, wio_volume_in_kb,
                    path)

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
#------------------------------------------------------------------------------

    def getJobID(self, jobName):
        query = '''
        select
            id
        from
            jobs
        where
            jobid = %s'''

        self.c.execute(query, (str(jobName),))
        rows = self.c.fetchall()
        if rows:
            return rows[0][0]
        else:
            return None
#------------------------------------------------------------------------------

    def job_running(self, jobID):
        query = '''
        select
            t_end
        from
            jobs
        where
            id = %s '''
        self.c.execute(query, (jobID,))
        rows = self.c.fetchall()

        if rows[0][0] < 0:
            return True
        else:
            return False
#------------------------------------------------------------------------------

    def print_job(self, jobName):
        # test if job exist
        jobID = self.getJobID(jobName)
        if not jobID:
            print '404', jobName
            exit(1)

        # test if job running
        job_start_end = self.get_job_start_end(jobID)
        if not job_start_end(jobID):
            print 'Not enough samples for job', jobName
            exit(1)

        jobRunning = self.job_running(jobID)

        if jobRunning:
            job_end = int(time.time())
        else:
            job_end = job_start_end[1]

        job_start = job_start_end[0]

        if self.verbose:
            print 'job id ', jobID
            self.explainJob(jobID)
            print 'job Running? ', jobRunning
            print 'job_end ', job_end
            print 'job_start ', job_start

        # get job data
        option = (jobID, job_start, job_end)

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
                timestamps ON timestamps.id = samples_ost.timestamp_id,
                filesystems
            where
                timestamps.c_timestamp between %s and %s
            group by timestamps.c_timestamp
        '''

        values_np = self.query_to_npArray(query, option)

        query = ''' select
                        c_timestamp
                    from
                        timestamps
                    where
                        c_timestamp
                            between
                                %s and %s
                                '''
        allTimestamps = self.query_to_npArray(query, (job_start, job_end))
        values_np = self.np_fillAndSort(values_np, allTimestamps)

        # ignore division by zero
        # it is posibel that wbs has a value but wio is zero. in this case
        # replaced with 0 (np.nan_to_num)

        np.seterr(divide='ignore', invalid='ignore')

        # transform data
        timestamps = values_np[:, 0]
        duration = max(timestamps) - min(timestamps)  # in seconds
        duration = duration / 60 / 60  # in hours
        wbs = values_np[:, 1]
        wbs_per_second = wbs / 60
        wbs_kb_per_s = wbs_per_second / 1024
        wbs_mb_per_s = wbs_kb_per_s / 1024
        wio = values_np[:, 2]
        wio_volume_in_kb = np.nan_to_num((wbs / wio) / 1024)

        rbs = values_np[:, 3]
        rbs_per_second = rbs / 60
        rbs_kb_per_s = rbs_per_second / 1024
        rbs_mb_per_s = rbs_kb_per_s / 1024
        rio = values_np[:, 4]
        rio_volume_in_kb = np.nan_to_num((rbs / rio) / 1024)

        # print job data
        if jobRunning:
            path = '/var/www/ludalo-web/calc/jobs/' + str(jobName)
        else:
            path = '/var/www/ludalo-web/calc/jobs/' + str(jobName)

        print jobName, get_fingerprint(duration, wbs, rbs, rio, wio)

        if self.verbose:
            plotJob(timestamps,
                        rbs_mb_per_s, rio_volume_in_kb,
                        wbs_mb_per_s, wio_volume_in_kb,
                        path)
            print 'done'
            exit()
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
        #print num_rows

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
#------------------------------------------------------------------------------

    def print_all_jobs(self):
        ''' test the  classification of the jobs'''

        self.c.execute('''select id, jobid from jobs where t_end > 1''')
        jobs = self.c.fetchall()
        valid_jobs = []

        print '# of jobs: ' + str(len(jobs))
        self.conn.commit()

        for job in jobs:
            start_end = self.get_job_start_end(job[0])
            if start_end:
                valid_jobs.append(job[1])

        print '# of valid jobs: ' + str(len(valid_jobs))

        #pool = Pool()
        #pool.map(self.print_job, valid_jobs)
        self.conn.commit()
        for job in valid_jobs:
            self.print_job(job)

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
    parser.add_argument("-e", "--experimentel",
                        help='''test for new methodes, in this case fft
                                    ''', default=False, action='store_true',)
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
        db.print_job(args.job)
        #exit()
    elif args.experimentel:
        print 'Printing all Jobs'
        db.verbose = False
        db.print_all_jobs()
        #exit()
    else:
        parser.print_help()
        exit(1)

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in:", str(time_end - time_start), "sec"


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
