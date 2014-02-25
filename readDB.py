'''
Created on 18.02.2014

@author: uwe
'''

import time
import sqlite3
from plotGraph import plotGraph
import sys

class readDB(object):
    
    def __init__(self, dbFile):
        '''
        Constructor
        '''
        self.dbFile = dbFile
        self.conn = sqlite3.connect(dbFile)
        self.c = self.conn.cursor()

#------------------------------------------------------------------------------
    def read_write_sum_to_Nid(self, start, end, nidName):
        tmp = self.c.execute(''' 
                        select samples_ost.rb, samples_ost.wb, timestamps.time, nids.nid 
                        from samples_ost, nids, timestamps 
                        where nids.id = samples_ost.nid 
                        and timestamps.id = samples_ost.time 
                        and timestamps.time between ? and ? 
                        and nids.nid = ?;''',(start, end, nidName)).fetchall()

        timeMapRB = {}
        timeMapWB = {}
        tmp_time = self.c.execute('''
                            select time from timestamps 
                            where time between ? and ?''',(start, end)).fetchall()
        
        for timeStamp in tmp_time:
            timeMapRB[timeStamp[0]] = 0
            timeMapWB[timeStamp[0]] = 0
        
        nidList = set()
        for item in tmp:
            read = item[0]
            write = item[1]
            timestamp = item[2]
            nid = item[3]
            if timestamp not in timeMapRB:
                timeMapRB[timestamp] = 0
                timeMapWB[timestamp] = 0
            timeMapRB[timestamp] += read
            timeMapWB[timestamp] += write
            nidList.add(nid)
                
        return (start, end, timeMapRB, timeMapWB, nidList)


#------------------------------------------------------------------------------
    def get_sum_nids_to_job(self, jobID):
        nids = self.get_nid_to_Job(jobID)
        start_end = self.get_job_star_end(jobID)
        if start_end:
            start = start_end[0] 
            end = start_end[1]
            if not (end-start < 180):
                #print 'find nids'
                colReturn = []
                for nid in nids:
                    colReturn.append(self.read_write_sum_to_Nid(start, end, nid))
                return colReturn
            else: return None
        else: return None
#------------------------------------------------------------------------------
    def get_job_star_end(self, jobID):
        start_end = self.c.execute(''' 
                        select start, end 
                        from jobs
                        where id = ? ''',(jobID,)).fetchall()
        samples_max = self.c.execute(''' 
                        select time from timestamps 
                        order by time desc 
                        limit 1 ''').fetchall()
        samples_min = self.c.execute(''' 
                        select time from timestamps 
                        order by time 
                        limit 1 ''').fetchall()
                        
        # out of sample range
        if not (start_end[0][0] < samples_min[0][0] or start_end[0][1] >  samples_max[0][0]):
            if start_end:
                return start_end[0]
            else: return None
        else: return None
        

#------------------------------------------------------------------------------
    def get_nid_to_Job(self, jobID):
        nids = self.c.execute('''
                        select nids.nid 
                        From jobs, nids, nodelist 
                        where nids.id = nodelist.nid 
                        and jobs.id = nodelist.job 
                        and jobs.id = ?;
                        ''', (jobID,)).fetchall()
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
        timestamp_start = timestamp_end - (houers*60*60)
        
        volume = rw_in_MB * 1000 * 1000
        
        p1 = db.getAll_Nid_IDs_Between(timestamp_start, timestamp_end, volume)
        userList = set()
        for p in p1:
            tmp = db.getAll_Jobs_to_Nid_ID_Between(timestamp_start, timestamp_end, p)
            if tmp:
                userList.add(db.get_User_To_Job(tmp[0][2])[0][0])
        return userList
#------------------------------------------------------------------------------

    def get_User_To_Job(self, jobID):
        user = self.c.execute(''' 
                       select users.username 
                       from users, jobs 
                       where users.id = jobs.owner 
                       and jobid = ? ''',(jobID,)).fetchall()
        return user
        
#------------------------------------------------------------------------------

    def getAll_Jobs_to_Nid_ID_Between(self, timeStamp_start, timeStamp_end, nidID):
        
        job_list = self.c.execute(''' 
            select owner, nodelist.nid, jobs.jobid, jobs.start, jobs.end 
            from jobs, nodelist 
            where nodelist.job = jobs.id 
            and nodelist.nid = ? 
            and jobs.end > ?
            and jobs.start < ?''',(nidID, timeStamp_start, timeStamp_end)).fetchall()
        return job_list
#------------------------------------------------------------------------------
    def get_hi_lo_TimestampsID_Between(self,timeStamp_start,timeStamp_end):
        t_end = self.c.execute(''' SELECT * FROM timestamps 
                            WHERE TIME BETWEEN ? AND ? 
                            order by time desc 
                            limit 1
                            ''', (timeStamp_start,timeStamp_end)).fetchone()

        t_start = self.c.execute(''' SELECT * FROM timestamps 
                            WHERE TIME BETWEEN ? AND ? 
                            order by time  
                            limit 1
                            ''', (timeStamp_start,timeStamp_end)).fetchone()
                            
        if not t_start and not t_end:
            return None
        else:
            return (t_start[0], t_end[0])
        
    def getAll_Nid_IDs_Between(self, timeStamp_start, timeStamp_end, threshold_b = 0):
        ''' get all nids between two timestamps  if thershold only nids with
            more rb or wb between this timestamps'''
        
        timestamp = self.get_hi_lo_TimestampsID_Between(timeStamp_start, timeStamp_end)
        
        if timestamp:
            timeStamp_end_id = timestamp[1]
            timeStamp_start_id = timestamp[0]
        
            c = self.c.execute(''' SELECT nid, rb, wb FROM samples_ost 
                                WHERE TIME BETWEEN ? AND ?
                                ''', (timeStamp_start_id,timeStamp_end_id)).fetchall()
    
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

    def getTimeStamp(year, month, day, houer, minute):
        ''' convert from year day month to time stamp '''
        dateTimeInput = datetime.datetime(year, month, day, houer, minute)
        timeStamp = time.mktime(dateTimeInput.timetuple())
        timeStamp = int(timeStamp)
        return timeStamp
#------------------------------------------------------------------------------

    def timeStampToDate(timeStamp):
        ''' converts form time stamp to year day month '''
        return datetime.datetime.fromtimestamp(
                                float(timeStamp)).strftime('%Y-%m-%d %H:%M:%S')
#------------------------------------------------------------------------------

    def print_job(self, job):
        
        check_job = self.c.execute(''' 
                        select * from jobs where id = ?
                         ''',(job,))
        
        if not check_job:
            print 'No such job: ', job
            sys.exit(0)
        
        sum = db.get_sum_nids_to_job(job)
        
        if sum:
            jobid = job

            job_info = db.c.execute('''
                    select jobs.jobid, users.username 
                    from jobs, users 
                    where jobs.id = ? 
                    and users.id = jobs.owner''', (jobid,)).fetchone()
    
            title = 'Job: ' + str(job_info[0]) + ' Owner: ' + str(job_info[1])
            List_of_lists = []
            
            for nid in sum:
                start = nid[0]
                end = nid[1]
                readDic = nid[2]
                writeDic = nid[3]
        
                readY = []
                readX = sorted(readDic.keys())
                for timeStamp in readX:
                    readY.append(-readDic[timeStamp]/60)
        
            
                writeY = []
                writeX = sorted(writeDic.keys())
                for timeStamp in writeX:
                    writeY.append(writeDic[timeStamp]/60)
        
                
                if readX and readY and writeY and writeX:
                    List_of_lists.append(readX)
                    List_of_lists.append(readY)
        
                    List_of_lists.append(writeX)
                    List_of_lists.append(writeY)
            print 'Plot: ', title
            plotGraph(List_of_lists,title)

if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    db = readDB('sqlite_new.db')
    jobs = db.c.execute('''select id from jobs''').fetchall()
    valid_jobs = []
    print '# of jobs: ' + str(len(jobs))
    
    
    for job in jobs:
        if db.get_job_star_end(job[0]):
            valid_jobs.append(job[0])
            
    print '# of valid jobs: ' + str(len(valid_jobs))
    
    sum = None
    jobid = None
    
    tmpTest = 0
    
    for job in valid_jobs[5:]:
        sum = db.get_sum_nids_to_job(job)
        
        if sum:
            jobid = job
            
            if tmpTest == 0:
                break
            tmpTest += 1
    print 'JobID: ', jobid
    job_info = db.c.execute('''
            select jobs.jobid, users.username 
            from jobs, users 
            where jobs.id = ? 
            and users.id = jobs.owner''', (jobid,)).fetchone()

    title = 'Job: ' + str(job_info[0]) + ' Owner: ' + str(job_info[1])
    List_of_lists = []
    
    for nid in sum:
        start = nid[0]
        end = nid[1]
        readDic = nid[2]
        writeDic = nid[3]

        readY = []
        readX = sorted(readDic.keys())
        for timeStamp in readX:
            readY.append(-readDic[timeStamp]/60)

    
        writeY = []
        writeX = sorted(writeDic.keys())
        for timeStamp in writeX:
            writeY.append(writeDic[timeStamp]/60)

        
        if readX and readY and writeY and writeX:
            List_of_lists.append(readX)
            List_of_lists.append(readY)

            List_of_lists.append(writeX)
            List_of_lists.append(writeY)

    plotGraph(List_of_lists,title)
    #print db.get_nid_to_Job(1)
    #l = db.get_All_Users_r_w(24*10, 100) # all user how have ritten more then 100mb in the last 10 days
    #for user in l:
    #    print userx

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
