'''
Created on 18.02.2014

@author: uwe
'''

import time
import sqlite3

class readDB(object):
    
    def __init__(self, dbFile):
        '''
        Constructor
        '''
        self.dbFile = dbFile
        self.conn = sqlite3.connect(dbFile)
        self.c = self.conn.cursor()

#------------------------------------------------------------------------------
    def get_sum_nids_to_job(self, jobID):
        nids = self.get_nid_to_Job(jobID)
        start_end = self.get_job_star_end(jobID)
        start = start_end[0] 
        end = start_end[1]
        print jobID, start-end
        if start-end < 120:
            return False
        else:
            print self.getAll_Nid_IDs_Between(start, end)
        

    def get_job_star_end(self, jobID):
        start_end = self.c.execute(''' 
                        select start, end 
                        from jobs
                        where id = ? ''',(jobID,)).fetchall()
        if start_end:
            return start_end[0]
        else: return [0,0]
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
        
    def getAll_Nid_IDs_Between(self, timeStamp_start, timeStamp_end, threshold_b = 0):
        ''' get all nids between two timestamps  if thershold only nids with
            more rb or wb between this timestamps'''
        
        timestamp = self.c.execute(''' SELECT * FROM timestamps 
                            WHERE TIME BETWEEN ? AND ? 
                            order by time desc 
                            limit 1
                            ''', (timeStamp_start,timeStamp_end)).fetchone()
        print timestamp
        timeStamp_end_id = timestamp[0]
        
        timestamp = self.c.execute(''' SELECT * FROM timestamps 
                            WHERE TIME BETWEEN ? AND ? 
                            order by time 
                            limit 1
                            ''', (timeStamp_start,timeStamp_end)).fetchone()

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
        

if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    db = readDB('sqlite_new.db')
    for i in range(1,40):
        db.get_sum_nids_to_job(i)
    #print db.get_nid_to_Job(1)
    #l = db.get_All_Users_r_w(24*10, 100) # all user how have ritten more then 100mb in the last 10 days
    #for user in l:
    #    print user

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
