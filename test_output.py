'''
Created on 12.02.2014

@author: uwe
'''

import time
import datetime
import sqlite3

class Intervall:
    def __init__(self):
        self.times = set()
        self.wb = 0
        self.rb = 0
        self.wbs = 0
        self.rbs = 0
        self.show_time = 0
    def toString(self):
        return 'show_time = ' + str(self.show_time) + ' wbs / rbs ' + str(self.wbs) + ' / ' + str(self.rbs) 



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
    dbFile = 'sqlite_new.db'
    conn = sqlite3.connect(dbFile)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.arraysize = 1000
    output = c.execute('''
        SELECT time FROM timestamps ORDER BY time Limit 100
         ''').fetchall()
    first_last_timestamp = 0
    one_houer = 3600
    intervalls = []
    
    for timestamp in output:
        #id     time            id        time        source    nid    rio    rb    wio    wb
        #112    1390503693      75190     112         25        273    0      0     1      63
        
        
        executeSQL = c.execute('''
            SELECT * FROM timestamps 
            WHERE time BETWEEN ? AND ?
            ORDER BY time''', (
                       first_last_timestamp + 1 - one_houer, 
                       first_last_timestamp-1)).fetchall()

        #print str(first_last_timestamp + one_houer -1)
        #print len(executeSQL)
        if len(executeSQL)>=59:
            #print len(executeSQL)
            timer_start = time.time()
            executeSQL = c.execute('''
                SELECT * FROM samples_ost 
                JOIN
                 timestamps on timestamps.time
                 between ? and ? 
                 and samples_ost.time = timestamps.id ''', (
                       first_last_timestamp + 1 - one_houer, 
                       first_last_timestamp-1)) .fetchmany()
            print str(time.time() - timer_start)
            inter = Intervall()
            for row in executeSQL:
                inter.times.add(row[1])
                inter.rb = inter.rb + row[7]
                inter.wb = inter.wb + row[9]
            lmax = max(inter.times)
            lmin = min(inter.times)
            inter.show_time = lmax
            duration = lmax - lmin
            inter.rbs = inter.rb / duration
            inter.wbs = inter.wb / duration
            intervalls.append(inter)
        first_last_timestamp = timestamp['time']
    for inter in intervalls:
        print inter.toString()
#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
    
#-----------------------------------------------------------------------------


    