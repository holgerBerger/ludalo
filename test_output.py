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
        self.show_time = 0
    def toString(self):
        print len (self.times)



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
    output = c.execute('''
        SELECT time FROM timestamps ORDER BY time
         ''').fetchall()
    first_last_timestamp = 0
    one_houer = 3600
    intervalls = []
    
    for timestamp in output:
        printMe = c.execute('''
            SELECT time FROM timestamps 
            WHERE time BETWEEN ? AND ?
            ORDER BY time
                 ''', (
                       first_last_timestamp+1, 
                       first_last_timestamp + one_houer -1))
        inter = Intervall()
        inter.times = printMe.fetchall()
        if len(inter.times)>=55:
            inter.show_time = max(inter.times)
            intervalls.append(inter)
        first_last_timestamp = timestamp['time']
    print len(intervalls)
    print intervalls[0].times[1][0]
#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
    
#-----------------------------------------------------------------------------


    