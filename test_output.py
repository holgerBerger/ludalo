'''
Created on 12.02.2014

@author: uwe
This code require Python 2.2.1 or later

'''
import time
import datetime
import sqlite3
import matplotlib.pyplot as plt

from MovingAverage import MovingAverage


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
    def toDB(self):
        return (self.show_time, self.wbs, self.rbs) 



def ResultIter(cursor, arraysize=10000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


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
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS
            ost_oneHouer_stat (
            id integer primary key asc, 
            time integer,
            wbs integer,
            rbs integer)
    ''')
    
    output = c.execute('''
        SELECT * FROM timestamps ORDER BY time
         ''').fetchall()
    first_last_timestamp = 0
    one_houer = 3600*8
    intervalls = []
    rbsMovingAverage = MovingAverage(one_houer)
    wbsMovingAverage = MovingAverage(one_houer)
    rbSum = {}
    wbSum = {}
    
    for DBtimestamp in output:
        timestampID = DBtimestamp[0]
        timestamp = DBtimestamp[1]
        tmp = c.execute('''
                        SELECT rb, wb FROM samples_ost WHERE time = ?
                        ''', (timestampID,)).fetchall()
        for item in tmp:
            rbsMovingAverage.addValue(timestamp,item[0])
            wbsMovingAverage.addValue(timestamp,item[1])
            
            rbSum.setdefault(timestamp, 0)
            rbSum[timestamp]+= item[0]
            
            wbSum.setdefault(timestamp, 0)
            wbSum[timestamp]+= item[1]
            
                
    plotrbs = []
    plotrb = []
    rbs = rbsMovingAverage.getAveragesDict()
    wbs = wbsMovingAverage.getAveragesDict()
    for key in rbs:
        plotrbs.append(rbs[key])
    for key in output:
        plotrb.append(rbSum[key[1]]/60)
        
        
    #plt.plot(plotrbs)
    #plt.plot(plotrb)
    #plt.show()

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
    
#-----------------------------------------------------------------------------


    