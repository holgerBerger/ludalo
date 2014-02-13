'''
Created on 12.02.2014

@author: uwe
This code require Python 2.2.1 or later

'''
import time,atexit,curses,sys
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

def eta(secs):
    if secs<60:
      return "%2.2d sec       "%secs
    else:
      return "%d min %2.2d sec" % (secs/60, secs%60)

def cleanup():
  print curses.tigetstr("cnorm")


if __name__ == '__main__':
    curses.setupterm()
    print curses.tigetstr("civis"),
    atexit.register(cleanup)

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
    
    starttime = time.time()
    size = len(output)
    counter=1
    for DBtimestamp in output:
        timestampID = DBtimestamp[0]
        timestamp = DBtimestamp[1]
        c.execute('''SELECT rb, wb FROM samples_ost WHERE time = ?''', (timestampID,))
        for item in ResultIter(c):
            rbsMovingAverage.addValue(timestamp,item[0])
            wbsMovingAverage.addValue(timestamp,item[1])
            
            rbSum.setdefault(timestamp, 0)
            rbSum[timestamp]+= item[0]
            
            wbSum.setdefault(timestamp, 0)
            wbSum[timestamp]+= item[1]
        # progressbar
        duration = (time.time() - starttime)
        fraction = (float(counter)/float(size))
        endtime = duration * (1.0/ fraction) - duration
        printString = str("\rextracted %9d timestamps [%s] ETA = %s"
                         %(counter,"|"*int(fraction*20.0)+"\\|/-"[counter%4]+"-"*(19-int(fraction*20.0)), eta(endtime)))
        print printString,
        sys.stdout.flush()
        counter+=1
        # progressbar end

                
    plotrbs = []
    plotrb = []
    rbs = rbsMovingAverage.getAveragesDict()
    wbs = wbsMovingAverage.getAveragesDict()
    for key in rbs:
        plotrbs.append(rbs[key])
    for key in output:
        plotrb.append(rbSum[key[1]]/60)
        
        
    plt.plot(rbs.keys(),plotrbs, rbSum.keys(), plotrb)
    #plt.plot(plotrb)
    plt.show()

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
    
#-----------------------------------------------------------------------------


    
