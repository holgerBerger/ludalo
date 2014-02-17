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



def ResultIter(cursor, arraysize=100):
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
        SELECT * FROM timestamps ORDER BY time limit 10000
         ''').fetchall()
    first_last_timestamp = 0
    one_houer = 3600*8
    intervalls = []
    rbSum = {}
    wbSum = {}
    
    starttime = time.time()
    size = len(output)
    counter=1
    for DBtimestamp in output:
        timestampID = DBtimestamp[0]
        timestamp = DBtimestamp[1]
        c.execute('''SELECT rb, wb FROM samples_ost WHERE time = ?''', (timestampID,))
        tmpSumRB = 0
        tmpSumWB = 0
        if timestamp not in rbSum: 
          rbSum[timestamp]=0
          wbSum[timestamp]=0
        while True:
          res = c.fetchmany(500)
          if not res: 
            break
          else:
            for item in res:
                #rbSum.setdefault(timestamp, 0)
                tmpSumRB += item[0]
                tmpSumWB += item[1]
                #rbSum[timestamp]+= item[0]
                #wbSum[timestamp]+= item[1]

        rbSum[timestamp]+= tmpSumRB
        wbSum[timestamp]+= tmpSumWB            
                
        duration = (time.time() - starttime)
        fraction = (float(counter)/float(size))
        endtime = duration * (1.0/ fraction) - duration
        printString = str("\rextracted %9d timestamps [%s] ETA = %s"
                         %(counter,"|"*int(fraction*20.0)+"\\|/-"[counter%4]+"-"*(19-int(fraction*20.0)), eta(endtime)))
        print printString,
        sys.stdout.flush()
        counter+=1
        # progressbar end

    rbsMovingAverage = MovingAverage(61)
    wbsMovingAverage = MovingAverage(61)

    for key in sorted(rbSum.keys()):
        rbsMovingAverage.addValue(key, rbSum[key])

    for key in sorted(wbSum.keys()):
        wbsMovingAverage.addValue(key, wbSum[key])

                
    plotrbs = []
    plotrbsTims = []
    plotrb = []
    rbs = rbsMovingAverage.getAverage()
    wbs = wbsMovingAverage.getAverage()
    print len(rbs)
    for item in rbs:
        plotrbsTims.append(item[0])
        plotrbs.append(item[1])
    for key in sorted(rbSum.keys()):
        plotrb.append(rbSum[key])
        

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
    
#-----------------------------------------------------------------------------
        
    plt.plot(sorted(rbSum.keys()), plotrb, lw=0.1)
    plt.plot(plotrbsTims,plotrbs, lw=3)
    
    plt.show()



    
