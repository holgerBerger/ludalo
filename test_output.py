'''
Created on 12.02.2014

@author: uwe
This code require Python 2.2.1 or later

'''
from __future__ import generators 
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
        DROP TABLE ost_oneHouer_stat
    ''')
    
    c.execute('''
        CREATE IF NOT EXISTS 
            ost_oneHouer_stat (
            id integer primary key asc, 
            time integer,
            wbs integer,
            rbs integer)
    ''')
    
    output = c.execute('''
        SELECT time FROM timestamps ORDER BY time limit 80
         ''').fetchall()
    first_last_timestamp = 0
    one_houer = 3600
    intervalls = []
    
    for timestamp in output:
#id    time    source    nid    rio    rb    wio    wb      id    time
#1     1       2         13     0      0     1      3120    1     1390497035        
        executeSQL = c.execute('''
            SELECT * FROM timestamps 
            WHERE time BETWEEN ? AND ?
            ORDER BY time''', (
                       first_last_timestamp + 1 - one_houer, 
                       first_last_timestamp-1)).fetchall()
        timer_start = time.time()
        if len(executeSQL)>=59:

            executeSQL = c.execute('''
                SELECT * FROM samples_ost 
                JOIN
                 timestamps on timestamps.time 
                 BETWEEN ? AND ?
                 and samples_ost.time = timestamps.id ''', (
                       first_last_timestamp + 1 - one_houer, 
                       first_last_timestamp-1))
            
            inter = Intervall()
            for row in ResultIter(executeSQL):
                inter.times.add(row[9])
                inter.rb = inter.rb + row[5]
                inter.wb = inter.wb + row[7]

            lmax = max(inter.times)
            lmin = min(inter.times)
            inter.show_time = lmax
            duration = lmax - lmin
            inter.rbs = inter.rb / duration
            inter.wbs = inter.wb / duration
            intervalls.append(inter)
        #print str(time.time() - timer_start)        
        first_last_timestamp = timestamp['time']
    dbInter = []
    for inter in intervalls:
        dbInter.append(inter.toDB())

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
    
#-----------------------------------------------------------------------------


    