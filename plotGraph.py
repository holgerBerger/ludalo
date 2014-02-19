'''
Created on 19.02.2014

@author: uwe
'''
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as md
import numpy as np
import datetime as dt
import time
from MovingAverage import MovingAverage

class ArgMismatch(Exception):
     def __init__(self, value):
         self.value = value
     def __str__(self):
         return repr(self.value)


def plotGraph(list_of_list, diagramName = ''):
    ''' [[xValues],[yValues]] 
        Thist Plotes all given values and a his filtert line.
    '''
    if not len(list_of_list)%2 == 0:
        raise ArgMismatch('Please provide [[xValues],[yValues], ...] as arguments')
    
    time_start = time.time()
#------------------------------------------------------------------------------
    
    oneHouer = 60*60
    quater_day = oneHouer * 6
    half_a_day = oneHouer * 12
    oneDay = oneHouer*24
    oneWeek = oneDay*7
    oneMonth = oneWeek*4
    half_a_Year = oneMonth*6
    one_Year = half_a_Year*2
    tmin = 0

    now=time.mktime(time.localtime())
    
    for i in range(0,len(list_of_list),2):
        # ---- Calc filtered values ------
        mva = MovingAverage(21)
        for j in  range(0, len(list_of_list[i])):
            mva.addValue(list_of_list[i][j], list_of_list[i+1][j])
        
        filterd_values = mva.getAverage()
        fvTimes = []
        fvValues = []
        for item in filterd_values:
            fvTimes.append(item[0])
            fvValues.append(item[1])
        
        # ---- Append org values
        dates = [dt.datetime.fromtimestamp(ts) for ts in list_of_list[i]]
        datenums=md.date2num(dates)
        ax=plt.gca()
        xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        plt.plot(datenums,list_of_list[i+1],lw=0.2, color='gray')

        # ---- Append filtered values ------
        dates = [dt.datetime.fromtimestamp(ts) for ts in fvTimes]
        datenums=md.date2num(dates)
        tmin = min(datenums)
        ax=plt.gca()
        xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        plt.plot(datenums,fvValues,lw=2)

    plt.subplots_adjust(bottom=0.2)
    plt.xticks( rotation=90 )
    ax.grid(True)  
    plt.title(diagramName)
    plt.text(tmin, -300, r'Read')
    plt.text(tmin, +200, r'Write')
    plt.xlabel('Time')
    plt.ylabel('Speed [MB/s]')
    plt.show()
#------------------------------------------------------------------------------
    time_end = time.time()
    #print "end with no errors in: " + str(time_end - time_start)
    plt.show()
