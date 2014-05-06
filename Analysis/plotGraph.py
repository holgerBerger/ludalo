'''
Created on 19.02.2014

@author: uwe
'''

import matplotlib
matplotlib.use('AGG')

import matplotlib.pyplot as plt
import matplotlib.dates as md
import datetime as dt
import time
from MovingAverage import MovingAverage


class ArgMismatch(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def plotGraph(list_of_list, diagramName='', mvaLength=21):
    ''' [[xValues],[yValues]]
        This Plots all given values and his filtered line.
    '''
    if not len(list_of_list) % 2 == 0:
        raise ArgMismatch('Please provide [[xValues],[yValues], ...] as arguments')

    time_start = time.time()
#------------------------------------------------------------------------------

    oneHouer = 60 * 60
    quater_day = oneHouer * 6
    half_a_day = oneHouer * 12
    oneDay = oneHouer * 24
    oneWeek = oneDay * 7
    oneMonth = oneWeek * 4
    half_a_Year = oneMonth * 6
    one_Year = half_a_Year * 2
    tmin = 0

    now = time.mktime(time.localtime())

    for i in range(0, len(list_of_list), 2):
        # ---- Calc filtered values ------
        list_length = len(list_of_list[i])
        if list_length < mvaLength:
            mva = MovingAverage(list_length)
        else:
            mva = MovingAverage(mvaLength)
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
        datenums = md.date2num(dates)
        ax = plt.gca()
        xfmt = md.DateFormatter('%Y-%m-%d %H:%M')
        ax.xaxis.set_major_formatter(xfmt)
        plt.plot(datenums, list_of_list[i + 1], lw=1, color='gray')

        # ---- Append filtered values ------
        dates = [dt.datetime.fromtimestamp(ts) for ts in fvTimes]
        datenums = md.date2num(dates)
        tmin = min(datenums)
        ax = plt.gca()
        xfmt = md.DateFormatter('%Y-%m-%d %H:%M')
        ax.xaxis.set_major_formatter(xfmt)
        plt.plot(datenums, fvValues, lw=2)

    plt.subplots_adjust(bottom=0.2)
    plt.xticks(rotation=90)
    ax.grid(True)
    plt.title(diagramName)
    #plt.text(tmin, -300, r'Read')
    #plt.text(tmin, +200, r'Write')
    plt.xlabel('Time')
    plt.ylabel('Speed [MB/s]')
    plt.savefig('plt/' + str(diagramName) + '.png')
    plt.close('all')
#------------------------------------------------------------------------------
    time_end = time.time()
    #print "end with no errors in: " + str(time_end - time_start)
    #plt.show()


def plotJob(timestamps, rbs, rio, wbs, wio, title):

    # convert timestamps
    dates1 = [dt.datetime.fromtimestamp(ts) for ts in timestamps]

    # calculate filter size
    fsize = int(len(dates1) / 10)
    if fsize < 3:
        fsize = 3

    # claculate filterd values
    mvaRB = MovingAverage(fsize)
    mvaWB = MovingAverage(fsize)
    for i in range(len(timestamps)):
        mvaWB.addValue(timestamps[i], wbs[i])
        mvaRB.addValue(timestamps[i], rbs[i])

    filterd_WB = mvaWB.getAverage()
    filterd_RB = mvaRB.getAverage()

    WB_Values = []
    for item in filterd_WB:
        WB_Values.append(item[1])

    RB_Values = []
    for item in filterd_RB:
        RB_Values.append(item[1])

    # Write
    fig = plt.figure(figsize=(16, 9))
    ax1 = fig.add_subplot(2, 3, 1)
    plt.xticks(rotation=45)
    plt.xlabel('Time')
    plt.ylabel('Speed [MB/s]')
    ax2 = fig.add_subplot(2, 3, 2)
    ax3 = fig.add_subplot(2, 3, 3)

    # Read
    ax4 = fig.add_subplot(2, 3, 4)
    plt.xticks(rotation=45)
    plt.xlabel('Time')
    plt.ylabel('Speed [MB/s]')
    ax5 = fig.add_subplot(2, 3, 5)
    ax6 = fig.add_subplot(2, 3, 6)

    # Speed
    ax1.plot(dates1, wbs, label='exact data', lw=1, color='gray')
    ax1.plot(dates1, WB_Values, label='Filterd data', lw=2, color='green')
    ax1.set_title('Write MB')
    ax1.legend(loc='best')

    ax4.plot(dates1, rbs, label='exact data', lw=1, color='gray')
    ax4.plot(dates1, RB_Values, label='Filterd data', lw=2, color='blue')
    ax4.set_title('Read MB')
    ax4.legend(loc='best')

    # Histograms
    bins1 = 15

    ax2.hist(wio, bins=bins1, color='green')
    ax2.set_title('amount of write io size')

    ax5.hist(rio, bins=bins1, color='blue')
    ax5.set_title('amount of read io size')

    # scatter plots
    ax3.scatter(wbs * 10, wio * 10, color='green')
    ax3.set_title('scatter plots write')

    ax6.scatter(rbs * 10, rio * 10, color='blue')
    ax6.set_title('scatter plots read')

    # show data plot
    plt.tight_layout()
    plt.savefig(str(title) + '.png', dpi=120)
    plt.close('all')
    plt.show()
