'''
Created on 19.02.2014

@author: uwe
'''

import matplotlib
import math
matplotlib.use('AGG')

import matplotlib.pyplot as plt
import matplotlib.dates as md
#from matplotlib.ticker import FuncFormatter
from pylab import *
import datetime as dt
import numpy as np
#import time
from MovingAverage import MovingAverage


class ArgMismatch(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def to_percent(y, position):
    s = str(100 * y)

    # The percent symbol needs escaping in latex
    if matplotlib.rcParams['text.usetex'] is True:
        return s + r'$\%$'
    else:
        return s + '%'


def plotGraph(list_of_list, diagramName='', mvaLength=21):
    ''' [[xValues],[yValues]]
        This Plots all given values and his filtered line.
    '''
    if not len(list_of_list) % 2 == 0:
        raise ArgMismatch(
            'Please provide [[xValues],[yValues], ...] as arguments')

    #time_start = time.time()
#------------------------------------------------------------------------------

    #oneHouer = 60 * 60
    #quater_day = oneHouer * 6
    #half_a_day = oneHouer * 12
    #oneDay = oneHouer * 24
    #oneWeek = oneDay * 7
    #oneMonth = oneWeek * 4
    #half_a_Year = oneMonth * 6
    #one_Year = half_a_Year * 2
    #tmin = 0

    #now = time.mktime(time.localtime())

    for i in range(0, len(list_of_list), 2):
        # ---- Calc filtered values ------
        list_length = len(list_of_list[i])
        if list_length < mvaLength:
            mva = MovingAverage(list_length)
        else:
            mva = MovingAverage(mvaLength)
        for j in range(0, len(list_of_list[i])):
            mva.addValue(list_of_list[i][j], list_of_list[i + 1][j])

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
        #tmin = min(datenums)
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
    #time_end = time.time()
    # print "end with no errors in: " + str(time_end - time_start)
    # plt.show()


def plotJob(timestamps, wbs_per_second, wio_per_second, rbs_per_second, rio_per_second, mdt, title, verbose=False):
    # nc_limegreen = '#8BC34A'  # googlecolores green 500
    nc_lightgreen = '#558B2F'  # googlecolores light green 200
    nc_green = '#2E7D32'

    nc_blue = '#1565C0'  # googlecolores blue 500
    nc_lightblue = '#42A5F5'  # googlecolores indigo 200

    nc_orange = '#EF6C00'

    formatter = FuncFormatter(to_percent)

    # convert timestamps
    # timestamps = timestamps.astype(int)
    dates1 = [dt.datetime.fromtimestamp(ts) for ts in timestamps]
    xfmt = md.DateFormatter('%H:%M')

    Wmbs = wbs_per_second / (1024 * 1024)
    Rmbs = rbs_per_second / (1024 * 1024)

    # calculate filter size
    fsize = int(math.sqrt(len(dates1)))
    if fsize < 3:
        fsize = 3

    # claculate filterd values
    mvaRB = MovingAverage(fsize)
    mvaWB = MovingAverage(fsize)
    mvaRIO = MovingAverage(fsize)
    mvaWIO = MovingAverage(fsize)

    for i in range(len(timestamps)):
        mvaWB.addValue(timestamps[i], Wmbs[i])
        mvaRB.addValue(timestamps[i], Rmbs[i])
        mvaRIO.addValue(timestamps[i], rio_per_second[i])
        mvaWIO.addValue(timestamps[i], wio_per_second[i])

    filterd_WB = mvaWB.getAverage()
    filterd_RB = mvaRB.getAverage()

    filterd_IW = mvaWIO.getAverage()
    filterd_IR = mvaRIO.getAverage()

    WB_Values = []
    for item in filterd_WB:
        WB_Values.append(item[1])

    RB_Values = []
    for item in filterd_RB:
        RB_Values.append(item[1])

    RIO_Values = []
    for item in filterd_IR:
        RIO_Values.append(item[1])

    WIO_Values = []
    for item in filterd_IW:
        WIO_Values.append(item[1])

    # Write
    fig = plt.figure(figsize=(16, 10))
    ax1 = fig.add_subplot(2, 3, 1)
    ax1.xaxis.set_major_formatter(xfmt)
    plt.xticks(rotation=90)
    plt.xlabel('Time')
    plt.ylabel('Speed [MB/s]')
    # second axis
    ax11 = twinx()
    ax11.yaxis.tick_right()
    plt.ylabel('IO/s')

    ax2 = fig.add_subplot(2, 3, 2)
    ax2.yaxis.set_major_formatter(formatter)
    plt.xlabel('IO Size [KB]')
    # plt.gca().yaxis.set_major_formatter(formatter)

    ax3 = fig.add_subplot(2, 3, 3)
    plt.ylabel('Speed [MB/s]')
    plt.xlabel('IO Size [KB]')

    # Read
    ax4 = fig.add_subplot(2, 3, 4)
    ax4.xaxis.set_major_formatter(xfmt)
    plt.xticks(rotation=90)
    plt.xlabel('Time')
    plt.ylabel('Speed [MB/s]')
    # second axis
    ax41 = twinx()
    ax41.yaxis.tick_right()
    plt.ylabel('IO/s')

    ax5 = fig.add_subplot(2, 3, 5)
    ax5.yaxis.set_major_formatter(formatter)
    plt.xlabel('IO Size [KB]')
    # plt.gca().yaxis.set_major_formatter(formatter)

    ax6 = fig.add_subplot(2, 3, 6)
    plt.ylabel('Speed [MB/s]')
    plt.xlabel('IO Size [KB]')

    # Speed

    ax11.plot(dates1, wio_per_second, label='IOs',
              lw=1, color='gray', alpha=0.7)  # IO

    ax11.plot(dates1, WIO_Values, label='IOs filterd',
              lw=2, color=nc_lightgreen, alpha=0.7)

    ax11.plot(dates1, mdt, label='MDT IO',
              lw=2, color=nc_orange, alpha=0.3)

    ax1.plot(dates1, Wmbs, label='Exact Data',
             lw=1, color='gray', alpha=0.7)  # speed
    # filterd speed
    ax1.plot(dates1, WB_Values, label='Filtered Data',
             lw=2, color=nc_green)
    ax1.set_title('Write MB and IO')
    ax1.legend(loc='upper left')
    ax11.legend(loc='upper right')

    ax41.plot(dates1, rio_per_second, label='IOs',
              lw=1, color='gray', alpha=0.7)

    ax41.plot(dates1, RIO_Values, label='IOs filterd',
              lw=2, color=nc_lightblue, alpha=0.7)

    ax41.plot(dates1, mdt, label='MDT IO',
              lw=2, color=nc_orange, alpha=0.3)

    ax4.plot(dates1, Rmbs, label='Exact Data', lw=1, color='gray', alpha=0.7)
    ax4.plot(dates1, RB_Values, label='Filtered Data', lw=2, color=nc_blue)
    ax4.set_title('Read MB and IO')
    ax4.legend(loc='upper left')
    ax41.legend(loc='upper right')

    # ------ scatter plots --------

    kb_per_wio = np.nan_to_num((wbs_per_second / wio_per_second) / 1024)
    kb_per_rio = np.nan_to_num((rbs_per_second / rio_per_second) / 1024)

    if len(wio_per_second) > 1 and len(kb_per_wio) > 1:
        ax3.hexbin(kb_per_wio, wio_per_second, bins='log', mincnt=1)
        # ax3.scatter(wio, wbs, color='green', s=1)
        ax3.set_title('Scatter Plots Write')

    if len(rio_per_second) > 1 and len(kb_per_rio) > 1:
        ax6.hexbin(kb_per_rio, rio_per_second, bins='log', mincnt=1)
        #ax6.scatter(rio[rio > 0], rbs[rbs > 0], color='blue', s=1)
        ax6.set_title('Scatter Plots Read')

    # ------ Histograms --------
    bins1 = 30
    # avoid arrays with only one elemet. important!
    #plot_wio = np.append(wio[wio > 0], 1)
    #plot_wbs = np.append(wbs[wbs > 0], 1)

    #plot_rio = np.append(rio[rio > 0], 1)
    #plot_rbs = np.append(rbs[rbs > 0], 1)

    weights = np.ones_like(kb_per_wio) / len(kb_per_wio)
    ax2.hist(kb_per_wio, bins=bins1, weights=weights,
             normed=False, color=nc_green)
    ax2.set_title('Histogram of Write IO Size')

    weights = np.ones_like(kb_per_rio) / len(kb_per_rio)
    ax5.hist(kb_per_rio, bins=bins1, weights=weights,
             normed=False, color=nc_blue)

    ax5.set_title('Histogram of Read IO Size')

    # show data plot
    plt.tight_layout()
    plt.savefig('test/' + str(title) + '.png', dpi=80)  # 120
    print 'plot to', str(title) + '.png'
    # plt.show()
    plt.close('all')
    return [np.max(RB_Values), np.max(RIO_Values), np.max(WIO_Values), np.max(WB_Values)]
