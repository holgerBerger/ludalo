'''
Created on 19.02.2014

@author: uwe
'''

import matplotlib
import math
matplotlib.use('AGG')

import matplotlib.pyplot as plt
import matplotlib.dates as md
from matplotlib.ticker import FuncFormatter
from pylab import *
import datetime as dt
import numpy as np
import time
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


def plotJob(timestamps, wbs_per_second, wio_per_second, rbs_per_second, rio_per_second, title, verbose=False):
    nc_limegreen = '#CDDC39'  # googlecolores lime green 500
    nc_ligthtgreen = '#C5E1A5'  # googlecolores light green 200

    nc_blue = '#2196F3'  # googlecolores blue 500
    nc_lightblue = '#9FA8DA'  # googlecolores indigo 200

    # convert timestamps
    dates1 = [dt.datetime.fromtimestamp(int(ts)) for ts in timestamps]

    Wmbs = wbs_per_second / (1024 * 1024)
    Rmbs = rbs_per_second / (1024 * 1024)

    # calculate filter size
    fsize = int(math.sqrt(len(dates1)))
    if fsize < 3:
        fsize = 3

    # claculate filterd values
    mvaRB = MovingAverage(fsize)
    mvaWB = MovingAverage(fsize)
    for i in range(len(timestamps)):
        mvaWB.addValue(timestamps[i], Wmbs[i])
        mvaRB.addValue(timestamps[i], Rmbs[i])

    filterd_WB = mvaWB.getAverage()
    filterd_RB = mvaRB.getAverage()

    WB_Values = []
    for item in filterd_WB:
        WB_Values.append(item[1])

    RB_Values = []
    for item in filterd_RB:
        RB_Values.append(item[1])

    # Write
    fig = plt.figure(figsize=(16, 10))
    ax1 = fig.add_subplot(2, 3, 1)
    plt.xticks(rotation=45)
    plt.xlabel('Time')
    plt.ylabel('Speed [MB/s]')
    # second axis
    ax11 = twinx()
    ax11.yaxis.tick_right()
    plt.ylabel('IO/s')

    ax2 = fig.add_subplot(2, 3, 2)
    plt.xlabel('IO Size [KB]')
    plt.ylabel('IOs')
    # plt.gca().yaxis.set_major_formatter(formatter)

    ax3 = fig.add_subplot(2, 3, 3)
    plt.ylabel('Speed [MB/s]')
    plt.xlabel('IO Size [KB]')

    # Read
    ax4 = fig.add_subplot(2, 3, 4)
    plt.xticks(rotation=45)
    plt.xlabel('Time')
    plt.ylabel('Speed [MB/s]')
    # second axis
    ax41 = twinx()
    ax41.yaxis.tick_right()
    plt.ylabel('IO/s')

    ax5 = fig.add_subplot(2, 3, 5)
    plt.xlabel('IO Size [KB]')
    plt.ylabel('IOs')
    # plt.gca().yaxis.set_major_formatter(formatter)

    ax6 = fig.add_subplot(2, 3, 6)
    plt.ylabel('Speed [MB/s]')
    plt.xlabel('IO Size [KB]')

    # Speed

    ax11.plot(dates1, wio_per_second, label='IOs',
              lw=0.5, color=nc_ligthtgreen)  # IO
    ax1.plot(dates1, Wmbs, label='Exact Data', lw=1, color='gray')  # speed
    # filterd speed
    ax1.plot(dates1, WB_Values, label='Filtered Data',
             lw=2, color=nc_limegreen)
    ax1.set_title('Write MB and IO')
    ax1.legend(loc='best')

    ax41.plot(dates1, rio_per_second, label='IOs', lw=1, color=nc_lightblue)
    ax4.plot(dates1, Rmbs, label='Exact Data', lw=1, color='gray')
    ax4.plot(dates1, RB_Values, label='Filtered Data', lw=2, color=nc_blue)
    ax4.set_title('Read MB and IO')
    ax4.legend(loc='best')

    # ------ scatter plots --------

    kb_per_wio = np.nan_to_num((wbs_per_second / wio_per_second) / 1024)
    kb_per_rio = np.nan_to_num((rbs_per_second / rio_per_second) / 1024)

    if len(wio_per_second) > 1 and len(kb_per_wio) > 1:
        ax3.hexbin(wio_per_second, kb_per_wio, bins='log', mincnt=1)
        # ax3.scatter(wio, wbs, color='green', s=1)
        ax3.set_title('Scatter Plots Write')

    if len(rio_per_second) > 1 and len(kb_per_rio) > 1:
        ax6.hexbin(rio_per_second, kb_per_rio, bins='log', mincnt=1)
        #ax6.scatter(rio[rio > 0], rbs[rbs > 0], color='blue', s=1)
        ax6.set_title('Scatter Plots Read')

    # ------ Histograms --------
    bins1 = 30
    # avoid arrays with only one elemet. important!
    #plot_wio = np.append(wio[wio > 0], 1)
    #plot_wbs = np.append(wbs[wbs > 0], 1)

    #plot_rio = np.append(rio[rio > 0], 1)
    #plot_rbs = np.append(rbs[rbs > 0], 1)

    ax2.hist(wio_per_second, bins=bins1, normed=True, color=nc_limegreen)
    ax2.set_title('Histogram of Write IO Size')

    ax5.hist(rio_per_second, bins=bins1, normed=True, color=nc_blue)
    ax5.set_title('Histogram of Read IO Size')

    formatter = FuncFormatter(to_percent)

    plt.gca().yaxis.set_major_formatter(formatter)

    # show data plot
    plt.tight_layout()
    plt.savefig('test/' + str(title) + '.png', dpi=120)
    print 'plot to', str(title) + '.png'
    # plt.show()
    plt.close('all')
