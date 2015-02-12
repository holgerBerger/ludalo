""" @brief          Collect all information for one job
    @deprecated     Not longer in use Created on 24.03.2014
    @author         uwe.schilling[at]hlrs.de
"""

import time
from fft_series import get_Spectrum
import numpy as np


class Job(object):

    '''
    classdocs
    '''

    def __init__(self, JobName):
        '''
        Constructor
        '''

        self.Name = JobName
        self.t_Start = -2
        self.t_End = -2

        self.WR = np.array()
        self.RD = np.array()
        self.WQ = np.array()
        self.RQ = np.array()

    def get_WR_sepctrum(self):
        return get_Spectrum(self.WR)

    def set_start(self, t_Start):
        self.t_Start = t_Start

    def set_end(self, t_end):
        self.t_End = t_end

    def getName(self):
        return self.Name

    def getEndTime(self):
        if not self.isEnded():
            t_End = time.time()
        else:
            t_End = self.t_End
        return t_End

    def getDuration(self):
        t_End = self.getEndTime()
        return t_End - self.t_Start

    def isEnded(self):
        if self.t_End < 0:
            return False
        else:
            return True

    def isValid(self):
        returnValue = False
        minMinutes = 15 * 60
        duration = self.getDuration()
        if duration > minMinutes:
            returnValue = True
        return returnValue
