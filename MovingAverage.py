'''
Created on 13.02.2014

@author: uwe
'''

import time

class MovingAverage(object):
    def __init__(self, size=60):
        self.size = size
        self.dfd = {}
        self.average = {}

    def addValue(self, timestamp, value):
        
        if self.dfd.keys():
            timeIntervall_max = max(self.dfd.keys())
            timeIntervall_min = min(self.dfd.keys())
            timeIntervall = timeIntervall_max - timeIntervall_min
            
            if timeIntervall < self.size:
                self.dfd.setdefault(timestamp, 0)
                self.dfd[timestamp]+= value
            else:
                tempSum = 0
                for key in self.dfd:
                    tempSum += self.dfd[key]
                
                
                self.average[timeIntervall_max] = tempSum / (timeIntervall+1)
                del self.dfd[timeIntervall_min]
                self.dfd.setdefault(timestamp, 0)
                self.dfd[timestamp]+= value
        else:
            self.dfd.setdefault(timestamp, 0)
            self.dfd[timestamp]+= value

    def getAveragesDict(self):
        if self.dfd.keys():
            #print '1'
            timeIntervall_max = max(self.dfd.keys())
            timeIntervall_min = min(self.dfd.keys())
            timeIntervall = timeIntervall_max - timeIntervall_min
            if timeIntervall < self.size: return None
            else: 
                #print '2'
                tempSum = 0
                for key in self.dfd:
                    tempSum += self.dfd[key]
                self.average[timeIntervall_max] = tempSum / (timeIntervall+1)
                return self.average
            
        else: return None

if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    mav = MovingAverage(2)
    
    mav.addValue(0, 408)
    mav.addValue(1, 372)
    mav.addValue(2, 480)
    mav.addValue(3, 444)
    mav.addValue(4, 447)
    mav.addValue(5, 492)
    mav.addValue(6, 429)
    mav.addValue(7, 411)
    mav.addValue(8, 486)
    mav.addValue(9, 525)
    mav.addValue(10, 495)
    '''
    if output =
    2 420
    3 432
    4 457
    5 461
    6 456
    7 444
    8 442
    9 474
    10 502 -> TOP
    else -> retry
    '''
    ad = mav.getAveragesDict()
    for key in ad:
        print str(key) + ' ' + str(ad[key])
#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
    