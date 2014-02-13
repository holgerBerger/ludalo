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
            timeIntervall_max = max(self.dfd.keys())
            timeIntervall_min = min(self.dfd.keys())
            timeIntervall = timeIntervall_max - timeIntervall_min
            if timeIntervall > self.size:
                tempSum = 0
                for key in self.dfd:
                    tempSum += self.dfd[key]
                self.average[timeIntervall_max] = tempSum / (timeIntervall+1)
                return self.average
            else: return None
        else:
            return None

if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    mav = MovingAverage(2)
    
    mav.addValue(0, 408)
    mav.addValue(1, 372)

    
    ad = mav.getAveragesDict()
    
    if ad:
        for key in mav.getAveragesDict():
            print str(key) + ' ' + str(ad[key])
    
    
    
    
#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
    