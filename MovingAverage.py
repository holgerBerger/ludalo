'''
Created on 13.02.2014

@author: uwe schilling


This is a class for simple and eficient calculation of the
moving average.  If you need the simple Average, set calcArray to [1, 1, 1]
for the average over 3 values. 

to do:

    remove min and max due the lace of performance problems
'''

import time
import math
import binomialCoeff

class MovingAverage(object):
    def __init__(self, size = None, filterArry = None):
        if not size:
            if not filterArry:
                print 'pleas check the values for the MovingAverage init'
                exit()
            else:
                calcArray = filterArry
                size = len(filterArry)
        else:
             calcArray = binomialCoeff.binArray(size)
                
        self.size = size
        self.calcArray = calcArray
        self.calcSum = sum(calcArray)
        self.valueList = [None]*size
        self.sizeHalf = math.ceil(size/2)
        self.pointPos = 0
        self.pointMin = 0
        self.pointMax = 0
        self.pointMid = 0
        #self.dfd = {}
        self.average = []
        
    def getValue(self, valueArray):
        tmp = 0
        for i in range(0, self.size-1):
            tmp = tmp + (valueArray[i] * self.calcArray[i])
        tmp = tmp / self.calcSum
        return int(round(tmp))

    def calculate(self):
        calc = False
        #calculate only if more then the half array is filled
        if self.valueList[self.sizeHalf]:
            #state 1 -> only half array is filed
            if not self.valueList[self.size-1]:
                newValueArray = []
                valueSize = self.pointPos - self.pointMin
                newElements = self.size - valueSize
                
                # fill with the last value
                for i in range(0, newElements):
                    newValueArray[i] = self.valueList[self.pointMin]
                
                # fill with the given values
                tmp = self.pointMin
                for i in range(newElements+1 , self.size-1):
                    newValueArray[i] = self.valueList[tmp][1]
                    tmp += 1
                
                self.average.append( 
                    (valueList[self.pointMid][0], self.getValue(newValueArray)))

                self.pointMid = self.pointMid + 1
                calc = True
            #state 2 -> normal mode
            else:
                newValueArray = []
                for i in range(0 , self.size -1):
                    key = (self.min + i) % self.size
                    newValueArray.append(self.valueList[key][1])
                
                self.average.append( 
                    (valueList[self.pointMid%self.size][0], self.getValue(newValueArray)))

                self.pointMid = self.pointMid + 1
                self.pointMin = self.pointMin + 1 
                calc = True
        return calc

    def calcLast(self):
        while self.pointMid <= self.pointMax:
            newValueArray = []
            # from min to max
            if (self.pointMax - self.pointMin) >= self.size:
                for i in range(self.pointMin, self.pointMax):
                    newValueArray.append(self.valueList[i%self.size][1])
                self.average.append( 
                    (valueList[self.pointMid%self.size][0], self.getValue(newValueArray)))
                self.pointMid = self.pointMid + 1
                self.pointMin = self.pointMin + 1
            else:
                for i in range(self.pointMin, self.pointMax):
                    newValueArray.append(self.valueList[i%self.size][1])

                for i in range(len(newValueArray)-1, self.size - len(newValueArray)):
                    newValueArray.append(self.valueList[self.pointMax%self.size][1])

                self.average.append( 
                    (valueList[self.pointMid%self.size][0], self.getValue(newValueArray)))

                self.pointMid = self.pointMid + 1
                self.pointMin = self.pointMin + 1

    def addValue(self, timestamp, value):
        '''
            This methode calculates the filter with the values that go in.
            
        '''
        # first Value
        if not valueList[0]:
            valueList[0] = (timestamp, value)
            self.pointPos += 1

        # after first Value
        else:
            insertPoint = self.pointPos%self.size
            valueList[insertPoint] = (timestamp, value)
            self.pointMax = insertPoint
            self.pointPos += 1
        
        self.calculate()


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
    