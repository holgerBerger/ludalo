'''
Created on 13.02.2014

@author: uwe schilling


This is a class for simple and eficient calculation of the
moving average.  If you need the simple Average, set calcArray to [1, 1, 1]
for the average over 3 values.

'''

import time
import math
import binomialCoeff


class MovingAverage(object):
    def __init__(self, size=None, filterArry=None):
        if not size:
            if not filterArry:
                print 'pleas check the values for the MovingAverage init'
                exit()
            else:
                calcArray = filterArry
                size = len(filterArry)
        else:
            calcArray = binomialCoeff.binArray(size)

        self.size = size + 1
        self.calcArray = calcArray
        self.calcSum = sum(calcArray)
        self.valueList = [None] * self.size
        self.sizeHalf = int(math.ceil(size / 2))
        self.pointPos = 0
        self.pointMin = 0
        self.pointMax = 0
        self.pointMid = 0
        self.average = []

    def getAverage(self):
        self.calcLast()
        return self.average

    def getValue(self, valueArray):
        tmp = 0
        for i in range(0, self.size):
            tmp = tmp + (valueArray[i] * self.calcArray[i])
        tmp = tmp / self.calcSum
        #return int(round(tmp))
        return tmp

    def calculate(self):
        #calculate only if more then the half array is filled
        if self.valueList[self.sizeHalf]:
            #state 1 -> only half array is filed
            if not self.valueList[self.size - 1]:
                newValueArray = [None] * self.size
                valueSize = self.pointPos - self.pointMin
                newElements = self.size - valueSize

                # fill with the last value
                for i in range(0, newElements):
                    newValueArray[i] = self.valueList[self.pointMin % self.size][1]

                # fill with the given values
                tmp = self.pointMin
                for i in range(newElements, self.size):
                    newValueArray[i] = self.valueList[tmp][1]
                    tmp += 1

                self.average.append(
                    (self.valueList[self.pointMid][0], self.getValue(newValueArray)))

                self.pointMid = self.pointMid + 1
            #state 2 -> normal mode
            else:
                newValueArray = []
                for i in range(0, self.size):
                    key = (self.pointMin + i) % self.size
                    newValueArray.append(self.valueList[key][1])

                self.average.append(
                    (self.valueList[self.pointMid % self.size][0], self.getValue(newValueArray)))

                self.pointMid = self.pointMid + 1
                self.pointMin = self.pointMin + 1

    def calcLast(self):

        while self.pointMid <= self.pointMax:
            newValueArray = []
            # from min to max
            if (self.pointMax - self.pointMin) >= self.size:
                for i in range(self.pointMin, self.pointMax):
                    newValueArray.append(self.valueList[i % self.size][1])
                self.average.append(
                    (self.valueList[self.pointMid % self.size][0], self.getValue(newValueArray)))
                self.pointMid = self.pointMid + 1
                self.pointMin = self.pointMin + 1
            else:
                for i in range(self.pointMin, self.pointMax):
                    newValueArray.append(self.valueList[i % self.size][1])

                for k in range(len(newValueArray), self.size):
                    newValueArray.append(self.valueList[self.pointMax % self.size][1])

                self.average.append(
                    (self.valueList[self.pointMid % self.size][0], self.getValue(newValueArray)))

                self.pointMid = self.pointMid + 1
                self.pointMin = self.pointMin + 1

    def addValue(self, timestamp, value):
        '''
            This methode calculates the filter with the values that go in.
        '''

        # first Value
        if not self.valueList[0]:
            self.valueList[0] = (timestamp, value)
            self.pointPos += 1

        # after first Value
        else:
            insertPoint = self.pointPos % self.size
            self.valueList[insertPoint] = (timestamp, value)
            self.pointMax += 1
            self.pointPos += 1

        self.calculate()


if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    mav = MovingAverage(4)
    # [1, 4, 6, 4, 1]
    p1 = 408 * 1 + 408 * 4 + 408 * 6 + 372 * 4 + 480 * 1
    p1 = int(round(p1 / 16))

    p2 = 408 * 1 + 408 * 4 + 372 * 6 + 480 * 4 + 444 * 1
    p2 = int(round(p2 / 16))

    p3 = 408 * 1 + 372 * 4 + 480 * 6 + 444 * 4 + 447 * 1
    p3 = int(round(p3 / 16))

    p4 = 372 * 1 + 480 * 4 + 444 * 6 + 447 * 4 + 492 * 1
    p4 = int(round(p4 / 16))

    p5 = 480 * 1 + 444 * 4 + 447 * 6 + 492 * 4 + 429 * 1
    p5 = int(round(p5 / 16))

    p6 = 444 * 1 + 447 * 4 + 492 * 6 + 429 * 4 + 411 * 1
    p6 = int(round(p6 / 16))

    p7 = 447 * 1 + 492 * 4 + 429 * 6 + 411 * 4 + 486 * 1
    p7 = int(round(p7 / 16))

    p8 = 492 * 1 + 429 * 4 + 411 * 6 + 486 * 4 + 486 * 1
    p8 = int(round(p8 / 16))

    p9 = 429 * 1 + 411 * 4 + 486 * 6 + 486 * 4 + 486 * 1
    p9 = int(round(p9 / 16))

    test = [(0, p1), (1, p2), (2, p3),
            (3, p4), (4, p5), (5, p6),
            (6, p7), (7, p8), (8, p9)]
    #print test

    mav.addValue(0, 408)
    mav.addValue(1, 372)
    mav.addValue(2, 480)
    mav.addValue(3, 444)
    mav.addValue(4, 447)
    mav.addValue(5, 492)
    mav.addValue(6, 429)
    mav.addValue(7, 411)
    mav.addValue(8, 486)

    mav.calcLast()
    #print mav.average
    if test == mav.average:
        print 'Test pass!'

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

#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
