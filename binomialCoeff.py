'''
Created on 17.02.2014

@author: uwe schilling

For possible filter's use this and choose a length.

'''

import time


def binomialCoeff(n, k):
    result = 1
    for i in range(1, k + 1):
        result = result * (n - i + 1) / i
    return result


def binArray(binStart):
    bioArray = []
    for i in range(0, binStart + 1):
        bioArray.append(binomialCoeff(binStart, i))
    return bioArray

if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    print 'binomialCoeffs from 0 to 8 ' + str(binArray(8))
#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
