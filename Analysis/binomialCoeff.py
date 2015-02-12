""" @brief      Calculates the binomial coefficient
    @link       http://en.wikipedia.org/wiki/Binomial_coefficient
    @author     Uwe Schilling uweschilling[at]hlrs.de
"""

import time


def binomialCoeff(n, k):
    """ @brief      Calculates the binomial coefficient for n over k
    """
    result = 1
    for i in range(1, k + 1):
        result = result * (n - i + 1) / i
    return result


def binArray(binStart):
    """ @brief      Calculates the binomial coefficient for a array
        @details    binStart    [binomialCoeff(binStart, 0),
                                 binomialCoeff(binStart, 1),
                                 binomialCoeff(binStart, 2),
                                 ...
                                 binomialCoeff(binStart, binStart),
                                ]
    """
    bioArray = []
    for i in range(0, binStart + 1):
        bioArray.append(binomialCoeff(binStart, i))
    return bioArray

if __name__ == '__main__':
    time_start = time.time()
#------------------------------------------------------------------------------
    print 'binomialCoeffs from 0 to 4 ' + str(binArray(4))
#------------------------------------------------------------------------------
    time_end = time.time()
    print "end with no errors in: " + str(time_end - time_start)
