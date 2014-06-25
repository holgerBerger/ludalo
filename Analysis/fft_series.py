'''
Created on 07.03.2014

@author: Uwe Schilling

Modul to categorize data form jobs. FFT_spectrum is used to analyse data
strucktur. Try with more informations to categorize jobs.

    To Do:
        - select category or buckets for jobs
        - try to group simular jobs
        - analyse behaviar
        - find bad behaviar

'''
from scipy import fft, arange
import numpy as np


def get_Spectrum(y, Fs=60.0):

    """
    retrun list of tubles with
    [(freq_1, ampli_1), (freq_2, ampli_2), (freq_n, ampli_n)]
    """
    n = len(y)  # length of the signal
    k = arange(n)
    T = n / Fs
    frq = k / T  # two sides frequency range
    frq = frq[range(n / 2)]  # one side frequency range
    print 'Base frequency:', Fs / n
    Y = fft(y) / n  # fft computing and normalization
    Y = Y[range(n / 2)]
    # frq_amp_list = [(freq, ampli), (freq, ampli), (freq, ampli)]
    frq_amp_list = zip(frq, abs(Y))

    # only frequenz with amplitud larger then zero
    filtered = filter(lambda x: x[1] > 0, frq_amp_list)
    # return the 10 highest amplituden
    return sorted(filtered, key=lambda amps: amps[1], reverse=True)[:10]


def class_1(duration):
    ''' duration in hours '''
    if duration < 3:
        return 'a'
    elif duration < 24:
        return 'b'
    else:
        return 'c'


def class_2(rb, wb, fs=None, duration=None):
    ''' volumen in byte '''
    total = rb + wb

    total_mb = sum(total) / 1024 / 1024

    if total_mb < 20:
        return 'a'
    elif total_mb < 200:
        return 'b'
    elif total_mb < 2000:
        return 'c'
    elif total_mb < 20000:
        return 'd'
    elif total_mb < 200000:
        return 'e'
    elif total_mb < 2000000:
        return 'f'
    elif total_mb < 20000000:
        return 'g'
    else:
        return 'h'


def class_3(rio, wio, rbs, wbs):
    rio_volume_in_kb = np.nan_to_num((rbs / rio) / 1024)
    wio_volume_in_kb = np.nan_to_num((wbs / wio) / 1024)

    rio_aver = np.average(rio_volume_in_kb)
    wio_aver = np.average(wio_volume_in_kb)

    return_st = ''

    if wio_aver < 100:
        return_st = return_st + 'a'
    elif wio_aver < 300:
        return_st = return_st + 'b'
    elif wio_aver < 500:
        return_st = return_st + 'c'
    elif wio_aver < 800:
        return_st = return_st + 'd'
    else:
        return_st = return_st + 'e'

    if rio_aver < 100:
        return_st = return_st + 'a'
    elif rio_aver < 300:
        return_st = return_st + 'b'
    elif rio_aver < 500:
        return_st = return_st + 'c'
    elif rio_aver < 800:
        return_st = return_st + 'd'
    else:
        return_st = return_st + 'e'
    return return_st


def class_4(rio, wio, rb, wb):
    total_io = rio + wio
    total_b = rb + wb

    b_per_io_list = np.nan_to_num(total_b / total_io)

    spec = get_Spectrum(b_per_io_list)
    return_value = set()

    for feq, amp in spec:
        return_value.add(round(feq, 2))
    return str(return_value)[4:-1]  # Srip set() and ceep the rest


def get_fingerprint(duration, rio, wio, wbs, rbs, fs=None):
    a = class_1(duration)
    b = class_2(rbs, wbs, fs=None, duration=None)
    c = class_3(rio, wio, rbs, wbs)
    d = class_4(rio, wio, rbs, wbs)
    return a + b + c + d

if __name__ == '__main__':
    a1 = np.sort([3, 7, -8, 3, -4, -14, 5, 9, -1, 1, -4, 12])

    a = [1, 2, 1, 0, 1, 2, 1, 0]
    Fs = 60.0  # sampling rate

    print get_Spectrum(a, Fs)
