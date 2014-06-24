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
    return sorted(filtered, key=lambda amps: amps[1], reverse=True)[:11]

if __name__ == '__main__':

    a = [1, 2, 1, 0, 1, 2, 1, 0]
    Fs = 60.0  # sampling rate

    print get_Spectrum(a, Fs)
