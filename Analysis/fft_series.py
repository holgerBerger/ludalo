'''
Created on 07.03.2014

@author: Uwe Schilling

Modul to categorize data form jobs.

See:
http://stackoverflow.com/questions/3637350/how-to-write-stereo-wav-files-in-python
http://stackoverflow.com/questions/3694918/how-to-extract-frequency-associated-with-fft-values-in-python

    To Do:
        - select category or buckets for jobs
        - calaculate the ffts and group jobs
        - try to group simular jobs
        - analyse behaviar
        - find bad behaviar

'''
import numpy as np


def get_fft_coef(value_array):
    x = np.array(value_array)
    w = np.fft.fft(x)
    freqs = np.fft.fftfreq(len(x))

    print(freqs.min(), freqs.max())
    # (-0.5, 0.499975)

    # Find the peak in the coefficients
    frate = 1
    idx = np.argmax(np.abs(w) ** 2)
    freq = freqs[idx]
    freq_in_hertz = abs(freq * frate)
    print(freq_in_hertz)

if __name__ == '__main__':
    print get_fft_coef([1, 2, 1, 0, 1, 2, 1, 0])
