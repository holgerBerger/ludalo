'''
    this modul should handel all db extraktion and the posible prints.
    another task of this modul is to generate pngs of the fs and jobs.
'''

import numpy as np
import time
import multiprocessing


class DataCollection(object):

    """docstring for DataCollection"""

    def __init__(self, name):
        super(DataCollection, self).__init__()
        self.name = name
        self.values = np.zeros((0, 5))
        # 0 ts
        # 1 rb
        # 2 rio
        # 3 wb
        # 4 wio

    def append(self, ts, valueString):
        ''' ts as int values as sting eg 1, 2, 3, 4 or 1 2 3 4 '''
        ts = str(ts) + ','
        nString = ts + valueString
        a = np.matrix(nString)
        a = np.concatenate((self.values, a))
        self.values = a

    def getDuration(self):
        d = max(self.values[:, 0]) - min(self.values[:, 0])
        return d.item(0)

    def getAverage(self):
        a = np.average(self.values[:, 1])
        b = np.average(self.values[:, 2])
        c = np.average(self.values[:, 3])
        d = np.average(self.values[:, 4])
        return [a, b, c, d]

    def getStd(self):
        a = np.std(self.values[:, 1])
        b = np.std(self.values[:, 2])
        c = np.std(self.values[:, 3])
        d = np.std(self.values[:, 4])
        return [a, b, c, d]

    def getVar(self):
        a = np.var(self.values[:, 1])
        b = np.var(self.values[:, 2])
        c = np.var(self.values[:, 3])
        d = np.var(self.values[:, 4])
        return [a, b, c, d]

    def getMean(self):
        a = np.mean(self.values[:, 1])
        b = np.mean(self.values[:, 2])
        c = np.mean(self.values[:, 3])
        d = np.mean(self.values[:, 4])
        return [a, b, c, d]

    def getMedian(self):
        a = np.median(self.values[:, 1])
        b = np.median(self.values[:, 2])
        c = np.median(self.values[:, 3])
        d = np.median(self.values[:, 4])
        return [a, b, c, d]

    def getTotal(self):
        a = np.sum(self.values[:, 1])
        b = np.sum(self.values[:, 2])
        c = np.sum(self.values[:, 3])
        d = np.sum(self.values[:, 4])
        return [a, b, c, d]


class dbExtraktor(multiprocessing.Process):

    """docstring for dbExtraktor"""

    def __init__(self, dbcomm, queue):
        super(dbExtraktor, self).__init__()
        self.db = dbcomm
        self.queue = queue
        self.start()

    def run(self):

        while True:
            while self.queue.empty():
                time.sleep(0.1)
            if not self.queue.empty():
                (collection, tstart, tend) = self.queue.get()
                data = self.selectFromCollection(collection, tstart, tend)
                raise NotImplementedError
                data.saveStats()

    def selectFromCollection(self, collection, tstart, tend):
        dc = DataCollection(collection)
        raise NotImplementedError
        data = self.db.getStuff(collection, tstart, tend)
        for dataSet in data:
            dc.append(dataSet['ts'], dataSet['val'])
        return dc


if __name__ == '__main__':
    # test funktion!
    dc = DataCollection('blub')
    for x in xrange(0, 4):
        ts = int(time.time())
        ap = str(x * ts * 1) + ' ' + str(x * ts * 2) + \
            ' ' + str(x * ts * 3) + ' ' + str(x * ts * 4)
        dc.append(ts, ap)
        time.sleep(1)

    # print dc.values
    print dc.getAverage()
    print dc.getMean()
