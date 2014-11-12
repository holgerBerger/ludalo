'''
    this modul should handel all db extraktion and the posible prints.
    another task of this modul is to generate pngs of the fs and jobs.
'''

import numpy as np
import time
import math
import multiprocessing
import Analysis.plotGraph as graph


class DataCollection(object):

    """docstring for DataCollection"""

    def __init__(self, name):
        super(DataCollection, self).__init__()
        self.name = name
        self.values = np.zeros((0, 6))
        # 0 ts
        # 1 rb
        # 2 rio
        # 3 wb
        # 4 wio
        self.timeStampSet = set()

    def append(self, ts, valueString):
        ''' ts as int values as sting eg 1, 2, 3, 4 or 1 2 3 4 '''
        # build a array from the timestamp
        nString = [ts]
        nStringWithNoTime = np.array([0])

        # append the values to the timestamp to get [ts, v1, v2, v3, v4]
        for item in valueString:
            nString = np.append(nString, item)
            nStringWithNoTime = np.append(nStringWithNoTime, item)
            # nStringWithNoTime.append(item)

        # append zero mdt value
        nString = np.append(nString, 0)
        nStringWithNoTime = np.append(nStringWithNoTime, 0)

        if ts not in self.timeStampSet:
            # build a numpy matrix to get [[ts, v1, v2, v3, v4]]
            a = np.array([nString])
            # append the new matrix to the original matrix.
            # [[ts, v1, v2, v3, v4], [ts, v1, v2, v3, v4], ...]
            a = np.concatenate((self.values, a))
            self.values = a

        else:
            # get timestamps
            npts = np.array(self.values[:, 0])
            # get array with timestamp ts and add the values to it.
            foo = self.values[npts == ts] + nStringWithNoTime
            # overwrite the array at the timestamp pos with the new values.
            self.values[npts == ts] = foo

        # add timestamp dosen't mater, it is a set.
        self.timeStampSet.add(ts)

    def appendMDT(self, ts, value):
        appArray = np.array([0, 0, 0, 0, 0, value[0]])
        appArrayTs = np.array([ts, 0, 0, 0, 0, value[0]])

        if ts not in self.timeStampSet:
            # build a numpy matrix to get [[ts, v1, v2, v3, v4]]
            a = np.array([appArrayTs])
            # append the new matrix to the original matrix.
            # [[ts, v1, v2, v3, v4], [ts, v1, v2, v3, v4], ...]
            a = np.concatenate((self.values, a))
            self.values = a

        else:
            # get timestamps
            npts = np.array(self.values[:, 0])
            # get array with timestamp ts and add the values to it.
            foo = self.values[npts == ts] + appArray
            # overwrite the array at the timestamp pos with the new values.
            self.values[npts == ts] = foo

        # add timestamp dosen't mater, it is a set.
        self.timeStampSet.add(ts)

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

    def getQuartil(self):
        a = self.values[:, 1]
        b = self.values[:, 2]
        c = self.values[:, 3]
        d = self.values[:, 4]

        a25 = self.quantil25(a)
        a5 = self.median(a)
        a75 = self.quantil75(a)

        b25 = self.quantil25(b)
        b5 = self.median(b)
        b75 = self.quantil75(b)

        c25 = self.quantil25(c)
        c5 = self.median(c)
        c75 = self.quantil75(c)

        d25 = self.quantil25(d)
        d5 = self.median(d)
        d75 = self.quantil75(d)

        return [(a25, a5, a75), (b25, b5, b75), (c25, c5, c75), (d25, d5, d75)]

    def quantil25(self, npArray):
        return self.quantil(0.25, npArray)

    def median(self, npArray):
        return self.quantil(0.5, npArray)

    def quantil75(self, npArray):
        return self.quantil(0.75, npArray)

    def quantil(self, p, InArray):
        workingSet = InArray[:]
        workingSet = sorted(workingSet)
        n = len(workingSet)
        index = n * p
        if int(index) == index:
            a = workingSet[int(index) - 1]
            b = workingSet[int(index)]
            retValue = (a + b) / 2
        else:
            retValue = workingSet[int(math.ceil(index)) - 1]
        return retValue

    def calcAll(self):
        self.getTotal = self.getTotal()
        # median of np.matrix is broken #4301 29.10.2014
        # self.getMedian = self.getMedian()
        # >>> np.version.version '1.8.1'
        self.quartil = self.getQuartil()
        self.getMean = self.getMean()
        self.getVar = self.getVar()
        self.getStd = self.getStd()
        self.getAverage = self.getAverage()
        self.getDuration = self.getDuration()

    def get_png(self):

        timestamps = self.values[:, 0]
        rio = self.values[:, 1]
        rbs = self.values[:, 2]
        wio = self.values[:, 3]
        wbs = self.values[:, 4]
        mdt = self.values[:, 5]

        # some calculation to
        wbs_per_second = wbs / 10  # toDo grap from config
        wio_per_second = wio / 10
        #wbs_kb_per_s = wbs_per_second / 1024
        #wbs_mb_per_s = wbs_kb_per_s / 1024
        #wio_volume_in_kb = np.nan_to_num((wbs / wio) / 1024)

        rbs_per_second = rbs / 10  # toDo grap from config
        rio_per_second = rio / 10
        #rbs_kb_per_s = rbs_per_second / 1024
        #rbs_mb_per_s = rbs_kb_per_s / 1024
        #rio_volume_in_kb = np.nan_to_num((rbs / rio) / 1024)

        mdt_per_second = mdt / 10

        title = self.name
        self.realMax = graph.plotJob(timestamps, wbs_per_second, wio_per_second,
                                     rbs_per_second, rio_per_second, mdt_per_second, title, verbose=False)

        # print 'quantil:', self.quartil, 'mean:', self.getMean, 'Var:', self.getVar, 
        # '\nStd:', self.getStd, 'aver:', self.getAverage, 'druation:', self.getDuration

    def save(self, db):
        pass


class dbFsExtraktor(multiprocessing.Process):

    """docstring for dbExtraktor"""

    def __init__(self, cfg, queue):
        super(dbFsExtraktor, self).__init__()
        self.db = cfg.getNewDB_Mongo_Conn()
        self.queue = queue
        #self.pool = multiprocessing.Pool(processes=4)
        self.start()

    def extract(self, input):
        (collection, tstart, tend) = input
        # collect informations and build objects
        dc = self.selectFromCollection(collection, tstart, tend)

        # calculate stats
        dc.calcAll()
        # generate png
        dc.get_png()
        # save data to db
        dc.save(self.db)

    def selectFromCollection(self, collection, tstart, tend):
        dc = DataCollection(collection)
        data = self.db.getFsData(collection, tstart, tend)
        for key in sorted(data.keys()):
            # print key, data[key]['val']
            for item in data[key]:
                if len(item['val']) >= 4:
                    dc.append(key, item['val'])
                else:
                    dc.appendMDT(key, item['val'])

        return dc

    def run(self):

        # main loop fs-extractor
        loopcounter = 0
        while True:

            # wait for request
            while self.queue.empty():
                time.sleep(0.1)

            # double check for work
            if not self.queue.empty():
                # do stuff here
                # print 'get queue stuff'
                # self.queue.get() = (collection, tstart, tend)
                obj = self.queue.get()
                # calcLilst.append(obj)
                #resultObjects = self.pool.map(extract, calcLilst)
                self.extract(obj)
            loopcounter = loopcounter + 1
            print 'loop:', loopcounter
