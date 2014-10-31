'''
    this modul should handel all db extraktion and the posible prints.
    another task of this modul is to generate pngs of the fs and jobs.
'''

import numpy as np
import time
import multiprocessing
import Analysis.plotGraph as graph


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
        # build a array from the timestamp
        nString = [ts]

        # append the values to the timestamp to get [ts, v1, v2, v3, v4]
        for item in valueString:
            nString.append(item)

        # build a numpy matrix to get [[ts, v1, v2, v3, v4]]
        a = np.matrix(nString)

        # append the new matrix to the original matrix.
        # [[ts, v1, v2, v3, v4], [ts, v1, v2, v3, v4], ...]
        a = np.concatenate((self.values, a))

        # overwrite the old matrix
        self.values = a

    def getDuration(self):
        d = max(self.values[:, 0]) - min(self.values[:, 0])
        return d.item(0)

    def getAverage(self):
        a = np.average(self.values[:, 1].A1)
        b = np.average(self.values[:, 2].A1)
        c = np.average(self.values[:, 3].A1)
        d = np.average(self.values[:, 4].A1)
        return [a, b, c, d]

    def getStd(self):
        a = np.std(self.values[:, 1].A1)
        b = np.std(self.values[:, 2].A1)
        c = np.std(self.values[:, 3].A1)
        d = np.std(self.values[:, 4].A1)
        return [a, b, c, d]

    def getVar(self):
        a = np.var(self.values[:, 1].A1)
        b = np.var(self.values[:, 2].A1)
        c = np.var(self.values[:, 3].A1)
        d = np.var(self.values[:, 4].A1)
        return [a, b, c, d]

    def getMean(self):
        a = np.mean(self.values[:, 1].A1)
        b = np.mean(self.values[:, 2].A1)
        c = np.mean(self.values[:, 3].A1)
        d = np.mean(self.values[:, 4].A1)
        return [a, b, c, d]

    def getMedian(self):
        a = np.median(self.values[:, 1].A1)
        b = np.median(self.values[:, 2].A1)
        c = np.median(self.values[:, 3].A1)
        d = np.median(self.values[:, 4].A1)
        return [a, b, c, d]

    def getTotal(self):
        a = np.sum(self.values[:, 1].A1)
        b = np.sum(self.values[:, 2].A1)
        c = np.sum(self.values[:, 3].A1)
        d = np.sum(self.values[:, 4].A1)
        return [a, b, c, d]

    def calcAll(self):
        self.getTotal = self.getTotal()
        # median of np.matrix is broken #4301 29.10.2014
        # self.getMedian = self.getMedian()
        # >>> np.version.version '1.8.1'
        self.getMean = self.getMean()
        self.getVar = self.getVar()
        self.getStd = self.getStd()
        self.getAverage = self.getAverage()
        self.getDuration = self.getDuration()

    def get_png(self):
        timestamps = self.values[:, 0].A1
        # matrix to array [:,1] -> first axis .A1 as array
        rbs = self.values[:, 1].A1
        rio = self.values[:, 2].A1
        wbs = self.values[:, 3].A1
        wio = self.values[:, 4].A1
        title = self.name
        graph.plotJob(timestamps, rbs, rio, wbs, wio, title, verbose=False)

    def save(self):
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
            dc.append(key, data[key]['val'])
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
                print 'get queue stuff'
                # self.queue.get() = (collection, tstart, tend)
                obj = self.queue.get()
                # calcLilst.append(obj)
                #resultObjects = self.pool.map(extract, calcLilst)
                self.extract(obj)
            loopcounter = loopcounter + 1
            print 'loop:', loopcounter
