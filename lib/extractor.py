""" @package    extractor
    @brief      This modul extract data form the database
    @details    to do this it tackts fs or job infrormations and print
                the information into a png or write the static details back
                to the database.
    @author     Uwe Schilling uweschilling[at]hlrs.de
"""


import numpy as np
import time
import math
import multiprocessing
import Analysis.plotGraph as graph


class DataCollection(object):

    """ @brief      This is the data holding class
        @details    to store the data from the databas in a working dataset
                    self.values is a numpy array with:
                    [0 ts,  (Timestamp)
                     1 rb,  (read byte)
                     2 rio, (read operation)
                     3 wb,  (write byte)
                     4 wio, (write operation)
                     5 mdo  (metadata operation)]
                    the return arrays are always
                    [1, 2, 3, 4] for average and so on...
    """

    def __init__(self, name):
        """ @brief      calss init
            @param      name of fs or job
        """
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
        """ @brief      this appends a data set to the existing set
            @param      ts              the timestamp of the data set
            @param      valueString     data string with 4 values
        """

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
        """ @brief      Append meta data operations
            @param      ts      timestamp
            @param      value   amount of mdo in the interval
        """
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
        """ @brief      this calaulates the time between ther first timestamp and the last ts.
            @return     duration of the dataset
        """
        if len(self.values[:, 0]) > 1:
            d = max(self.values[:, 0]) - min(self.values[:, 0])
            return d.item(0)
        else:
            return 0

    def getAverage(self):
        """ @brief      calclulates the average values oven all values
            @return     average array of the dataset
        """
        a = np.average(self.values[:, 1])
        b = np.average(self.values[:, 2])
        c = np.average(self.values[:, 3])
        d = np.average(self.values[:, 4])
        return [a, b, c, d]

    def getStd(self):
        """ @brief      Compute the standard deviation 
            @return     array of the dataset
        """
        a = np.std(self.values[:, 1])
        b = np.std(self.values[:, 2])
        c = np.std(self.values[:, 3])
        d = np.std(self.values[:, 4])
        return [a, b, c, d]

    def getVar(self):
        """ @brief      Compute the variance
            @return     array of the dataset
        """
        a = np.var(self.values[:, 1])
        b = np.var(self.values[:, 2])
        c = np.var(self.values[:, 3])
        d = np.var(self.values[:, 4])
        return [a, b, c, d]

    def getMean(self):
        """ @brief      Compute the arithmetic mean
            @return     array of the dataset
        """
        a = np.mean(self.values[:, 1])
        b = np.mean(self.values[:, 2])
        c = np.mean(self.values[:, 3])
        d = np.mean(self.values[:, 4])
        return [a, b, c, d]

    def getMedian(self):
        """ @brief      Compute the median
            @return     array of the dataset
        """
        a = np.median(self.values[:, 1])
        b = np.median(self.values[:, 2])
        c = np.median(self.values[:, 3])
        d = np.median(self.values[:, 4])
        return [a, b, c, d]

    def getTotal(self):
        """ @brief      Compute the sum over the dataset
            @return     array of the dataset
        """
        a = np.sum(self.values[:, 1])
        b = np.sum(self.values[:, 2])
        c = np.sum(self.values[:, 3])
        d = np.sum(self.values[:, 4])
        return [a, b, c, d]

    def getQuartil(self):
        """ @brief      Compute the 3 quartils (25, 50, 75)
            @return     array of the dataset
        """
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
        """ @brief      Compute the median
            @details    due to a bug in numpy and sipy this is reimplemented
            @return     array of the dataset
        """
        # if inArray len = 0 return
        if len(InArray) < 1:
            print '  ', 'quantil get empty array!!'
            return 0
        elif len(InArray) == 1:
            print '  ', 'quantil get array with one element!!'
            return InArray[0]
        else:
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
        """ @brief      this calcs all and save it as class variables
            @details    self.quartil = self.getQuartil()
                        self.mean = self.getMean()
                        self.var = self.getVar()
                        self.std = self.getStd()
                        self.average = self.getAverage()
                        self.duration = self.getDuration()
        """
        self.total = self.getTotal()
        # median of np.matrix is broken #4301 29.10.2014
        # self.getMedian = self.getMedian()
        # >>> np.version.version '1.8.1'
        self.quartil = self.getQuartil()
        self.mean = self.getMean()
        self.var = self.getVar()
        self.std = self.getStd()
        self.average = self.getAverage()
        self.duration = self.getDuration()

    def get_png(self):
        """ @brief      print a png
            @details    
            @param
            @return
        """
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

    def saveJob(self, db):
        """ @brief      
            @details
            @param
            @return
        """
        jobID, fs = self.name.split('@')
        stats = (self.total, self.quartil, self.mean, self.var,
                 self.std, self.average, self.duration)
        if sum(self.total) > 0:
            db.saveJobStats(jobID, fs, stats)
            db.set_job_calcState(jobID, 1)
        else:
            db.set_job_calcState(jobID, 2)


class dbFsExtraktor(multiprocessing.Process):

    """docstring for dbExtraktor"""

    def __init__(self, cfg, queue, tokenQueue):
        super(dbFsExtraktor, self).__init__()
        self.db = cfg.getNewDB_Mongo_Conn()
        self.queue = queue
        self.tokenQueue = tokenQueue
        #self.pool = multiprocessing.Pool(processes=4)
        self.start()

    def extract(self, input):
        (collection, tstart, tend) = input
        # collect informations and build objects
        print '  ', self.name, 'extrac:', collection
        dc = self.selectFromCollection(collection, tstart, tend)

        if len(dc.values) > 1:
            # calculate stats
            #t = time.time()
            dc.calcAll()
            # print 'timeToBuild calculations', dc.name, time.time() - t,
            # generate png
            #t = time.time()
            dc.get_png()
            # print 'timeToBuild png', dc.name, time.time() - t,
            # save data to db
            dc.save(self.db)
        else:
            print '  ', collection, tstart, tend, 'is empty!!!'

        # free memory
        del dc

    def selectFromCollection(self, collection, tstart, tend):
        fdName = str(collection) + '_' + str(int((tend - tstart) / 60))
        dc = DataCollection(fdName)
        data = self.db.getFsData(collection, tstart, tend)
        for key in sorted(data.keys()):
            # print key, data[key]['val']
            for item in data[key]:
                if len(item['val']) >= 4:
                    dc.append(key, item['val'])
                else:
                    dc.appendMDT(key, item['val'])

        return dc

    def dreamer(self, jobID):
        # holger example (616145.intern2-2014)

        # getting job data
        print '  ', self.name, ' dreame jobID', jobID[0]
        (collections, tstart, tend, nids) = self.db.getJobData(jobID[0])

        # datacollection
        for collection in collections:
            dc = DataCollection(None)
            data = self.db.selectJobData(collection, tstart, tend, nids)

            for key in sorted(data.keys()):
                # print key, data[key]['val']
                for item in data[key]:
                    if len(item['val']) >= 4:
                        dc.append(key, item['val'])
                    else:
                        dc.appendMDT(key, item['val'])

            dc.name = str(jobID[0]) + '@' + str(collection)
            if len(dc.values) > 1:
                dc.calcAll()
                # dc.get_png()
                dc.saveJob(self.db)
            else:
                self.db.set_job_calcState(jobID[0], 2)
                print '  ', self.name, 'empty collection', collection

            # free memory
            del dc

    def run(self):

        # main loop fs-extractor
        loopcounter = 0
        # self.dreamer('616145.intern2-2014')
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
                if obj[0] == 'fs':
                    self.extract(obj[1])
                    self.tokenQueue.put('fs')
                elif obj[0] == 'job':
                    self.dreamer(obj[1])
                    self.tokenQueue.put('job')

            loopcounter = loopcounter + 1
            print '  ', self.name, 'loop:', loopcounter
