""" @package    ludalo
    @brief      This is the main function of the ludalo project.

    @details    The ludalo (Lustre data logger) project is a project
                at the HRLS to store lustre perfromance data in a databese
                and analyse the data set if needed.

                The software is highly paralliced and should handel all sorts
                of network failure. Therfor this main loop can recover database
                connections and ssh connections to improve the stability and
                the perfromance.

    @author      Uwe Schilling uweschilling[at]hlrs.de
    @author      Holger Berger (NEC)
"""
import time
import lib.database as database
import sys


def mainCollector(cfg):
    """
        @brief      Main funktion for collector.

        @details    This prepare the data set, needet to start
                    the main loop of the collector.
                    This funktion need a file ( \em collector.cfg )
                    to connect via ssh to the MDS or OST to collect the
                    performance data.

        Note the following example code for the \em collector.cfg
        @code
            <Hoste_name_1: Host_IP_1,
             Hoste_name_2: Host_IP_2,
             [...]
             Hoste_name_N: Host_IP_N>
        @endcode

        @param      cfg is a \em DatabaseConfigurator object.
    """

    # imports
    import lib.collector as collector
    import datetime
    import json
    from multiprocessing import Manager
    # https://pypi.python.org/pypi/pyshmht
    # import pyshmht
    # setup for shared object
    # sharedDict = "/dev/shm/ludalosharedDict"

    # sharedDictInit = pyshmht.Cacher(sharedDict, capacity=2048)

    # setup
    collectInfo = open('collector.cfg', 'r')
    ips = json.load(collectInfo)
    sharedDict = Manager().dict()

    numberOfInserterPerDatabase = cfg.numberOfInserterPerDatabase   # or more?
    sleepingTime = cfg.sleepTime
    CollectorInserter = []

    # create collectoer and assert inserter
    for key in ips.keys():
        cip = collector.CollectorInserterPair(
            ips[key], cfg, numberOfInserterPerDatabase, sharedDict)
        CollectorInserter.append(cip)

    iteration = 0

    # main loop of the main thread. Pleas note that there are other threads
    # atatched to this. the other thereads should recover if this is runing
    # properly.

    try:
        while True:
            print 'Main-Thread: Starting loop'
            iteration = iteration + 1
            insertTimestamp = int(time.time())
            print 'Main-Thread: testing if collector is alive'
            for pair in CollectorInserter:
                if not pair.inserter_is_alive():
                    # try new connection
                    print 'Main-Thread: try recover inserter'
                    pair.inserter_reconnect()

                if not pair.collector_is_alive():
                    # try new connection
                    print 'Main-Thread: try recover collectror', pair.collector
                    pair.collector_reconnect()

                print 'Main-Thread: send collect signal'
                # send signal to collect data
                pair.collect(insertTimestamp)

            dateString = datetime.datetime.fromtimestamp(insertTimestamp)
            dateString = dateString.strftime('%Y-%m-%d %H:%M:%S')

            print 'Main-Thread iteration:', iteration, 'sleep', sleepingTime, 'sec\n', dateString
            # Global sleep!!!
            time.sleep(sleepingTime)

    # this should allow the software to exit gracefully. it is not tested with
    # the thread recover system.

    except KeyboardInterrupt:
        print '^C received, shutting down the system'
        for cip in CollectorInserter:
            cip.shutdown()
            cip.collect(insertTimestamp)
        print 'Bye'


def testExtract(cfg):
    """
        @brief      Here is a the place for fast test of the database and
                    database modifications.

        @details    Test funktion for the database and
                    the development of this software.

        @param      cfg is a \em DatabaseConfigurator object.
    """

    db = cfg.getNewDB_Mongo_Conn()

    result = db.db['jobStats'].find()
    for item in result:
        if item['total'][3] <= 0 and item['total'][1] > 0:
            pass
        elif item['total'][1] / item['total'][3] > 1:
            job = db.db['jobs'].find_one({'jobid': item['jobid']})
            if job['end'] - job['start'] > 2880:
                print item['jobid'], item['total'][1], item['total'][3]
            else:
                print '   ', item['jobid'], item['total'][1], item['total'][3]


def mainExtractor(cfg):
    """
        @brief      Main funktion for database extraction.

        @details    This prepare the data set, needet to start
                    the main loop of the extractor. Du to hardware limitations
                    is a bucket token algorithm integrated. this should be
                    moved to make this funkiton shorter and increse the
                    simplicity.

        @param      cfg is a \em DatabaseConfigurator object.
    """

    # imports
    import lib.extractor as extractor
    import multiprocessing

    # get config
    extractorSleep = cfg.extractorSleep
    nooextract = cfg.numberOfExtractros
    db = cfg.getNewDB_Mongo_Conn()

    timerange = 1800
    maxTokens = nooextract + 2
    fslist = ['lnec', 'nobnec', 'alnec']

    # create extractors
    extractors = []
    queue = multiprocessing.Queue()
    tokenQueue = multiprocessing.Queue()
    for x in xrange(0, nooextract):
        extractors.append(extractor.dbFsExtraktor(cfg, queue, tokenQueue))

    db.resetCalcState()
    for x in xrange(0, len(fslist)):
        tokenQueue.put('fs')

    for y in xrange(0, maxTokens - len(fslist)):
        tokenQueue.put('job')

    jobToken = 0
    fsToken = 0
    fsPos = 0
    # main loop

    # twoHouer = 60*60*2
    twoDays = 60 * 60 * 24 * 2
    ts = int(time.time())

    while True:
        print 'extractor queue length:', queue.qsize()
        jobsLeft, jobsRun = db.getJobsLeft()
        print 'Jobs remaining:', jobsLeft, 'Running Jobs:', jobsRun

        # commit tokens
        while tokenQueue.qsize() > 0:
            rt = tokenQueue.get()

            # return a job token
            if rt == 'job':
                jobToken = jobToken + 1

            # return a fs token
            elif rt == 'fs':
                # return a job token (stolen)
                if fsToken >= len(fslist):
                    jobToken = jobToken + 1
                    print 'return job token', jobToken
                else:
                    fsToken = fsToken + 1
            else:
                print 'undef token:', rt
        print 'token job/fs:', jobToken, fsToken

        t = int(time.time())

        # consume tokens for fs
        for fs in fslist:
            if fsToken > 0:
                queue.put(('fs', (fs, t - timerange, t)))
                # consume a token
                # print 'consume a fsToken', fs
                fsToken = fsToken - 1

            elif jobToken > 0:
                queue.put(('fs', (fs, t - timerange, t)))
                # consume a token
                print 'steal job token for fs:', fs
                jobToken = jobToken - 1

        # consume tokens for jobs
        while jobToken > 0:
            if (ts + timerange) < t:
                fs = fslist[fsPos]
                print 'long fs png'
                queue.put(('fs', (fs, t - twoDays, t)))
                jobToken = jobToken - 1
                fsPos = fsPos + 1
                if fsPos >= len(fslist):
                    fsPos = 0
                    ts = int(time.time())

            else:
                # get one job and apend it
                job = db.oneUncalcJob()
                if job:
                    queue.put(('job', (job, t - timerange, t)))
                    print 'consume a job token', job
                    jobToken = jobToken - 1
                else:
                    print 'no jobs to calculat...'
                    break

        time.sleep(extractorSleep)


if __name__ == '__main__':
    """
        @brief      Main funktion of ludalo.

        @details    This prepare the data set, needet to start
                    the main loop of the extractor. Du to hardware limitations
                    is a bucket token algorithm integrated. this should be
                    moved to make this funkiton shorter and increse the
                    simplicity.

        @param      None but this need a working config file. (db.conf)
                    It will be generated by the \em DatabaseConfigurator object
                    and must be adapt to the lustre and database Environment.

    """

    conf = 'db.conf'
    if len(sys.argv) <= 1:
        'take standard config:', conf
    else:
        conf = sys.argv[1]

    # getting configs

    cfg = database.DatabaseConfigurator(conf)

    # if collector than this but think of extractor
    mainCollector(cfg)
