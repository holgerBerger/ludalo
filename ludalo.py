import time
import json
import multiprocessing
import datetime
import lib.database as database
import lib.collector as collector
import lib.extractor as extractor
import sys


'''

this is the main function of the ludalo project.

'''


def mainCollector(cfg):
    # setup
    collectInfo = open('collector.cfg', 'r')
    ips = json.load(collectInfo)

    numberOfInserterPerDatabase = cfg.numberOfInserterPerDatabase   # or more?
    sleepingTime = cfg.sleepTime
    CollectorInserter = []

    # create collectoer and assert inserter
    for key in ips.keys():
        cip = collector.CollectorInserterPair(
            ips[key], cfg, numberOfInserterPerDatabase)
        CollectorInserter.append(cip)

    iteration = 0

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

    except KeyboardInterrupt:
        print '^C received, shutting down the system'
        for cip in CollectorInserter:
            cip.shutdown()
            cip.collect(insertTimestamp)
        print 'Bye'


def mainExtractor(cfg):
    # get config
    extractorSleep = cfg.extractorSleep
    nooextract = cfg.numberOfExtractros
    db = cfg.getNewDB_Mongo_Conn()

    timerange = 1800
    maxTokens = nooextract
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
        print 'token job/fs:', jobToken, fsToken

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
                queue.put(('job', (job, t - timerange, t)))
                print 'consume a job token', job
                jobToken = jobToken - 1

        time.sleep(extractorSleep)


if __name__ == '__main__':

    conf = 'db.conf'
    if len(sys.argv) <= 1:
        'take standard config:', conf
    else:
        conf = sys.argv[1]

    # getting configs

    cfg = database.DatabaseConfigurator(conf)

    # if collector than this but think of extractor
    mainExtractor(cfg)
