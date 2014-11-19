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
    maxTokens = nooextract + 3

    # create extractors
    extractors = []
    queue = multiprocessing.Queue()
    for x in xrange(0, nooextract):
        extractors.append(extractor.dbFsExtraktor(cfg, queue))

    db.resetCalcState()

    # main loop
    while True:
        print 'extractor queue length:', queue.qsize()

        # commit tokens
        tokens = maxTokens - queue.qsize()

        fslist = ['lnec', 'nobnec', 'alnec']

        t = int(time.time())

        # consume tokens for fs
        for fs in fslist:
            if tokens > 0:
                queue.put(('fs', (fs, t - timerange, t)))
                # consume a token
                tokens = tokens - 1

        # consume tokens for jobs
        while tokens > 0:
            # get one job and apend it
            job = db.oneUncalcJob()
            # queue.put(('job', (fs, t - timerange, t)))
            print job

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
