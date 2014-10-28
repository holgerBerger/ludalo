import time
import json
import multiprocessing
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
            iteration = iteration + 1
            insertTimestamp = int(time.time())
            for pair in CollectorInserter:
                if not pair.inserter_is_alive():
                    # try new connection
                    print 'Main-Thread: try recover inserter'
                    pair.inserter_reconnect()

                if not pair.collector_is_alive():
                    # try new connection
                    print 'Main-Thread: try recover collectror', pair.collector
                    pair.collector_reconnect()

                # send signal to collect data
                pair.collect(insertTimestamp)
            print 'Main-Thread iteration:', iteration, 'sleep', sleepingTime, 'sec\n'
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

    # create extractors
    extractors = []
    queue = multiprocessing.Queue()
    for x in xrange(0, nooextract):
        extractors.append(extractor.dbExtraktor(cfg, queue))

    # main loop
    while True:
        print 'extractor queue length:', len(queue)
        joblist = []
        # check jobs
        # put jobs in queue
        for job in joblist:
            queue.put(job.name)

        fslist = []
        # check fs
        # put fs in queue
        for fs in fslist:
            queue.put(fs.name)
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
