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


def MainCollector():
    pass


def MainExtractor(cfg):
    # create extractors
    extractors = []
    queue = multiprocessing.Queue()
    for x in xrange(0, 3):
        extractors.append(extractor.dbExtraktor(cfg, queue))

    # main loop
    while True:

        pass


if __name__ == '__main__':

    conf = 'db.conf'
    if not sys.argv[1]:
        'take standart config:', conf
    else:
        conf = sys.argv[1]

    # read names and ip-adress
    cfg = open('collector.cfg', 'r')
    ips = json.load(cfg)

    # getting db configs

    cfg = database.DatabaseConfigurator(conf)

    numberOfInserterPerDatabase = cfg.numberOfInserterPerDatabase   # or more?
    sleepingTime = cfg.sleepingTime
    CollectorInserter = []

    # create collectoer and assert inserter
    for key in ips.keys():
        cip = collector.CollectorInserterPair(
            ips[key], cfg, numberOfInserterPerDatabase)
        CollectorInserter.append(cip)

    iteration = 0

    while True:
        iteration = +1
        insertTimestamp = int(time.time())
        for pair in CollectorInserter:
            if not pair.inserter_is_alive():
                # try new connection
                pair.inserter_reconnect()

            if not pair.collector_is_alive():
                # try new connection
                pair.collector_reconnect()

            # send signal to collect data
            pair.collect(insertTimestamp)
        print 'Main-Thread iteration:', iteration, 'sleep', sleepingTime, 'sec'
        # Global sleep!!!
        time.sleep(sleepingTime)
