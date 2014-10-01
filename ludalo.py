import time
import json
import lib.database as database
import lib.collector as collector


'''

this is the main function of the ludalo project.

'''


if __name__ == '__main__':

    # read names and ip-adress
    cfg = open('collector.cfg', 'r')
    ips = json.load(cfg)

    # getting db configs
    conf = 'db.conf'
    cfg = database.DatabaseConfigurator(conf)

    # TODO move in config!!!
    numberOfInserterPerDatabase = 3  # or more?
    sleepingTime = 10

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
        # sleep 60 seconds
        time.sleep(sleepingTime)  # TODO grap form config!
