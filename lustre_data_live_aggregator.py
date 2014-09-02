'''
    Autor uwe.schilling[at]hlrs.de 2014
    this programm is based on this thread:
    http://stefaanlippens.net/python-asynchronous-subprocess-pipe-reading
'''
import subprocess
import time
import threading
import Queue
import json
import sys
import MySQLdb
from pymongo import MongoClient


# import ConfigParser
# import MySQLdb

class PerformanceData(object):

    """
        this class holds the data and information about the Performance Data
        it will return the right data object for the different databases.
    """

    def __init__(self, timestamp, target, nid, values, fs, s_type, aggre=False):
        super(PerformanceData, self).__init__()

        self.timestamp = timestamp
        self.s_type = s_type
        self.target = target
        self.nid = nid
        self.values = values
        self.fs = fs
        self.isAggr = aggre

    def getMongo_Obj(self):
        obj = {"timestamps": self.timestamp,
               "s_type": self.s_type,
               "target": self.target,
               "nid": self.nid,
               "fs": self.fs,
               "values": self.values}
        return obj

    def getMySQL_Obj(self):
        pass


class MySQL_Conn(object):

    """docstring for MySQL_Conn"""

    def __init__(self, dbpassword, dbname, dbhost, dbuser):
        super(MySQL_Conn, self).__init__()

        # construct the connection and cursor
        self.conn = MySQLdb.connect(passwd=dbpassword,
                                    db=dbname,
                                    host=dbhost,
                                    user=dbuser)
        self.c = self.conn.cursor()


class Mongo_Conn(object):

    """docstring for Mongo_Conn"""

    def __init__(self, port='', ip='', db_name='testdb'):
        super(Mongo_Conn, self).__init__()

        # geting client and connect
        self.client = MongoClient()

        # getting db
        self.db = self.client[db_name]

        # getting collection
        self.collectionAggre = db['Aggre']
        self.collectionNotAggre = db['notAggre']
        self.collectionJobs = db['jobs']

    def insert_performanceAggre(self, obj):
        self.collectionAggre.insert(obj)

    def insert_performanceNotAggre(self, obj):
        self.collectionNotAggre.insert(obj)

    def insert_values(self, timestamps, target, nid, values, fs, aggre=False):
        obj = self.get_object(timestamps, target, nid, values, fs)

        if aggre:
            self.insert_performanceAggre(obj)
        else:
            self.collectionNotAggre(obj)


class DatabaseInserter(threading.Thread):

    '''
    This class handels the data form the collectors (inserterQueue)
    implemented as Thread to insert async and with as less as posibel
    dependencys to the collector.
    '''

    def __init__(self, queue, dbconf):
        threading.Thread.__init__(self)

        self.insertQueue = queue

        # Dry run !!!!

        # get config settings for the db
        # self.config = ConfigParser()
        # try:
        #     self.config.readfp(open(dbconf))
        # except IOError:
        #     print "no db.conf file found."
        #     sys.exit()
        # self.dbname = self.config.get("database", "name")
        # self.dbpassword = self.config.get("database", "password")
        # self.dbhost = self.config.get("database", "host")
        # self.dbuser = self.config.get("database", "user")

        # start the thread and begin to insert if entrys in the queue
        self.start()

    def _execute(self, query, data):
        pass

    def insert(self, jsonDict):
        '''
            split the json object into the informations for the
            database. the json object is defined as:
                {} json
                    {} fs-ost-/mdsname
                        [] aggr
                            [0] rio
                            [1] rb
                            [2] wio
                            [3] wb
                        [] nodeIP@connection
                            [0] rio
                            [1] rb
                            [2] wio
                            [3] wb
                [...]
        '''

        # insertTimestamp = jsonDict[0]
        data = jsonDict[1]
        insert_me = []

        for base in data.keys():
            sb = base.split('-')
            ost_map = data[base]
            fs_name = sb[0]
            name = sb[1]

            # switch for OST / MDS
            if 'OST' in name:
                for key in ost_map.keys():
                    resource_values = ost_map[key]
                    if key is not 'aggr':
                        sk = key.split('@')
                        resourceIP = sk[0]

                        rio = resource_values[0]
                        rb = resource_values[1]
                        wio = resource_values[2]
                        wb = resource_values[3]
                    else:
                        # handle aggr values...
                        pass
            elif 'MDT' in name:
                # handle MDS
                pass
            else:
                print 'weird things in the json... pleas check it.'
                print 'no MDS or MDT string is', name

    def run(self):
        while True:
            if not self.insertQueue.empty():
                insert = self.insertQueue.get()
                self.insert(insert)

    def close(self):
        '''
            to close the connectionen properly if the db thread has problems
        '''
        # self.conn.commit()
        # self.conn.close()
        pass  # dry run !!!


class AsynchronousFileReader(threading.Thread):

    '''
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''

    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        '''The body of the tread: read lines and decode json
        then put them on the queue.'''

        for line in iter(self._fd.readline, ''):
            # if not json print the exeption and the string
            try:
                self._queue.put(json.loads(line))
            except Exception, e:
                self._queue.put(line)
                print e
                # print 'in:', line

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()


class Collector(threading.Thread):

    '''
        This class is to manage connections over ssh and copy
        the real collector to the machines over scp
        this create 2 more Threads one
    '''

    def __init__(self, ip, insertQueue):
        threading.Thread.__init__(self)
        self.ip = ip
        self.command = ['ssh', '-C', self.ip, '/tmp/collector']
        self.out = sys.stdout
        self.insertQueue = insertQueue

        # Copy collector
        subprocess.call(['scp', 'collector', ip + ':/tmp/'])
        # Launch the command as subprocess.
        self.process = subprocess.Popen(
            self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Launch the asynchronous readers of the process' stdout and stderr.
        self.stdout_queue = Queue.Queue()
        self.stdout_reader = AsynchronousFileReader(
            self.process.stdout, self.stdout_queue)
        self.stdout_reader.start()
        self.stderr_queue = Queue.Queue()
        self.stderr_reader = AsynchronousFileReader(
            self.process.stderr, self.stderr_queue)
        self.stderr_reader.start()

        # Launch Tread
        self.start()
        print 'created', self.name

    def run(self):
        '''
        Consume standard output and standard error of
        a subprocess asynchronously without risk on deadlocking.
        '''

        try:
            self.name = self.stdout_queue.get(True, 10)
        except Exception, e:
            print 'got no correct name from ssh', e, self.name

        print 'started:', self.name

        # Check the queues if we received some output (until there is nothing more
        # to get).
        while not self.stdout_reader.eof() or not self.stderr_reader.eof():
            # Show what we received from standard output.
            while not self.stdout_queue.empty():
                line = self.stdout_queue.get()

                # Do Stuff!!!!
                self.insertQueue.put(line)

            # Show what we received from standard error.
            while not self.stderr_queue.empty():
                line = self.stderr_queue.get()
                print self.name + 'Received line on standard error: ' + repr(line)

            # Sleep a bit before asking the readers again.
            time.sleep(0.1)

        # Let's be tidy and join the threads we've started.
        self.stdout_reader.join()
        self.stderr_reader.join()

        # Close subprocess' file descriptors.
        self.process.stdout.close()
        self.process.stderr.close()

    def sendRequest(self):
        # getting data form collector
        self.process.stdin.write('\n')


if __name__ == '__main__':

    # read names and ip-adress
    cfg = open('collector.cfg', 'r')
    ips = json.load(cfg)
    dbconf = 'db.cfg'

    ts_delay = 10
    data_Queue = Queue.Queue()     # create dataqueue
    db_Queue = Queue.Queue()
    db = DatabaseInserter(db_Queue, dbconf)  # create DB connection

    sshObjects = []

    # for all ip's creat connections to the collector

    for key in ips.keys():
        sshObjects.append(Collector(ips[key], data_Queue))

    time.sleep(1)

    # loop over all connections look if they are alive
    while True:
        t_start = time.time()
        for t in sshObjects:
            if not t.isAlive():
                ip = t.ip
                # remove thread from list
                sshObjects.remove(t)
                # recover thread....
                print "recover", ip
                sshObjects.append(Collector(ip, data_Queue))
            else:
                t.sendRequest()
                # t.process.stdin.write('\n')

        # Database Timestamp !!!
        insertTimestamp = int(time.time())

        if not db.isAlive():
            # recover database connection
            print 'recover database'
            db.close()
            del(db)
            db = DatabaseInserter(db_Queue, dbconf)

            # put data form collectors into db queue
            print 'database Queue lenght:', db_Queue.qsize()
            while not data_Queue.empty():
                tmp = data_Queue.get()
                db_Queue.put((insertTimestamp, tmp))

        else:
            # put data form collectors into db queue
            print 'database Queue lenght:', db_Queue.qsize()
            while not data_Queue.empty():
                tmp = data_Queue.get()
                db_Queue.put((insertTimestamp, tmp))

        # look at db connectionen if this is alaive
        t_end = time.time()

        time.sleep(ts_delay - (t_end - t_start))
