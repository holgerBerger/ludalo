#!/usr/bin/env python

'''
    Autor uwe.schilling[at]hlrs.de 2014
    this programm is based on this thread:
    http://stefaanlippens.net/python-asynchronous-subprocess-pipe-reading
'''
import subprocess
import multiprocessing
from ConfigParser import ConfigParser
import time
import threading
import Queue
import json
import sys
import MySQLdb
import sqlite3
from pymongo import MongoClient


# import ConfigParser
# import MySQLdb

class PerformanceData(object):

    """
        this class holds the data and information about the Performance Data
        it will return the right data object for the different databases.
    """

    def __init__(self, timestamp, target, nid, values, fs, s_type):
        super(PerformanceData, self).__init__()

        self.timestamp = timestamp
        self.s_type = s_type
        self.target = target
        self.nid = nid
        self.values = values
        self.fs = fs

    def getMongo_Obj(self):
        obj = {"ts": self.timestamp,
               "st": self.s_type,
               "tgt": self.target,
               "nid": self.nid,
               "val": self.values}
        return obj

    def getSQL_Obj(self):
        # this will crash :-(
        if len(self.values) < 3:
            newValues = []
            newValues[0] = self.values
            newValues[1] = 0
            newValues[2] = 0
            newValues[3] = 0
            self.values = newValues

        obj = (self.timestamp,
               self.s_type,
               self.target,
               self.nid,
               self.values[0],
               self.values[1],
               self.values[2],
               self.values[3])
        return obj


class MySQL_Conn(object):

    """docstring for MySQL_Conn"""

    def __init__(self, host, port, user, password, dbname):
        super(MySQL_Conn, self).__init__()

        self.lock = multiprocessing.Lock()
        # construct the connection and cursor
        self.conn = MySQLdb.connect(passwd=password,
                                    db=dbname,
                                    host=host,
                                    port=port,
                                    user=user)
        self.c = self.conn.cursor()
        # self.generateDatabase()
        print 'MySQL Connected'

    def insert_performance(self, ip, objlist):
        fslist = {}
        for obj in objlist:
            if obj.fs not in fslist:
                fslist[obj.fs] = []
            fslist[obj.fs].append(obj.getSQL_Obj())

        sum = 0
        t1 = time.time()
        for fs in fslist.keys():
            query = ''' INSERT INTO  ''' + str(fs) + ''' (
                                            c_timestamp,
                                            c_servertype,
                                            c_target,
                                            nid,
                                            rio,rb,
                                            wio,wb)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''

            # Prevent other threads form execute
            with self.lock:
                # self.generateDatabaseTable(fs)
                # self.c.executemany(query, fslist[fs])
                pass

            sum += len(fslist[fs])
        t2 = time.time()
        print " inserted %d documents into MySQL (%d inserts/sec)" % (sum, sum / (t2 - t1))

    def generateDatabaseTable(self, fs):
        performanceTable = ''' CREATE TABLE IF NOT EXISTS ''' + str(fs) + '''(
                                  id serial primary key,
                                  c_timestamp BIGINT UNSIGNED NOT NULL,
                                  c_servertype VARCHAR(16) NOT NULL,
                                  c_target VARCHAR(16) NOT NULL,
                                  nid VARCHAR(26) NOT NULL,
                                  rio INT UNSIGNED,
                                  rb INT UNSIGNED,
                                  wio INT UNSIGNED,
                                  wb INT UNSIGNED)'''
        self.c.execute(performanceTable)

    def closeConn(self):
        self.conn.commit()
        self.conn.close()


class SQLight_Conn(object):

    """docstring for SQLight_Conn"""

    def __init__(self, path):
        super(SQLight_Conn, self).__init__()

        self.lock = multiprocessing.Lock()

        # construct the connection and cursor
        self.conn = sqlite3.connect(path)
        self.c = self.conn.cursor()
        print 'sqlite3 Connected'
        # self.generateDatabase()

    def insert_performance(self, ip, objlist):
        fslist = {}
        for obj in objlist:
            if obj.fs not in fslist:
                fslist[obj.fs] = []
            fslist[obj.fs].append(obj.getSQL_Obj())

        sum = 0
        t1 = time.time()
        for fs in fslist.keys():

            query = ''' INSERT INTO  ''' + str(fs) + ''' (
                                            c_timestamp,
                                            c_servertype,
                                            c_target,
                                            nid,
                                            rio,rb,
                                            wio,wb)
                        VALUES (?,?,?,?,?,?,?,?)'''

            # Prevent other threads form execute
            with self.lock:
                # self.c.executemany(query, fslist[fs])
                # self.generateDatabaseTable(fs)
                pass

            sum += len(fslist[fs])
        t2 = time.time()
        print " inserted %d documents into MySQL (%d inserts/sec)" % (sum, sum / (t2 - t1))

    def generateDatabaseTable(self, fs):
        performanceTable = ''' CREATE TABLE IF NOT EXISTS ''' + str(fs) + '''(
                                  id serial primary key,
                                  c_timestamp BIGINT UNSIGNED NOT NULL,
                                  c_servertype VARCHAR(16) NOT NULL,
                                  c_target VARCHAR(16) NOT NULL,
                                  nid VARCHAR(26) NOT NULL,
                                  rio INT UNSIGNED,
                                  rb INT UNSIGNED,
                                  wio INT UNSIGNED,
                                  wb INT UNSIGNED)'''
        self.c.execute(performanceTable)

    def closeConn(self):
        self.conn.commit()
        self.conn.close()


class Mongo_Conn(object):

    """docstring for Mongo_Conn"""

    def __init__(self, host, port, dbname):
        super(Mongo_Conn, self).__init__()

        self.lock = multiprocessing.Lock()
        # geting client and connect
        self.client = MongoClient(host, port)

        # getting db
        self.db = self.client[dbname]

        # getting collection
        self.collection = self.db['performanceData']
        self.collectionJobs = self.db['jobs']

    def insert_performance(self, objlist):
        fslist = {}
        for obj in objlist:
            if obj.fs not in fslist:
                fslist[obj.fs] = []
            fslist[obj.fs].append(obj.getMongo_Obj())

        sum = 0
        t1 = time.time()
        for fs in fslist.keys():

            # Prevent other threads form execute
            with self.lock:
                self.db[fs].insert(fslist[fs])

            sum += len(fslist[fs])
        t2 = time.time()
        print " inserted %d documents into mongodb (%d inserts/sec)" % (sum, sum / (t2 - t1))

    def closeConn(self):
        self.client.close()


class DatabaseInserter(multiprocessing.Process):

    '''
    This class handels the data form the collectors (inserterQueue)
    implemented as Thread to insert async and with as less as posibel
    dependencys to the collector.
    '''

    def __init__(self, oIn, db):
        super(DatabaseInserter, self).__init__()
        self.oIn = oIn
        self.db = db

        # start the thread and begin to insert if entrys in the queue
        self.start()

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

        insertTimestamp = jsonDict[0]
        data = jsonDict[1]
        insert_me = []

        # Split the json into data obj
        for base in data.keys():
            sb = base.split('-')  # "alnec-OST0002"
            ost_map = data[base]
            fs_name = sb[0]  # alnec
            name = sb[1]  # OST0002

            # switch for OST / MDS
            if 'OST' in name:
                s_type = 'OST'
            elif 'MDT' in name:
                s_type = 'MDT'
            else:
                print 'weird things in the json... please check it.'
                print 'no MDS or MDT string is', name
                break

            for key in ost_map.keys():
                # key = "aggr" or "10.132.10.0@o2ib42"
                resource_values = ost_map[key]
                sk = key.split('@')
                resourceIP = sk[0]

                ins = PerformanceData(
                    insertTimestamp, name, resourceIP, resource_values, fs_name, s_type)
                insert_me.append(ins)

        # Insert data Obj
        self.db.insert_performance(insert_me)

    def run(self):
        while True:
            # Read pipe
            insertObject = self.oIn.recv()
            # Insert the object form pipe db
            self.insert(insertObject)

    def close(self):
        '''
            to close the connectionen properly if the db thread has problems
        '''
        self.db.closeConn()


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


class DummyCollector(multiprocessing.Process):

    """docstring for DummyCollector"""

    def __init__(self, ip, sOut, oIn, mds=1, ost=2, nid=10):
        super(DummyCollector, self).__init__()
        self.ip = ip
        self.sOut = sOut
        self.oIn = oIn
        self.mds = mds
        self.ost = ost
        self.nid = nid

        # Launch Tread
        self.start()
        print 'created DUMMY', self.name

    def run(self):
        '''
        Consume standard output and standard error of
        a subprocess asynchronously without risk on deadlocking.
        '''
        print 'started DUMMY:', self.name

        while True:

            ts = self.sOut.recv()
            jObject = self.sendRequest()
            self.oIn.send((ts, jObject))

    def sendRequest(self):
        return self.getDummyData()

    def getDummyData(self):

        data = {}

        mdsNames = []
        ostNames = []
        nidNames = []
        ostValues = [81680085, 81680085, 81680085, 81680085]

        # Pleas append mds to map !!!

        # mdsValues = 81680085

        for x in xrange(1, self.mds):
            mdsNames.append('dummyfs-MDS_' + '{0:06}'.format(x))  # DUMMY-1

        for x in xrange(1, self.ost):
            ostNames.append('dummyfs-OST_' + '{0:06}'.format(x))  # DUMMY-1

        for x in xrange(1, self.nid):
            # DUMMY-1
            nidNames.append('Nid_DUMMY-' + '{0:06}'.format(x) + '@alpha')

        for ost in ostNames:
            tmp = {}
            for nid in nidNames:
                tmp[str(nid)] = ostValues
            data[str(ost)] = tmp

        # self.insertQueue.put(json.dumps(data))
        return data


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
                self.insertQueue.put((self.ip, line))

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


class DatabaseConfigurator(object):

    """this class handles the configparser"""

    def __init__(self, cfgFile):
        super(DatabaseConfigurator, self).__init__()

        self.cfgFile = cfgFile
        self.defaultCfgFile = 'default.cfg'
        self.cfg = ConfigParser()

        self.databases = {}

        try:
            self.cfg.readfp(open(self.cfgFile))
        except IOError:
            print "no %s file found." % self.cfgFile
            print 'Generate default config see %s' % self.defaultCfgFile
            print 'pleas configurate this file and rename it'
            self.writeDefaultConfig(self.defaultCfgFile)
            sys.exit()

        self.sectionMongo = 'MongoDB'
        self.sectionMySQL = 'MySQL'
        self.sectionSQLight = 'SQLight'

        if self.cfg.has_section(self.sectionMongo):
            if self.cfg.getboolean(self.sectionMongo, 'aktiv'):
                # host, port, dbname
                host = self.cfg.get(self.sectionMongo, 'host')
                port = self.cfg.getint(self.sectionMongo, 'port')
                dbname = self.cfg.get(self.sectionMongo, 'dbname')
                # do stuff
                self.databases[self.sectionMongo] = Mongo_Conn(
                    host, port, dbname)

        if self.cfg.has_section(self.sectionMySQL):
            if self.cfg.getboolean(self.sectionMySQL, 'aktiv'):
                # host, port, user, password, dbname
                host = self.cfg.get(self.sectionMySQL, 'host')
                port = self.cfg.get(self.sectionMySQL, 'port')
                user = self.cfg.get(self.sectionMySQL, 'user')
                password = self.cfg.get(self.sectionMySQL, 'password')
                dbname = self.cfg.get(self.sectionMySQL, 'dbname')
                # do stuff
                self.databases[self.sectionMySQL] = MySQL_Conn(
                    host, port, user, password, dbname)

        if self.cfg.has_section(self.sectionSQLight):
            if self.cfg.getboolean(self.sectionSQLight, 'aktiv'):
                # path
                path = self.cfg.get(self.sectionSQLight, 'path')
                # do stuff
                self.databases[self.sectionSQLight] = SQLight_Conn(path)

    def writeDefaultConfig(self, defaultCfgFile):
        cfgString = ('[MongoDB]' + '\n' +
                     'aktiv = [1/yes/true/on | 0/no/false/off]' + '\n' +
                     'host = [IP]' + '\n' +
                     'port = 3333' + '\n' +
                     'dbname = [databaseName]' + '\n' + '\n' +

                     '[MySQL]' + '\n' +
                     'aktiv = [1/yes/true/on | 0/no/false/off]' + '\n' +
                     'host = [IP]' + '\n' +
                     'port = 3333' + '\n' +
                     'user = [userName]' + '\n' +
                     'password = [userPasswort]' + '\n' +
                     'dbname = [databaseName]' + '\n' + '\n' +

                     '[SQLight]' + '\n' +
                     'aktiv = [1/yes/true/on | 0/no/false/off]' + '\n' +
                     'path = [pathToDatabase]' + '\n' + '\n' +

                     '[hostfile]' + '\n' +
                     'hosts = [pathToTheHostFile]' + '\n' + '\n' +

                     '[replacePattern]' + '\n' +
                     'pattern = (.*)(-ib)' + '\n' +
                     'replace = \1' + '\n' + '\n' +

                     '[batchsystem]' + '\n' +
                     'postfix = [.name]' + '\n' +
                     'usermapping = [file]' + '\n' + '\n')

        f = open(defaultCfgFile, 'w')
        f.write(cfgString)


if __name__ == '__main__':

    # read names and ip-adress
    cfg = open('collector.cfg', 'r')
    ips = json.load(cfg)

    # getting db configs
    conf = 'db.conf'
    cfg = DatabaseConfigurator(conf)

    # global delay
    ts_delay = 10
    test_dummy_insert = True
    numberOfInserterPerDatabase = 3  # or more?

    # Mongo
    try:
        # connection to mongo db
        dbMongo_conn = cfg.databases[cfg.sectionMongo]
        mongoInserter = {}
        for x in xrange(0, numberOfInserterPerDatabase):
            (pin, pout) = multiprocessing.Pipe()
            db = DatabaseInserter(pout, dbMongo_conn)
            mongoInserter[x] = (db, pin)
        dbMongoactive = True
    except:
        dbMongoactive = False

    # MySQL
    try:
        dbMySQL_conn = cfg.databases[cfg.sectionMySQL]
        dbMySQL_Queue = Queue.Queue()
        db_mySQL = DatabaseInserter(dbMySQL_Queue, dbMySQL_conn)
        dbMySQLactive = True
    except:
        dbMySQLactive = False

    # SQLight
    try:
        dbSQLight_conn = cfg.databases[cfg.sectionSQLight]
        dbSQLight_Queue = Queue.Queue()
        db_SQLight = DatabaseInserter(dbSQLight_Queue, dbSQLight_conn)
        dbSQLightactive = True
    except:
        dbSQLightactive = False

    if not dbMongoactive and not dbMySQLactive and not dbSQLightactive:
        print 'No Database configered. Pleas look at your config.'

    # create DB inserter

    sshObjects = []

    signalPipe = []

    objectPipe = []

    opjectQueue = Queue.Queue()

    # Setup signal pipes

    # for all ip's creat connections to the collector
    if test_dummy_insert:

        (sIn, sOut) = multiprocessing.Pipe()  # pipe for signals
        (oIn, oOut) = multiprocessing.Pipe()  # pipe for objects
        sshObjects.append(
            DummyCollector('blub', sOut, oIn, mds=1, ost=24, nid=500))

        signalPipe.append(sIn)
        objectPipe.append(oOut)

    else:
        for key in ips.keys():
            # sshObjects.append(Collector(ips[key], data_Queue))
            pass
    time.sleep(1)

    while True:
        t_start = time.time()

        # Database Timestamp !!!
        insertTimestamp = int(time.time())

        # loop over all connections look if they are alive
        for t in sshObjects:
            if not t.is_alive():
                ip = t.ip
                # remove thread from list
                sshObjects.remove(t)
                # recover thread....
                # print "recover", ip
                #sshObjects.append(Collector(ip, data_Queue))
                print 'ssh connection lost'
                exit(1)

        # send requests
        for pipe in signalPipe:
            pipe.send(insertTimestamp)

        # recive data

        for pipe in objectPipe:
            obj = pipe.recv()
            inserter = 0

            # insert in databases

            # Mongo
            if dbMongoactive:
                for pair in mongoInserter.keys():
                # recover database connection
                    (db, pin) = mongoInserter[pair]
                    if not db.is_alive():
                        print 'recover database'
                        mongoInserter[pair][0].close()
                        del mongoInserter[pair]
                        (pin, pout) = multiprocessing.Pipe()
                        mongoInserter[pair] = (DatabaseInserter(
                            pout, dbMongo_conn), pin)

            # put data form collectors into db queue
            if dbMongoactive:
                mongoInserter[inserter][1].send(obj)
                inserter += 1
                if inserter >= numberOfInserterPerDatabase:
                    inserter = 0

            # MySQL
            if dbMySQLactive and not db_mySQL.isAlive():
                # recover database connection
                print 'recover database'
                db_mySQL.close()
                del(db_mySQL)
                db_mySQL = DatabaseInserter(dbMySQL_Queue, dbMySQL_conn)

            # put data form collectors into db queue
            if dbMySQLactive:
                print 'database Queue length:', dbMySQL_Queue.qsize()
                dbMySQL_Queue.put(obj)

            # SQLight
            if dbSQLightactive and not db_SQLight.isAlive():
                # recover database connection
                print 'recover database'
                db_SQLight.close()
                del(db_SQLight)
                db_SQLight = DatabaseInserter(dbSQLight_Queue, dbSQLight_conn)

            # put data form collectors into db queue
            if dbSQLightactive:
                print 'database Queue length:', dbSQLight_Queue.qsize()
                dbSQLight_Queue.put(obj)

        # look at db connectionen if this is alaive
        t_end = time.time()

        time.sleep(ts_delay - (t_end - t_start))
