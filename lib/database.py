import multiprocessing
import time
import re
'''
This module handles all things assigned to the database.
Database inserters, data structur cases and database connections.
Here is also the place for the configparser wich collects the setup
from the config to connect to the different databases.

support for:
- mongo db (full)

partially support for:
- mysql
- sqlite3

'''


class DatabaseInserter(multiprocessing.Process):

    '''
    This class handles the data from the collectors (inserterQueue)
    implemented as Thread to insert async and with as less as possible
    dependencies to the collector.
    '''

    def __init__(self, comQueue, cfg):
        super(DatabaseInserter, self).__init__()
        self.comQueue = comQueue
        self.cfg = cfg
        self.db = None
        # use this to end job from outside!
        self.exit = multiprocessing.Event()
        self.nidMap = self.readhostfile()
        try:
            self.db = self.cfg.getNewDB_Mongo_Conn()
            # start the thread and begin to insert if entrys in the queue
            self.start()
        except:
            pass

    def readhostfile(self):
        hosts = self.cfg.hosts
        pattern = self.cfg.pattern
        replace = self.cfg.replace
        nidMap = {}
        try:
            with open(hosts, "r") as f:
                for l in f:
                    if not l.startswith('#'):
                        sp = l[:-1].split()
                        if len(sp) == 0:
                            continue
                        ip = sp[0]
                        name = sp[1]
                        nidMap[ip] = re.sub(pattern, replace, name)
                nidMap['aggr'] = 'aggr'
        except:
            print 'etc/hosts read error. please check'
        return nidMap

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
                resourceName = self.nidMap[resourceIP]

                # nid fs map append
                self.db.insert_nidFS(resourceName, fs_name)

                ins = PerformanceData(
                    insertTimestamp, name, resourceName,
                    resource_values, fs_name, s_type)
                insert_me.append(ins)

        # Insert data Obj
        self.db.insert_performance(insert_me)

    def run(self):
        while not self.exit.is_set():
            while self.comQueue.empty():
                time.sleep(0.1)
            if not self.comQueue.empty():
                if not self.db or not self.db.alive():
                    self.reconnect()

                insertObject = self.comQueue.get()
                # Insert the object form pipe db
                try:
                    self.insert(insertObject)
                except Exception, e:
                    self.comQueue.put(insertObject)
                    print 'could not insert object to db, put it back to queue. Queue length:', self.comQueue.qsize()
                    print 'exeption:', e
        print 'exit inserter', self.name

    def _close(self):
        '''
            to close the connectionen properly if the db thread has problems
        '''
        if self.db:
            self.db.closeConn()

    def shutdown(self):
        self.exit.set()

    def reconnect(self, nr_try=0):
        # try 9 reconnects if not exit
        if not self.db or not self.db.alive():
            try:
                self.db.close()
            except Exception:
                pass
            if nr_try > 9:
                print 'Reconnection faild! after 9 trys wait 30 sec and retry'
                time.sleep(30)
                nr_try = 0
            self.db = None
            try:
                self.db = self.cfg.getNewDB_Mongo_Conn()
            except:
                pass
            time.sleep(1)
            self.reconnect(nr_try + 1)


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
        import MySQLdb

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
                with self.lock:
                    self.generateDatabaseTable(obj.fs)
            fslist[obj.fs].append(obj.getSQL_Obj())

        sum = 0
        # t1 = time.time()
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
                self.c.executemany(query, fslist[fs])

            sum += len(fslist[fs])
        # t2 = time.time()
        # print "inserted %d documents into MySQL (%d inserts/sec)" % (sum, sum
        # / (t2 - t1))

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
        import sqlite3

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
                with self.lock:
                    self.generateDatabaseTable(obj.fs)
            fslist[obj.fs].append(obj.getSQL_Obj())

        sum = 0
        #t1 = time.time()
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
                self.c.executemany(query, fslist[fs])

            sum += len(fslist[fs])
        #t2 = time.time()
        # print "inserted %d documents into sqlite3 (%d inserts/sec)" % (sum,
        # sum / (t2 - t1))

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
        from pymongo import MongoClient

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
        #t1 = time.time()
        for fs in fslist.keys():

            # Prevent other threads form execute
            with self.lock:
                self.db[fs].insert(fslist[fs])

            sum += len(fslist[fs])
        #t2 = time.time()
        # print "inserted %d documents into MongoDB (%d inserts/sec)" % (sum,
        # sum / (t2 - t1))

    def insert_nidFS(self, nid, fs):
        nidFS = self.db['nidFS']
        result = nidFS.find_one({'nid': nid})
        print self.name, 'find fs to nid:', nid, fs, 'result set:', result
        if result:
            allfs = result['fs']
            allfs.append(fs)
            nidFS.update({'nid': nid}, {'nid': nid, 'fs': allfs})
        else:
            obj = {'nid': nid, 'fs': [fs]}
            nidFS.insert(obj)

    def insert_jobData(self, jobid, start, end, owner, nids, cmd):
        cyear = time.localtime(start).tm_year
        jobid = jobid + "-" + str(cyear)

        obj = {"jobid": jobid,
               "owner": owner,
               "start": start,
               "end": end,
               "nids": nids,
               "cmd": cmd,
               "calc": -1}
        # calc -1 job not calculatet
        # calc 0 job in calculation
        # calc 1 job compleet calculated

        self.db["jobs"].insert(obj)

    def set_job_calcState(self, jobid, start, calc):
        cyear = time.localtime(start).tm_year
        jobid = jobid + "-" + str(cyear)

        self.db["jobs"].update({"jobid": jobid}, {"$set": {"clac": calc}})

    def update_jobData(self, jobid, start, end):
        cyear = time.localtime(start).tm_year
        jobid = jobid + "-" + str(cyear)

        self.db["jobs"].update({"jobid": jobid}, {"$set": {"end": end}})

    def closeConn(self):
        self.client.close()

    def commit(self):
        pass

    def alive(self):
        return self.client.alive()


class DatabaseConfigurator(object):

    """this class handles the configparser"""

    def __init__(self, cfgFile):
        super(DatabaseConfigurator, self).__init__()
        import sys
        from ConfigParser import ConfigParser

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

        if self.cfg.has_section('ludaloConfig'):
            self.sleepTime = self.cfg.getint('ludaloConfig', 'sleepTime')
            self.extractorSleep = self.cfg.getint(
                'ludaloConfig', 'extractorSleep')
            self.numberOfInserterPerDatabase = self.cfg.getint(
                'ludaloConfig', 'numberOfInserterPerDatabase')
            self.numberOfExtractros = self.cfg.getint(
                'ludaloConfig', 'numberOfExtractros')
        else:
            output = ('[ludaloConfig]' + '\n' +
                      'sleepTime = 60' + '\n' +
                      'extractorSleep = 60' + '\n' +
                      'numberOfInserterPerDatabase = 3' + '\n' +
                      'numberOfExtractros = 3' + '\n' + '\n')
            print 'missing config option. Please append \n', output
            exit(1)

        if self.cfg.has_section('hostfile'):
            self.hosts = self.cfg.get('hostfile', 'hosts')

        if self.cfg.has_section('replacePattern'):
            self.pattern = self.cfg.get('replacePattern', 'pattern')
            self.replace = self.cfg.get('replacePattern', 'replace')

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

                     '[ludaloConfig]' + '\n' +
                     'sleepTime = 60' + '\n' +
                     'extractorSleep = 60' + '\n' +
                     'numberOfInserterPerDatabase = 3' + '\n' + '\n' +
                     'numberOfExtractros = 3' + '\n' + '\n' +

                     '[batchsystem]' + '\n' +
                     'postfix = [.name]' + '\n' +
                     'usermapping = [file]' + '\n' + '\n')

        f = open(defaultCfgFile, 'w')
        f.write(cfgString)

    def getNewDB_Mongo_Conn(self):
        if self.cfg.has_section(self.sectionMongo):
            if self.cfg.getboolean(self.sectionMongo, 'aktiv'):
                # host, port, dbname
                host = self.cfg.get(self.sectionMongo, 'host')
                port = self.cfg.getint(self.sectionMongo, 'port')
                dbname = self.cfg.get(self.sectionMongo, 'dbname')
                # do stuff
                return Mongo_Conn(host, port, dbname)
        else:
            print 'No connection MongoDB configered!'

    def getNewDB_MySQL_Conn(self):
        # configuration has mysql
        if self.cfg.has_section(self.sectionMySQL):
            # mysql set to aktiv
            if self.cfg.getboolean(self.sectionMySQL, 'aktiv'):
                if not (self.sectionMySQL in self.databases):
                    # host, port, user, password, dbname
                    host = self.cfg.get(self.sectionMySQL, 'host')
                    port = self.cfg.get(self.sectionMySQL, 'port')
                    user = self.cfg.get(self.sectionMySQL, 'user')
                    password = self.cfg.get(self.sectionMySQL, 'password')
                    dbname = self.cfg.get(self.sectionMySQL, 'dbname')
                    # do stuff
                    self.databases[self.sectionMySQL] = MySQL_Conn(
                        host, port, user, password, dbname)
                return self.databases[self.sectionMySQL]
        else:
            print 'No connection MySQL configered!'

    def getNewDB_SQLight_Conn(self):
        # configuration has sqligth
        if self.cfg.has_section(self.sectionSQLight):
            # sqligth set to aktiv
            if self.cfg.getboolean(self.sectionSQLight, 'aktiv'):
                # ther is no other connection
                if not (self.sectionSQLight in self.databases):
                    # path
                    path = self.cfg.get(self.sectionSQLight, 'path')
                    # do stuff
                    self.databases[self.sectionSQLight] = SQLight_Conn(path)
                return self.databases[self.sectionSQLight]
        else:
            print 'No connection SQLight configered!'
