""" @package    lib.database

    @brief      This module handles all things assigned to the database.

    @details    Database inserters, data structur cases and database
                connections.
                Here is also the place for the configparser wich
                collects the setup from the config to connect to the
                different databases.

                support for:
                - mongo db (full)

                partially support for:
                - mysql
                - sqlite3

    @author      Uwe Schilling uweschilling[at]hlrs.de
"""
import multiprocessing
import time
import re
import os.path
from collections import defaultdict


class DatabaseInserter(multiprocessing.Process):

    """ @brief      This class handles the data from the collectors.

        @details    To store the informations form the collectors into the
                    database is this class needet. It handles async the
                    incomming data form the \em comQueue and inset it into
                    the database with as less as possible
                    dependencies to the collector.

        @param      comQueue is a queue form the python \em multiprocessing modul
        @param      cfg is a \em DatabaseConfigurator object. witch is generated
                    by the ludalo main funktion
        @param      sharedDict is a shared object from the
                    \em multiprocessing.Manager modul.
    """

    def __init__(self, comQueue, cfg, sharedDict):
        """ @brief      Class inti with first database connection
        """
        super(DatabaseInserter, self).__init__()
        self.sharedDict = sharedDict
        self.comQueue = comQueue
        self.cfg = cfg
        self.db = None
        # use this to end job from outside!
        self.exit = multiprocessing.Event()
        try:
            self.db = self.cfg.getNewDB_Mongo_Conn(self.sharedDict)
            # start the thread and begin to insert if entries in the queue
            self.start()
        except:
            print 'init DatabaseInserter with no mongo connection'

    def readhostfile(self):
        """ @brief      This funktion builds the mapping from ip to hostname.
            @details    The \em self.nidMap is build when the process is
                        started and is needet for the maping from ip based
                        adressing to hoste name adressing.
            @return     The nidMap witch is a dictornary containing [Hostname: HostIP]
        """
        hosts = self.cfg.hosts
        pattern = self.cfg.pattern
        replace = self.cfg.replace
        nidMap = {}

        # use provided host file if not use config.
        if os.path.isfile('hosts'):
            hosts = 'hosts'

        try:
            with open(hosts, "r") as f:
                for l in f:
                    if not l.startswith('#'):
                        sp = l.rstrip().split()
                        if len(sp) <= 2:
                            continue
                        ip = sp[0]
                        name = sp[1]
                        nidMap[ip] = re.sub(pattern, replace, name)
                nidMap['aggr'] = 'aggr'
        except:
            print 'etc/hosts read error. please check'
        return nidMap

    def insert(self, jsonDict):
        """ @brief      Extract the datasets from the jsonDict and prepar it for
                        the database.
            @details    The jsonDoct must be converted to a data structur that
                        is compatible with the database
                        ( \em lib.database.PerformanceData ).
                        therfor the jsonDict will be itterated and the
                        information extracted.

            @param      jsonDict the json object is defined as:
            @code
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
            @endcode
        """

        insertTimestamp = jsonDict[0]
        data = jsonDict[1]
        insert_me = []

        t1 = time.time()

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
                hostMashine = sk[1]

                # resources are digie
                if 'nid' not in resourceIP:
                    resourceIP = 'nid' + resourceIP

                # appendens for cray or other systems:
                if not self.cfg.postfix:

                    # take name from nidMap if no name take ip
                    try:
                        resourceName = self.nidMap[resourceIP]

                    except KeyError:
                        print ('no resourceName to ip' + ' ' + resourceIP
                               + ' ' + str(resource_values) + ' ' + fs_name, name)
                        self.nidMap[resourceIP] = resourceIP
                        resourceName = resourceIP
                else:
                    while len(resourceIP) > 5:
                        resourceIP = '0' + resourceIP
                    resourceIP = resourceIP + hostMashine

                # except Exception, e:
                    # print repr(e)

                # nid fs map append
                self.db.insert_nidFS(resourceName, fs_name)

                ins = PerformanceData(
                    insertTimestamp, name, resourceName,
                    resource_values, fs_name, s_type)

                insert_me.append(ins)
        print 'time to bild inserter object:', time.time() - t1
        # Insert data Obj

        t1 = time.time()
        self.db.insert_performance(insert_me)
        print 'time to insert:', time.time() - t1

    def run(self):
        """ @brief      Run this as its owen process.
            @details    "main" funkten of the \em DatabaseInserter.
                        It runs a infinit loop and checks the commutnication
                        Queue for aktions. if this queue is empty it sleeps for
                        0.1 sec and allow the processor to go into a energy
                        efficient mode. (soft pull)
                        If ther is an object in the queue this funkten will
                        pull it out and intsert it into the database.
                        If this fails, it will push it back to the queue to
                        pervent data lost.
        """
        # build hostmap
        self.nidMap = self.readhostfile()

        # self.exit.is_set() fals until exit
        print '    ', self.name, 'Inserter Starting loop'
        while not (self.exit.is_set() and self.comQueue.empty()):
            while self.comQueue.empty():
                time.sleep(0.1)

            # print '    ', self.name, 'Inserter testing if connection is
            # alive'
            if not self.db or not self.db.alive():
                self.reconnect()

            insertObject = self.comQueue.get()
            # Insert the object form pipe db
            # print '    ', self.name, 'Inserter inserting object'
            try:
                self.insert(insertObject)
            except Exception:
                self.comQueue.put(insertObject)
                printstring = ('could not insert object to db,' +
                               ' put it back to queue. Queue length:')
                print printstring, self.comQueue.qsize()

        print 'exit inserter', self.name

    def _close(self):
        """ @brief      This close the connectionen properly
                        if the db thread has problems.
        """
        if self.db:
            self.db.closeConn()

    def shutdown(self):
        """
            @brief      Try to exit gracefully.
        """
        self.exit.set()

    def reconnect(self, nr_try=0):
        """ @brief      Fail save reconnect funktion
            @details    This funktin trys 9 reconnect in short order. After
                        that is a sleep for 30 seconds to prevent spaming on
                        the network connection.
            @param      optional nr_try default is 0. this is the first call.
                        the funktion calls itself with nr_try + 1.
        """
        # try 9 reconnects if not exit
        if not self.db or not self.db.alive():
            try:
                self.db.close()
            except Exception:
                print 'db.close faild'
            if nr_try > 9:
                print 'Reconnection failed! After 9 tries wait 30sec and retry'
                time.sleep(30)
                nr_try = 0
            self.db = None
            try:
                self.db = self.cfg.getNewDB_Mongo_Conn(self.sharedDict)
            except:
                print 'no new mongo connection'
            time.sleep(1)
            self.reconnect(nr_try + 1)


class PerformanceData(object):

    """ @brief      Store the Performance data in a unived way.
        @details    Unived data storage. It generates a singel data set
                    for the different databases.
    """

    def __init__(self, timestamp, target, nid, values, fs, s_type):
        """ @brief      Class inti with all the values needet
            @param      timestamp   Unix timestamp
            @param      target      e.g. OST0002
            @param      nid         the node id
            @param      values      array of data
            @param      fs          filesystem of the data set
            @param      s_type      the server type mds or ost
        """
        super(PerformanceData, self).__init__()

        self.timestamp = timestamp
        self.s_type = s_type
        self.target = target
        self.nid = nid
        self.values = values
        self.fs = fs

    def getMongo_Obj(self):
        """ @brief      create a data set for the mognoDB
            @return     a python dictonary with the data
        """
        obj = {"ts": self.timestamp,
               "st": self.s_type,
               "tgt": self.target,
               "nid": self.nid,
               "val": self.values}
        return obj

    def getSQL_Obj(self):
        """ @brief      create a data set for MySQL and sqlite3
            @return     a python tuple with the data
        """

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

    """ @brief      Database Connection Class for MySQL
        @details    Provide the funktions to comunicate with the database
        @warning    This is untested and not fully implemented.
    """

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

    """ @brief      Database Connection Class for SQLight
        @details    Provide the funktions to comunicate with the database
        @warning    This is untested and not fully implemented.
    """

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

    """ @brief      Database Connection Class for MongoDB
        @details    Provide the funktions to comunicate with the database
    """

    def __init__(self, host, port, dbname, sharedDict=None):
        """ @brief      Init Methode for the database

            @param      host        Hostename or IP adress for the Databaseserver
            @param      prot        Prot of the databesserver
            @param      dbname      Name of the Database
            @param      sharedDict  The nid map shared object

        """

        super(Mongo_Conn, self).__init__()
        try:
            from pymongo import MongoClient
        except Exception, e:
            print 'pleas install pymongo, import failed.'
            raise e

        self.name = 'Mongo_Conn'
        self.sharedDict = sharedDict
        self.lock = multiprocessing.Lock()
        # geting client and connect
        self.client = MongoClient(host, port)

        # getting db
        self.db = self.client[dbname]

        # getting collection
        self.collection = self.db['performanceData']
        self.collectionJobs = self.db['jobs']

        # get fs map
        if self.sharedDict is not None:
            self.getFSmap()

    def getFSmap(self):
        """ @brief      Funktion to update the sharedDict
            @details    This will collect old information form the database
                        and Provide it to all other Processes
        """
        dbNidFS = self.db['nidFS']
        result = dbNidFS.find()
        for item in result:
            self.sharedDict[item['nid']] = item['fs']

    def insert_performance(self, objlist):
        """ @brief      Insert an all objects form a list into the DB.
            @param      objlist is a list of all \em PerformanceData
                        objects to insert.
        """
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
        """ @brief      Insert or update the map of filesystems in the db.
            @details    If the nid has an entry in the database, it will update
                        the filsystems witch are mounted on this nid.
                        If the nid has no entrys or didn't exist it will
                        generate a new entry.

            @param      nid     the node id
            @param      fs      the filesystem
        """
        nidFS = self.db['nidFS']  # collection in the database
        try:
            if fs not in self.sharedDict[nid]:
                self.sharedDict[nid].append(fs)
                nidFS.update(
                    {'nid': nid}, {'nid': nid, 'fs': self.sharedDict[nid]})

        except KeyError:
            print 'insert new fs (', fs, ') to nid (', nid, ')'
            self.sharedDict[nid] = [fs]
            obj = {'nid': nid, 'fs': [fs]}
            nidFS.update({'nid': nid}, obj, upsert=True)

        except Exception, e:
            # if somthing else is wrong
            print repr(e)
            raise e

    def insert_jobData(self, jobid, start, end, owner, nids, cmd):
        """ @brief      Insert Jobdata in the Db.
            @details    will test if job exist and ad the calc value witch
                        indicate that this job is not calculated.
            @param      jobid   ID of the Job
            @param      start   Unix timestamp of the job start
            @param      end     Unix timestamp of the job end
            @param      owner   User ID
            @param      nids    A list of nids allocaded by this job.
        """
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

        if not self.db["jobs"].find_one({"jobid": jobid}):
            self.db["jobs"].insert(obj)
        else:
            print 'duplicated job', jobid

    def getFsData(self, collection, tstart, tend):
        """ @brief      This gets all 'aggr' values of one filesytem by time

            @param      collection      the filesysem name
            @param      tstart          start timestamp
            @param      tend            end timestamp
            @return     this returns a dic witch includ all document form
                        tstart to tend in the collection 'collection'
        """

        #t = time.time()
        result = self.db[collection].find(
            {"ts": {"$gte": tstart, "$lt": tend}, "nid": "aggr"})

        # enpty dict of lists
        returnDict = defaultdict(list)

        for item in result:
            nidDict = {'fs': collection,
                       "st": item['st'],
                       "tgt": item['tgt'],
                       "nid": item['nid'],
                       "val": item['val']}
            returnDict[item['ts']].append(nidDict)
        # print collection, time.time() - t
        # print returnDict
        return returnDict

    def getJobsLeft(self):
        """ @brief      Collect and return info for not calculated jobs
            @return     A tuple of
                        (#of job not calculated,
                            #of jobs witch not ended)
        """
        jobsRun = self.db['jobs'].find({'calc': -1}).count()
        jobsLeft = self.db['jobs'].find(
            {'calc': -1, 'end': {'$gte': 1}}).count()
        return (jobsLeft, jobsRun)

    def oneUncalcJob(self):
        """ @brief      return one uncalced jobID and set calcstat
            @details    for calculation of a job, this is used to finde
                        a job that is not calculated. It also update the
                        job status.
            @return     A singel Job or None in case of no jobs left to find.
        """
        db_query = {'calc': -1, 'end': {'$gte': 1}}
        result = self.db['jobs'].find_one(db_query)
        if result:
            jobID = result['jobid']
            self.set_job_calcState(jobID, 0)
            return jobID
        else:
            return None

    def selectJobData(self, collection, tstart, tend, nids):
        """ @brief      To get information for a Job
            @details    Find all information of one Filesystem to a list of nids
                        between two timestamps.
            @param      collection  the name of the Filesystem
            @param      tstart      start timestamp
            @param      tend        end timesamp
            @param      nids        list of nid names
            @return     a python dict {timestamp : {'fs': 'fsname', [...]}, [...]}
        """
        # find all timestamps between start and end for the nid in nids[]
        db_query = {"ts": {"$gte": tstart, "$lt": tend}, 'nid': {'$in': nids}}

        # execute
        result = self.db[collection].find(db_query)

        # enpty dict of lists
        returnDict = defaultdict(list)

        for item in result:
            nidDict = {'fs': collection,
                       "st": item['st'],
                       "tgt": item['tgt'],
                       "nid": item['nid'],
                       "val": item['val']}
            returnDict[item['ts']].append(nidDict)

        # print returnDict
        return returnDict

    def getJobData(self, jobID):
        """ @brief      Get infos of one Job
            @param      jobID the ID of the job
            @return     (filesystem, tstart, tend, list of nids)
        """
        result = self.db['jobs'].find_one({"jobid": jobID})
        # (collection, tstart, tend, nids)
        # print 'jobid', jobID, 'result set', result
        tstart = result['start']
        tend = result['end']
        nids = result['nids'].split(',')

        collections = set()

        for nid in nids:
            fsList = self.db['nidFS'].find_one({'nid': nid})
            for obj in fsList['fs']:
                collections.add(obj)

        return (collections, tstart, tend, nids)

    def resetCalcState(self):
        """ @brief  Resets all Job in the uncalculated State on the DB.
        """
        self.db["jobs"].update({'calc': 0}, {"$set": {"calc": -1}}, multi=True)

    def saveJobStats(self, jobID, fs, stats):
        """ @brief      Save job stats
            @details    Write statistic of one job back to the database
            @param      jobID   Job ID
            @param      fs      Filesystem of the Job
            @param      stats   a tuple of (total, quartil, mean, var, std, average, duration)
        """
        total, quartil, mean, var, std, average, duration = stats

        self.db["jobStats"].update({'jobid': jobID}, {'$set': {
            'fs': fs,
            'total': total,
            'quartil': quartil,
            'mean': mean,
            'var': var,
            'std': std,
            'average': average,
        }}, upsert=True)

    def set_job_calcState(self, jobid, calc, start=None):
        """ @brief      set the calculate stat of a job
            @details    # calc -1 job not calculatet
                        # calc 0 job in calculation
                        # calc 1 job compleet calculated
                        # calc 2 no data or curupted
            @param      jobid   ID of the Job
            @param      calc    is the calc status see details
            @param      start   if ID not from the Databas pleas append start
                                time this is to prevent duplicated job ids
        """

        if start:
            cyear = time.localtime(start).tm_year
            jobid = jobid + "-" + str(cyear)
        self.db["jobs"].update({"jobid": jobid}, {"$set": {"calc": calc}})

    def update_jobData(self, jobid, start, end, owner, nids, cmd):
        """ @brief      update job end time
            @param      jobid
            @param      start   job start timestamp
            @param      end     job end timestamp
            @param      owner   user id
            @param      nids    list of nids
            @param      cmd
        """
        cyear = time.localtime(end).tm_year
        # to handle year change, do it twice, will update only once
        dbjobid = jobid + "-" + str(cyear - 1)
        self.db["jobs"].update({"jobid": dbjobid}, {"$set": {"end": end}})

        dbjobid = jobid + "-" + str(cyear)
        self.db["jobs"].update({"jobid": dbjobid}, {"$set": {"end": end}})

    # statisic

    def updateJobStats(self):
        """ @brief      not NotImplemented
        """
        pass
        # self.db['webCache'].update({'typ': 'job'},
        #                           {'$set':
        #                           {'run': getJobsLeft()[1],
        #                            ''}})

    def closeConn(self):
        """ @brief      close the database connection
        """
        self.client.close()

    def commit(self):
        """ @brief      the other db systems require a commit but Mongo not.
        """
        pass

    def alive(self):
        """ @brief      Status of the connection
            @return     True    if connection is alive
                        False   if connection is dead
        """
        return self.client.alive()


class DatabaseConfigurator(object):

    """ @brief      This class handles the configparser
        @details
        @param
        @return
    """

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

        if self.cfg.has_section('batchsystem'):
            self.postfix = self.cfg.get('batchsystem', 'postfix')

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

    def getNewDB_Mongo_Conn(self, sharedDict):
        if self.cfg.has_section(self.sectionMongo):
            if self.cfg.getboolean(self.sectionMongo, 'aktiv'):
                # host, port, dbname
                host = self.cfg.get(self.sectionMongo, 'host')
                port = self.cfg.getint(self.sectionMongo, 'port')
                dbname = self.cfg.get(self.sectionMongo, 'dbname')
                # do stuff
                return Mongo_Conn(host, port, dbname, sharedDict)
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
