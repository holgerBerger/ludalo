'''
Created on 16.12.2013

@author: Uwe Schilling
'''
import psycopg2
from ConfigParser import ConfigParser


class PSQLObject(object):
    '''database connection class'''

    def __init__(self, dbFile):
        '''
        Constructor
        '''
        self.DB_VERSION = 1
        self.dbFile = dbFile
        self.config = ConfigParser()
        try:
            self.config.readfp(open("db.conf"))
        except IOError:
            print "no db.conf file found."
            sys.exit()
        self.dbname = self.config.get("database","name")
        self.dbpassword = self.config.get("database","password")
        self.conn = psycopg2.connect("dbname=%s" % self.dbname)
        self.c = self.conn.cursor()

        self.globalnidmap = {}
        self.servermap = {}
        self.per_server_nids = {}
        self.timestamps = {}
        self.sources = {}
        self.servertype = {}
        self.mdsmap = {}
        self.hostfilemap = {}
        
        # init sqliteDB
        self.build_database()
        if not self.check_version():
            self.c.execute(''' select version from version
                                        order by id
                                        desc limit 1 ''')
            v = self.c.fetchone()
            version = v[0]
            print ('\nThere is something wrong with the Database\n' +
                       'DB version is ' + str(version) +
                       ''' but expect version ''' +
                       str(self.DB_VERSION))
            sys.exit(0)

#------------------------------------------------------------------------------
    def build_database(self):
        self._generateDatabase()
        # bild map's

        # nid map
        self.c.execute('''SELECT * FROM nids;''')
        r = self.c.fetchall()
        for (k, v) in r:
            self.globalnidmap[str(v)] = k
        print "read %s old nid mappings" % len(self.globalnidmap)

        # sources map
        self.c.execute('''SELECT * FROM sources;''')
        r = self.c.fetchall()
        for (k, v) in r:
            self.sources[str(v)] = k
        print "read %s old sources" % len(self.sources)

        # server map
        self.c.execute('''SELECT * FROM servers;''')
        r = self.c.fetchall()
        for (k, v, t) in r:
            self.servermap[str(v)] = k
            self.per_server_nids[str(v)] = []
            self.servertype[str(v)] = t
            print "known server:", v, t

        # time stamp map
        self.c.execute('''SELECT * FROM timestamps;''')
        r = self.c.fetchall()
        for (k, v) in r:
            self.timestamps[str(v)] = k
        print "read %d old timestamps" % len(self.timestamps)


#------------------------------------------------------------------------------

    def closeConnection(self):
        ''' Closing db connection '''
        self.conn.commit()
        self.conn.close()
#------------------------------------------------------------------------------

    def addUser(self, userName, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()
#------------------------------------------------------------------------------

    def addJob(self, jobID, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()
#------------------------------------------------------------------------------

    def addGlobal(self, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()
#------------------------------------------------------------------------------

    def addOST(self, OST_name, timeStamp, WR_MB, RD_MB, REQS):
        self.insert_timestamp(timeStamp)
        self.insert_server(OST_name, 'ost')
        self.insert_source(OST_name)
        self.insert_SERVER_values(OST_name, REQS, timeStamp, 0)
#------------------------------------------------------------------------------

    def addMDT(self, MDT_name, timeStamp, WR_MB, RD_MB, REQS):
        self.insert_timestamp(timeStamp)
        self.insert_server(MDT_name, 'mdt')
        self.insert_source(MDT_name)
        self.insert_SERVER_values(MDT_name, REQS, timeStamp, 1)
#------------------------------------------------------------------------------

    def addOSS(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        self.insert_timestamp(timeStamp)
        self.insert_server(OSS_name, 'oss')
        self.insert_source(OSS_name)
        self.insert_SERVER_values(OSS_name, REQS, timeStamp, 2)
#------------------------------------------------------------------------------

    def addMDS(self, MDS_name, timeStamp, WR_MB, RD_MB, REQS):
        self.insert_timestamp(timeStamp)
        self.insert_server(MDS_name, 'mds')
        self.insert_source(MDS_name)
        self.insert_SERVER_values(MDS_name, REQS, timeStamp, 3)
#------------------------------------------------------------------------------

    def _addElement(self, tbName, timeStamp, WR_MB, RD_MB, REQS):
        self._addEntry(tbName, timeStamp, WR_MB, RD_MB, REQS)
#------------------------------------------------------------------------------

    def _addEntry(self, tbName, timeStamp, WR_MB, RD_MB, REQS):
        collection = (timeStamp, WR_MB, RD_MB, REQS)
        exeString = ('INSERT INTO ' + str(tbName) + '(' +
                     'timeStamp,' +
                     'WR_MB,' +
                     'RD_MB,' +
                     'REQS' +
                     ')' +
                     'VALUES (%s,%s,%s,%s)')
        self.c.execute(exeString, collection)
#------------------------------------------------------------------------------

    def _generateDatabase(self):
        # create table if not exist

        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            version (
                                id serial primary key,
                                version integer)''')

        # timestamp
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            timestamps (
                                id serial primary key ,
                                time integer)''')

        # name vom clienten
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            nids (
                                id serial primary key ,
                                nid varchar(64))''')

        # oss/mds server name
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            servers (
                                id serial primary key ,
                                server text,
                                type text)''')

        # ost / mdt
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            sources (
                                id serial primary key ,
                                source text)''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            ost_values (
                                id serial primary key ,
                                time integer, 
                                source text,
                                rio integer,
                                rb bigint,
                                wio integer,
                                wb bigint)''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            mdt_values (
                                id serial primary key ,
                                reqs integer)''')
        
        self.c.execute('''CREATE TABLE IF NOT EXISTS 
                            samples_ost (
                                id serial primary key , 
                                time integer, 
                                source integer, 
                                nid integer, 
                                rio integer, 
                                rb bigint, 
                                wio integer, 
                                wb bigint);''')
        
        self.c.execute('''CREATE TABLE IF NOT EXISTS 
                            samples_mdt (
                                id serial primary key , 
                                  time integer, 
                                  source integer, 
                                  nid integer, 
                                  reqs integer);''')

        self.c.execute(''' CREATE TABLE IF NOT EXISTS
                            users (
                                id serial primary key,
                                username text); ''')

        self.c.execute(''' CREATE TABLE IF NOT EXISTS
                            jobs (
                                id serial primary key,
                                jobid varchar(32),
                                t_start integer,
                                t_end integer,
                                owner integer,
                                nodelist text,
                                cmd text,
                                r_sum bigint,
                                w_sum bigint,
                                reqs_sum bigint); ''')

        self.c.execute(''' CREATE TABLE IF NOT EXISTS
                            nodelist (
                                id serial primary key,
                                job integer,
                                nid integer); ''')

        self.c.execute(''' CREATE TABLE IF NOT EXISTS
                            hashes (
                                hash varchar(63) primary key);''')
#------------------------------------------------------------------------------

        # create INDEX if not exists
        try:
          self.c.execute('''CREATE INDEX
                            jobid_index
                            ON jobs (jobid,t_start,t_end,owner);''')
        except:
          pass

        try:
          self.c.execute('''CREATE INDEX
                            nodelist_index
                            ON nodelist (job,nid);''')
        except:
          pass

        try:
          self.c.execute('''CREATE INDEX 
                              samples_ost_index ON samples_ost (time, nid)''')
        except:
          self.conn.rollback()

        try:
          self.c.execute('''CREATE INDEX
                              ost_values_index ON ost_values (time)''')
        except:
          self.conn.rollback()

        try:
          self.c.execute('''CREATE INDEX
                              samples_mdt_time ON samples_mdt (time)''')
        except:
          self.conn.rollback()

        try:
          self.c.execute('''CREATE INDEX
                              time_index ON timestamps (time)''')
        except:
          self.conn.rollback()
          
        try:
          self.c.execute('''CREATE INDEX
                              nids_index ON nids (nid)''')
        except:
          self.conn.rollback()
          
#------------------------------------------------------------------------------

    def check_version(self):
        self.c.execute(''' select version from version
                                        order by id
                                        desc limit 1 ''')
        version = self.c.fetchone()
        if version:
            if version[0] == self.DB_VERSION:
                return True
            else:
                return False
        else:
            self.c.execute(''' INSERT INTO version
                                    VALUES (DEFAULT, %s) ''', (self.DB_VERSION,))
            self.conn.commit()
            return self.check_version()

#------------------------------------------------------------------------------
    def has_hash(self, hexdigest):
        self.c.execute('''SELECT * FROM hashes WHERE hash=%s''', (hexdigest,))
        r = self.c.fetchall()
        if r:
            return True
        else:
            self.c.execute(''' INSERT INTO hashes VALUES (%s)''', (hexdigest,))
            return False
#------------------------------------------------------------------------------

    def insert_ost_global(self, server, tup, timestamp):
        if self.servertype[server] == 'ost':
            tup = tup.split(',')
            insert_string = []
            insert_string.append(self.timestamps[timestamp])
            insert_string.append(server)
            insert_string.append(tup[0]) # rio
            insert_string.append(tup[1]) # rb
            insert_string.append(tup[2]) # wio
            insert_string.append(tup[3]) # wb
            self.c.execute(''' INSERT INTO ost_values VALUES (DEFAULT, %s,%s,%s, %s,%s,%s)
                    ''', insert_string)
#------------------------------------------------------------------------------

    def insert_timestamp(self, timestamp):
        if timestamp not in self.timestamps:
            self.c.execute('''INSERT INTO timestamps VALUES (DEFAULT,%s) RETURNING ID''',
                                (timestamp,))
            self.timestamps[timestamp] = self.c.fetchone()[0]
#------------------------------------------------------------------------------

    def insert_source(self, source):
        if source not in self.sources:
            self.c.execute('''INSERT INTO sources VALUES (DEFAULT,%s) RETURNING ID''',
                                (source,))
            self.sources[source] = self.c.fetchone()[0]
#------------------------------------------------------------------------------

    def insert_server(self, server, stype):
        if server not in self.per_server_nids:
            print "new server:", server
            self.per_server_nids[server] = []
            self.c.execute('''INSERT INTO servers VALUES (DEFAULT,%s,%s) RETURNING ID''',
                                (server,stype,))
            self.servermap[server] = self.c.fetchone()[0]
            self.servertype[server] = stype
#------------------------------------------------------------------------------

    def add_nid_server(self, server, nid_name):
        nid = nid_name.split('@')[0]
        if self.hostfilemap:
            try:
                nid = self.hostfilemap[nid]
            except KeyError:
                pass
        if nid not in self.globalnidmap:
            self.c.execute('''INSERT INTO nids VALUES (DEFAULT,%s) RETURNING ID''',(nid,))
            self.globalnidmap[nid]=self.c.fetchone()[0]
        if nid not in self.per_server_nids[server]:
            self.per_server_nids[server].append(nid)
#------------------------------------------------------------------------------

    def getNidID(self, server, i):
        return self.globalnidmap[self.per_server_nids[server][i]]
#------------------------------------------------------------------------------

    def insert_ost_samples(self, il_ost):
        self.c.executemany('''INSERT INTO samples_ost VALUES (DEFAULT,%s,%s,%s,%s,%s,%s,%s)''',il_ost)
#------------------------------------------------------------------------------

    def insert_mdt_samples(self, il_mdt):
        self.c.executemany('''INSERT INTO samples_mdt VALUES (DEFAULT,%s,%s,%s,%s)''',il_mdt)
#------------------------------------------------------------------------------

    def insert_SERVER_values(self, mds_name, REQS, timeStamp, s_type):
        ''' type 0->ost 1->mdt 2->oss 3->mds '''
        c = self.c
        timeid = self.timestamps[timeStamp]
        c.execute('''INSERT INTO mds_values (DEFAULT,%s) RETURNING ID''', (REQS,))
        lastID = c.fetchone()[0]
        mdsID = self.sources[mds_name]
        # sampels:     id    time    type    source    nid    vals
        self.c.execute('''INSERT INTO samples VALUES
                                        (DEFAULT,%s,%s,%s,NULL,%s)''',
                                        # time  typ  mdsID    values
                                        (timeid, s_type, mdsID, lastID))
