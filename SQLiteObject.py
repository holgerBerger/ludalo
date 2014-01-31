'''
Created on 16.12.2013

@author: Uwe Schilling
'''
import sqlite3
from AbstractDB import AbstractDB


class SQLiteObject(AbstractDB):
    '''database connection class'''

    def __init__(self, dbFile):
        '''
        Constructor
        '''
        self.dbFile = dbFile
        self.conn = sqlite3.connect(dbFile)
        self.c = self.conn.cursor()

        self.globalnidmap = {}
        self.servermap = {}
        self.per_server_nids = {}
        self.timestamps = {}
        self.sources = {}
        self.servertype = {}
        self.mdsmap = {}
        self.hostfilemap = {}

#------------------------------------------------------------------------------

    def closeConnection(self):
        ''' Closing db connection '''
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
        self._generateSQLite()
        self.insert_timestamp(timeStamp)
        self.insert_server(OST_name, 'ost')
        self.insert_source(OST_name)
        self.insert_SERVER_values(OST_name, REQS, timeStamp, 0)
#------------------------------------------------------------------------------

    def addMDT(self, MDT_name, timeStamp, WR_MB, RD_MB, REQS):
        self._generateSQLite()
        self.insert_timestamp(timeStamp)
        self.insert_server(MDT_name, 'mdt')
        self.insert_source(MDT_name)
        self.insert_SERVER_values(MDT_name, REQS, timeStamp, 1)
#------------------------------------------------------------------------------

    def addOSS(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        self._generateSQLite()
        self.insert_timestamp(timeStamp)
        self.insert_server(OSS_name, 'oss')
        self.insert_source(OSS_name)
        self.insert_SERVER_values(OSS_name, REQS, timeStamp, 2)
#------------------------------------------------------------------------------

    def addMDS(self, MDS_name, timeStamp, WR_MB, RD_MB, REQS):
        self._generateSQLite()
        self.insert_timestamp(timeStamp)
        self.insert_server(MDS_name, 'mds')
        self.insert_source(MDS_name)
        self.insert_SERVER_values(MDS_name, REQS, timeStamp, 3)
#------------------------------------------------------------------------------

    def _addElement(self, tbName, timeStamp, WR_MB, RD_MB, REQS):
        self._generateSQLite()
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
                     'VALUES (?,?,?,?)')
        self.c.execute(exeString, collection)
#------------------------------------------------------------------------------

    def _generateSQLite(self):
        # create table if not exist

        # timestamp
        self.c.execute('''CREATE TABLE IF NOT EXIST
                            timestamps (
                                id integer primary key asc,
                                time text)''')

        # name vom clienten
        self.c.execute('''CREATE TABLE IF NOT EXIST
                            nids (
                                id integer primary key asc,
                                nid text)''')

        # oss/mds server name
        self.c.execute('''CREATE TABLE IF NOT EXIST
                            servers (
                                id integer primary key asc,
                                server text,
                                type text)''')

        # ost / mdt
        self.c.execute('''CREATE TABLE IF NOT EXIST
                            sources (
                                id integer primary key asc,
                                source text)''')

        self.c.execute('''CREATE TABLE IF NOT EXIST
                            ost_values (
                                id integer primary key asc,
                                rio integer,
                                rb integer,
                                wio integer,
                                wb integer)''')

        self.c.execute('''CREATE TABLE IF NOT EXIST
                            ost_nid_values (
                                id integer primary key asc,
                                rio integer,
                                rb integer,
                                wio integer,
                                wb integer)''')

        self.c.execute('''CREATE TABLE IF NOT EXIST
                            mdt_values (
                                id integer primary key asc,
                                reqs integer)''')

        self.c.execute('''CREATE TABLE IF NOT EXIST
                            mdt_nid_values (
                                id integer primary key asc,
                                reqs integer)''')

        self.c.execute('''CREATE TABLE IF NOT EXIST
                            oss_values (
                                id integer primary key asc,
                                reqs integer)''')

        self.c.execute('''CREATE TABLE IF NOT EXIST
                            mds_values (
                                id integer primary key asc,
                                reqs integer)''')

        # verknüpfung
        self.c.execute('''CREATE TABLE IF NOT EXIST
                            samples (
                                id integer primary key asc,
                                time integer, 
                                type integer,
                                source integer, 
                                nid integer,
                                vals integer)''')
        
        self.c.execute('''CREATE INDEX IF NOT EXISTS 
                            samples_time_index ON samples (time)''')
        
        self.c.execute('''CREATE INDEX IF NOT EXISTS 
                            time_index ON timestamps (time)''')

#------------------------------------------------------------------------------

    def _insert_timestamp(self, timestamp):
        if timestamp not in self.timestamps:
            self.cursor.execute('''INSERT INTO timestamps VALUES (NULL,?)''',
                                (timestamp,))
            self.timestamps[timestamp] = self.cursor.lastrowid
#------------------------------------------------------------------------------

    def _insert_source(self, source):
        if source not in self.sources:
            self.cursor.execute('''INSERT INTO sources VALUES (NULL,?)''',
                                (source,))
            self.sources[source] = self.cursor.lastrowid
#------------------------------------------------------------------------------

    def _insert_server(self, server, stype):
        if server not in self.per_server_nids:
            print "new server:", server
            self.per_server_nids[server] = []
            self.cursor.execute('''INSERT INTO servers VALUES (NULL,?)''',
                                (server,))
            self.servermap[server] = self.cursor.lastrowid
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
            self.cursor.execute('''INSERT INTO nids VALUES (NULL,?)''',(nid,))
            self.globalnidmap[nid]=self.cursor.lastrowid
        if nid not in self.per_server_nids[server]:
            self.per_server_nids[server].append(nid)
#------------------------------------------------------------------------------

    def insert_nids(self, server, timestamp, source, nidvals):
        stype = self.servertype[server]
        for i in range(len(nidvals)):
            nidid = self.globalnidmap[self.per_server_nids[server][i]]
            timeid = self.timestamps[timestamp]
            sourceid = self.sources[source]

            if nidvals[i] != "":
                if stype == 'ost':
                    self.cursor.execute('''INSERT INTO ost_nid_values VALUES
                                        (NULL,?,?,?,?)''',
                                        nidvals[i].split(','))

                    lastID = self.cursor.lastrowid
                    self.cursor.execute('''INSERT INTO samples VALUES
                                        (NULL,?,?,?,?,?)''',
                                        (timeid, 0, sourceid, nidid, lastID))

                if stype == 'mdt':
                    self.cursor.execute('''INSERT INTO mdt_nid_values VALUES
                                        (NULL,?)''',
                                        (nidvals[i],))

                    lastID = self.cursor.lastrowid
                    self.cursor.execute('''INSERT INTO samples VALUES
                                        (NULL,?,?,?,?,?)''',
                                        (timeid, 1, sourceid, nidid, lastID))
#------------------------------------------------------------------------------

    def _insert_SERVER_values(self, mds_name, REQS, timeStamp, type):
        ''' type 0->ost 1->mdt 2->oss 3->mds '''
        c = self.c
        timeid = self.timestamps[timeStamp]
        c.execute('''INSERT INTO mds_values (NULL,?)''', (REQS,))
        lastID = c.lastrowid
        mdsID = self.sources[mds_name]
        # sampels:     id    time    type    source    nid    vals
        self.cursor.execute('''INSERT INTO samples VALUES
                                        (NULL,?,?,?,NULL,?)''',
                                        # time  typ  mdsID    values
                                        (timeid, type, mdsID, lastID))
