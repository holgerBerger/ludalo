'''
Created on 16.12.2013

@author: Uwe Schilling
'''
import MySQLdb
from ConfigParser import ConfigParser
import sys, time, re


class MySQLObject(object):
    '''database connection class'''

    def __init__(self):
        '''
        Constructor
        '''
        self.DB_VERSION = 4
        self.config = ConfigParser()
        try:
            self.config.readfp(open("db.conf"))
        except IOError:
            print "no db.conf file found."
            sys.exit()
        self.dbname = self.config.get("database", "name")
        self.dbpassword = self.config.get("database", "password")
        self.dbhost = self.config.get("database", "host")
        self.dbport = int(self.config.get("database", "port"))
        self.dbuser = self.config.get("database", "user")
        self.hostfile = self.config.get("database", "hosts")
        self.pattern = self.config.get("database", "pattern")
        self.replace = self.config.get("database", "replace")

        self.batchpostfix = self.config.get("batchsystem", "postfix")

        self.conn = MySQLdb.connect(passwd=self.dbpassword, db=self.dbname, host=self.dbhost, port=self.dbport, user=self.dbuser)
        self.conn.autocommit(True)   # we enable autocommit to avoid locking issues
        self.c = self.conn.cursor()

        self.globalnidmap = {}
        self.filesystemmap = {}
        self.servermap = {}
        self.per_server_nids = {}
        self.timestamps = {}
        self.sources = {}
        self.servertype = {}
        self.mdsmap = {}
        self.hostfilemap = {}
        self.insertfile = None

        # init DB
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
        self.readhostfile()

#------------------------------------------------------------------------------
    def build_database(self):
        self._generateDatabase()
        # bild map's

        # fs map
        self.c.execute('''SELECT * FROM filesystems;''')
        r = self.c.fetchall()
        for (k, v) in r:
            self.filesystemmap[str(v)] = k
        print "read %s old filesystemmap mappings" % len(self.filesystemmap)

        # nid map
        self.c.execute('''SELECT * FROM nids;''')
        r = self.c.fetchall()
        for (k, v) in r:
            self.globalnidmap[str(v)] = k
        print "read %s old nid mappings" % len(self.globalnidmap)

        # sources map
        self.c.execute('''SELECT * FROM targets;''')
        r = self.c.fetchall()
        for (k, v, b) in r:
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
        if self.insertfile:
            print "completing insert by reading previously created CSV file..."   ,
            sys.stdout.flush()
            t1 = time.time()
            self.c.execute("""load data infile '/tmp/samples_ost.txt' into table samples_ost COLUMNS TERMINATED BY ',';""")
            t2 = time.time()
            print t2 - t1, "secs"
        print "analyzing database for better performance...",
        sys.stdout.flush()
        t1 = time.time()
        self.c.execute("ANALYZE TABLE samples_ost, timestamps, nids, jobs, nodelist;");
        t2 = time.time()
        print t2 - t1, "secs"
        self.conn.commit()
        self.conn.close()
#------------------------------------------------------------------------------

    def readhostfile(self):
        try:
            f = open(self.hostfile, "r")
        except:
            return
        for l in f:
            if not l.startswith('#'):
                sp = l[:-1].split()
                if len(sp) == 0:
                    continue
                ip = sp[0]
                name = sp[1]
                self.hostfilemap[ip] = re.sub(self.pattern, self.replace, name)
        print "read", len(self.hostfilemap), "host mappings"
        f.close()
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
        # self.conn.commit() # added
#------------------------------------------------------------------------------

    def _generateDatabase(self):
        # create table if not exist

        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            version (
                                id serial primary key,
                                version integer
                                COMMENT
                                'This describe the Version of the Database'
                                    ) engine=innodb''')

        # timestamp
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            timestamps (
                                id serial primary key ,
                                c_timestamp integer
                                COMMENT
                                'This is an time stamp of one Sample'
                                ) engine=innodb''')

        # name vom clienten
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            nids (
                                id serial primary key ,
                                nid varchar(64)
                                COMMENT
                                'This is the name of one nid'
                                ) engine=innodb''')

        # oss/mds server name
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            servers (
                                id serial primary key ,
                                server text
                                COMMENT
                                'name of the server',
                                server_type text
                                COMMENT
                                'describes the server type'
                                ) engine=innodb''')

        # ost / mdt
        self.c.execute('''
                        CREATE TABLE IF NOT EXISTS targets (
                            id serial primary key ,
                            target varchar(32)
                            COMMENT
                            'name of the target like lnec-OST000c or lnec-MDT0000',
                            fsid integer
                            COMMENT
                            'map to the filesystem'
                            ) engine=innodb''')

        self.c.execute('''
                        CREATE TABLE IF NOT EXISTS ost_values (
                            id serial primary key,
                            timestamp_id integer
                            COMMENT
                            'map to the timestamps table',
                            target integer
                            COMMENT
                            'map to the targets table',
                            server integer
                            COMMENT
                            'map to the servers table',
                            rio integer
                            COMMENT
                            'read io value in byte',
                            rb bigint
                            COMMENT
                            'read value in byte',
                            wio integer
                            COMMENT
                            'write io value in byte',
                            wb bigint
                            COMMENT
                            'write value in byte'
                        )  engine=innodb''')

        self.c.execute(''' CREATE TABLE IF NOT EXISTS
                            mdt_values (
                                id serial primary key ,
                                reqs integer) engine=innodb''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            samples_ost (
                                id serial primary key,
                                timestamp_id integer,
                                target integer,
                                nid integer,
                                rio integer,
                                rb bigint,
                                wio integer,
                                wb bigint) engine=innodb;''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS
                            samples_mdt (
                                id serial primary key,
                                  timestamp_id integer,
                                  target integer,
                                  nid integer,
                                  reqs integer) engine=innodb;''')

        self.c.execute(''' CREATE TABLE IF NOT EXISTS
                            users (
                                id serial primary key,
                                username text) engine=innodb; ''')

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
                                reqs_sum bigint) engine=innodb; ''')

        self.c.execute(''' CREATE TABLE IF NOT EXISTS
                            nodelist (
                                id serial primary key,
                                job integer,
                                nid integer) engine=innodb; ''')

        self.c.execute(''' CREATE TABLE IF NOT EXISTS
                            hashes (
                                hash varchar(63) primary key) engine=innodb;''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS
                    filesystems (
                        id serial primary key ,
                        filesystem varchar(32)) engine=innodb''')
        # self.conn.commit()  # added
#------------------------------------------------------------------------------
        # create INDEX if not exists
        try:
            self.c.execute('''CREATE INDEX
                            targets_index
                            ON targets (target);''')
        except:
            pass
        try:
            self.c.execute('''CREATE INDEX
                            filesystems_index
                            ON filesystems (filesystem);''')
        except:
            pass
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
                              samples_ost_index ON samples_ost (timestamp_id, nid, target);''')
        except:
            pass

        try:
            self.c.execute('''CREATE INDEX
                              ost_values_index ON ost_values (timestamp_id);''')
        except:
            pass

        try:
            self.c.execute('''CREATE INDEX
                              samples_mdt_time ON samples_mdt (timestamp_id);''')
        except:
            pass

        try:
            self.c.execute('''CREATE INDEX
                              time_index ON timestamps (c_timestamp);''')
        except:
            pass

        try:
            self.c.execute('''CREATE INDEX
                              nids_index ON nids (nid);''')
        except:
            pass

        # self.conn.commit()  # added
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
                                    VALUES (NULL, %s) ''', (self.DB_VERSION,))
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
            # self.conn.commit()  # added
            return False
#------------------------------------------------------------------------------

    def insert_ost_global(self, target, tup, timestamp, server):
        if self.servertype[server] == 'ost':
            tup = tup.split(',')
            target = self.sources[target]
            server = self.servermap[server]
            insert_string = []
            insert_string.append(self.timestamps[timestamp])
            insert_string.append(target)
            insert_string.append(server)
            insert_string.append(tup[0])  # rio
            insert_string.append(tup[1])  # rb
            insert_string.append(tup[2])  # wio
            insert_string.append(tup[3])  # wb
            self.c.execute(''' INSERT INTO ost_values VALUES (NULL, %s,%s,%s,%s,%s,%s,%s)
                    ''', insert_string)
            # self.conn.commit()   # added
#------------------------------------------------------------------------------

    def insert_timestamp(self, timestamp):
        if timestamp not in self.timestamps:
            self.c.execute('''INSERT INTO timestamps VALUES (NULL,%s)''',
                                (timestamp,))
            self.timestamps[timestamp] = self.c.lastrowid
            # self.conn.commit()   # added
#------------------------------------------------------------------------------

    def insert_source(self, source, fsName):
        if fsName not in self.filesystemmap:
            self.c.execute('''INSERT INTO filesystems VALUES (NULL,%s)''',
                                (fsName,))
            fsid = self.c.lastrowid
            self.filesystemmap[fsName] = fsid
        else:
            fsid = self.filesystemmap[fsName]

        if source not in self.sources:
            self.c.execute('''INSERT INTO targets VALUES (NULL,%s,%s)''',
                                (source, fsid))
            self.sources[source] = self.c.lastrowid
        # self.conn.commit()  # added
#------------------------------------------------------------------------------

    def insert_server(self, server, stype):
        if server not in self.per_server_nids:
            print "new server:", server
            self.per_server_nids[server] = []
            self.c.execute('''INSERT INTO servers VALUES (NULL,%s,%s)''',
                                (server, stype,))
            self.servermap[server] = self.c.lastrowid
            self.servertype[server] = stype
            # self.conn.commit()  # added
#------------------------------------------------------------------------------

    def add_nid_server(self, server, nid_name):
        nid = nid_name.split('@')[0]
        if self.hostfilemap:
            try:
                nid = self.hostfilemap[nid]
            except KeyError:
                pass
        if nid not in self.globalnidmap:
            self.c.execute('''INSERT INTO nids VALUES (NULL,%s)''', (nid,))
            self.globalnidmap[nid] = self.c.lastrowid
            # self.conn.commit()  # added
        if nid not in self.per_server_nids[server]:
            self.per_server_nids[server].append(nid)
#------------------------------------------------------------------------------

    def getNidID(self, server, i):
        return self.globalnidmap[self.per_server_nids[server][i]]
#------------------------------------------------------------------------------

    def insert_ost_samples(self, il_ost):
        if self.dbhost == 'localhost':
            #print "shortcut possible", len(il_ost)
            if not self.insertfile:
                self.insertfile = open("/tmp/samples_ost.txt", "w")
            for v in il_ost:
                #print v
                self.insertfile.write("NULL,%d,%d,%d,%s,%s,%s,%s\n" % tuple(v))
        else:
            self.c.executemany('''INSERT INTO samples_ost VALUES (NULL,%s,%s,%s,%s,%s,%s,%s)''', il_ost)
            # self.conn.commit()  # added
#------------------------------------------------------------------------------

    def insert_mdt_samples(self, il_mdt):
        self.c.executemany('''INSERT INTO samples_mdt VALUES (NULL,%s,%s,%s,%s)''', il_mdt)
        # self.conn.commit()  # added
#------------------------------------------------------------------------------

    def insert_SERVER_values(self, mds_name, REQS, timeStamp, s_type):
        ''' type 0->ost 1->mdt 2->oss 3->mds '''
        c = self.c
        timeid = self.timestamps[timeStamp]
        c.execute('''INSERT INTO mds_values (NULL,%s)''', (REQS,))
        lastID = c.lastrowid
        mdsID = self.sources[mds_name]
        # sampels:     id    time    type    source    nid    vals
        self.c.execute('''INSERT INTO samples VALUES
                                        (NULL,%s,%s,%s,NULL,%s)''',
                                        # time  typ  mdsID    values
                                        (timeid, s_type, mdsID, lastID))
        # # self.conn.commit()  # added

    # convenience from old job abstraction layer
    def close(self):
        self.conn.commit()
        self.conn.close()

    def commit(self):
        self.conn.commit()
    ###################

    def update_job(self, jobid, start, end, owner, nids, cmd):
        '''insert end time for job started before
            jobid is: jobid.batchserver-year
            jobid.batchserver must be built from caller, year is appended here.
            for cray, batchserver has to come from config file, is not in alps log file
            start, ownder, nids might contain nonsense, do not use!
        '''
        cyear = time.localtime(end).tm_year 
        for dbjobid in [ jobid+"-"+str(cyear), jobid+"-"+str(cyear-1), jobid ]:
            # old self.c.execute(''' UPDATE jobs SET t_end=%s WHERE jobid=%s AND t_start=%s''', (end, jobid, start))
            self.c.execute('''
                            UPDATE
                                jobs
                            SET
                                t_end=%s
                            WHERE
                                jobid=%s
                            ''', (end, dbjobid))
            
            if self.c.rowcount>0:
                # self.conn.commit()  # added
                return


    def insert_job(self, jobid, start, end, owner, nids, cmd):
        '''insert complete job with all dependencies'''

        # new: we add year to jobid to make it unique
        cyear = time.localtime(start).tm_year 
        jobid = jobid + "-" + str(cyear)

        #print jobid, start, end, owner, nids, cmd
        # check if job is already in DB
        #  OLD self.c.execute(''' SELECT jobid FROM jobs WHERE jobid = %s AND t_start = %s''', (jobid, start))
        self.c.execute('''
                        SELECT
                            jobid
                        FROM
                            jobs
                        WHERE
                            jobid = %s
                        ''', (jobid, ))
        
        if not self.c.fetchone():
            # check if user is already in DB
            self.c.execute('''SELECT id
                              FROM users
                              WHERE users.username = %s''', (owner,))
            r = self.c.fetchone()
            if r:
                userid = r[0]
            else:
                self.c.execute('''INSERT INTO users
                                  VALUES (NULL,%s)''', (owner,))
                userid = self.c.lastrowid
            self.c.execute('''INSERT INTO jobs
                              VALUES
                                (NULL,%s,%s,%s,%s,%s,%s,NULL,NULL,NULL)''',
                              (jobid, start, end, userid, nids, cmd))
            jobkey = self.c.lastrowid
            # nodes - expand cray name compression with ranges
            nl = []
            for node in nids.split(','):
                if "-" in node:
                    (s, e) = node.split("-")
                    # in case a hostname is not NUMERIC-NUMERIC,
                    # we assume it is just a hostname with a - and append it
                    try:
                        nl.extend(map(str, range(int(s), int(e) + 1)))
                    except:
                        nl.append(node)
                else:
                    nl.append(node)
            # insert into db
            # check if node is already in DB
            for node in nl:
                self.c.execute('''SELECT id
                                  FROM nids
                                  WHERE nid = %s''', (node,))
                r = self.c.fetchone()
                if r:
                    nodeid = r[0]
                else:
                    self.c.execute('''INSERT INTO nids
                                      VALUES (NULL,%s)''', (node,))
                    nodeid = self.c.lastrowid
                self.c.execute('''INSERT INTO nodelist
                                  VALUES (NULL,%s,%s)''', (jobkey, nodeid))
            # self.conn.commit()  # added
