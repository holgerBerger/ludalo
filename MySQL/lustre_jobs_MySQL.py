

# helper to add data to MySQL database, using some preexisting tables
# create_tables only creates new tables for this routine
# uses NIDS table created by data importer.

# this routine imports names as coming from batch system,
# so in case of torque, it is hostnames, not IP addresses.

import MySQLdb
from ConfigParser import ConfigParser
import sys


class DB:
    def __init__(self, dbname=None):
        self.config = ConfigParser()
        try:
            self.config.readfp(open("db.conf"))
        except IOError:
            print "no db.conf file found."
            sys.exit()
        self.dbname = self.config.get("database","name")
        self.dbpassword = self.config.get("database","password")
        self.dbhost = self.config.get("database","host")
        self.dbuser = self.config.get("database","user")
        self.conn = MySQLdb.connect(passwd=self.dbpassword, db=self.dbname, host=self.dbhost, user=self.dbuser)
        self.c = self.conn.cursor()

    def close(self):
        self.conn.commit()
        self.conn.close()

    def create_tables(self):
        '''create tables and indices needed for job insertion'''
        self.c.execute('''CREATE TABLE IF NOT EXISTS
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
                                        reqs_sum bigint
                                        )''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                                users (
                                        id serial primary key,
                                        username text
                                        )''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS
                                nodelist (
                                        id serial primary key,
                                        job integer,
                                        nid integer
                                        )''')
        try:
            self.c.execute('''CREATE INDEX
                                jobid_index
                              ON
                                jobs (jobid,t_start,t_end,owner)''')

            self.c.execute('''CREATE INDEX
                                nodelist_index
                              ON
                                nodelist (job,nid)''')
        except:
            pass

    def update_job(self, jobid, start, end, owner, nids, cmd):
        '''insert end time for job started before'''
        self.c.execute('''UPDATE jobs SET t_end=%s WHERE jobid=%s AND t_start=%s''',(end, jobid, start))

    def insert_job(self, jobid, start, end, owner, nids, cmd):
        '''insert complete job with all dependencies'''
        #print jobid, start, end, owner, nids, cmd
        # check if job is already in DB
        self.c.execute('''SELECT jobid FROM jobs WHERE jobid = %s and t_start = %s''', (jobid,start))
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
