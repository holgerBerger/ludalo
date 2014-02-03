#!/usr/bin/env python


# small test if SQL is an option, inserts data into a sqlite database
# Holger Berger 2014
#
# TODO
#  - read old values for some hashes from database (to update a database)
#  - insert OST and MDS values as well 
#  - add job data/user mapping
#  - MySQL/PSQL Support??

# fist argument is hosts file for name mapping, following arguments is files

# nice benchmark query is
'''
 SELECT nids.nid FROM samples
   INNER JOIN timestamps
   ON timestamps.time BETWEEN 1390912496 AND 1390912596
    AND samples.time = timestamps.id
 INNER JOIN nids
 ON samples.nid = nids.id;
'''



import sys
import os.path
import time
import sqlite3
from DatabaseHelper import DatabaseHelper

def create_tables(c):
  c.execute('''CREATE TABLE IF NOT EXISTS timestamps (id integer primary key asc, time integer)''')
  c.execute('''CREATE TABLE IF NOT EXISTS nids (id integer primary key asc, nid text)''')
  c.execute('''CREATE TABLE IF NOT EXISTS servers (id integer primary key asc, server text, type text)''')
  c.execute('''CREATE TABLE IF NOT EXISTS sources (id integer primary key asc, source text)''')
  c.execute('''CREATE TABLE IF NOT EXISTS ost_values (id integer primary key asc, rio integer, rb integer, wio integer, wb integer)''')
  c.execute('''CREATE TABLE IF NOT EXISTS mdt_values (id integer primary key asc, reqs integer)''')
  # tables no longer needed - spillte dinto 2 samples_*
  #c.execute('''CREATE TABLE IF NOT EXISTS ost_nid_values (id integer primary key asc, rio integer, rb integer, wio integer, wb integer)''')
  #c.execute('''CREATE TABLE IF NOT EXISTS mdt_nid_values (id integer primary key asc, reqs integer)''')
  #c.execute('''CREATE TABLE IF NOT EXISTS samples (id integer primary key asc, time integer, type integer, source integer, nid integer, vals integer)''')
  c.execute('''CREATE TABLE IF NOT EXISTS samples_ost (id serial primary key, time integer, source integer, nid integer, rio integer, rb bigint, wio integer, wb bigint);''')
  c.execute('''CREATE TABLE IF NOT EXISTS samples_mdt (id serial primary key, time integer, source integer, nid integer, reqs integer);''')

  #c.execute('''CREATE INDEX IF NOT EXISTS samples_time_index ON samples (time)''')
  c.execute('''CREATE INDEX IF NOT EXISTS samples_ost_time ON samples_ost (time)''')
  c.execute('''CREATE INDEX IF NOT EXISTS samples_mdt_time ON samples_mdt (time)''')
  c.execute('''CREATE INDEX IF NOT EXISTS time_index ON timestamps (time)''')


class logfile:

  def __init__(self, cursor, filename, hostfile=None):

    self.filename = filename
    self.cursor = cursor
    
    self.myDB = DatabaseHelper()
    self.myDB.addSQLite('sqlite_new.db')  # change to db name if save...
    
    self.globalnidmap = {}
    self.servermap = {}
    self.per_server_nids = {}
    self.timestamps = {}
    self.sources = {}
    self.servertype = {}
    self.hostfilemap = {}

    self.filesize = os.path.getsize(filename)

    if hostfile:
      self.readhostfile(hostfile)

    self.read_globalnids()
    self.read_servers()
    self.read_sources()
    self.read_timestamps()

  def eta(self,secs):
    if secs<60:
      return "%d secs"%secs
    else:
      return "%dm%2.2ds" % (secs/60, secs%60)
    
  ########################
  def read(self):
    ''' action is HERE'''
    f = open(self.filename,"r")
    counter=0
    starttime = time.time()

    #1.0;hmds1;time;mdt;reqs;
    #1.0;hoss3;time;ost;rio,rb,wio,wb;
    for line in f:
      sp = line[:-1].split(";") 
      if line.startswith("#"):
        server = sp[1]
        stype = sp[3]
        self.insert_server(server, stype)
        self.insert_nids_server_old(server, sp[5:])
        ''' -> preperation
        for nid in sp[5:]:
            self.insert_nids_server(server, nid)'''
      else:
        counter+=1
        if counter%10 == 0:
          duration = (time.time() - starttime)
          fraction = (float(f.tell())/float(self.filesize))
          endtime = duration * (1.0/ fraction) - duration
          print "\rinserted %d records / %d%% ETA = %s"%(counter,int(fraction*100.0), self.eta(endtime)),
        server = sp[0]
        timestamp = sp[1]
        source = sp[2]
        self.insert_timestamp(timestamp)
        # self.myDB.insert_timestamp(timestamp) 
        self.insert_source(source)
        # self.myDB.insert_source(source)
        #  server = sp[0] timestamp = sp[1] source = sp[2]
        self.insert_nids(server, timestamp, source, sp[4:])
        ''' -> preperation
        index = 0
        for nid in sp[4:]:
            nidID = getNidID(server, index)
            self.insert_nid(server, timeStamp, source, nid, nidID):
            index++
        '''


  ########################

  def read_globalnids(self):
    self.cursor.execute('''SELECT * FROM nids;''')
    r = self.cursor.fetchall()
    for (k,v) in r:
      self.globalnidmap[str(v)]=k
    print "read %s old nid mappings" % len(self.globalnidmap)

  def read_sources(self):
    self.cursor.execute('''SELECT * FROM sources;''')
    r = self.cursor.fetchall()
    for (k,v) in r:
      self.sources[str(v)]=k
    print "read %s old sources" % len(self.sources)

  def read_servers(self):
    self.cursor.execute('''SELECT * FROM servers;''')
    r = self.cursor.fetchall()
    for (k,v,t) in r:
      self.servermap[str(v)]=k
      self.per_server_nids[str(v)] = []
      self.servertype[str(v)]=t
      print "known server:",v,t

  def read_timestamps(self):
    self.cursor.execute('''SELECT * FROM timestamps;''')
    r = self.cursor.fetchall()
    for (k,v) in r:
      self.timestamps[str(v)]=k
    print "read %d old timestamps" % len(self.timestamps)



  def readhostfile(self, hostfile):
    try:
      f = open(hostfile, "r")
    except:
      return
    for l in f:
      if not l.startswith('#'):
        sp = l[:-1].split()
        if len(sp)==0: continue
        ip = sp[0]
        name = sp[1]
        self.hostfilemap[ip]=name
    print "read",len(self.hostfilemap),"host mappings"
    f.close()

  def insert_timestamp(self, timestamp):
    if timestamp not in self.timestamps:
      self.cursor.execute('''INSERT INTO timestamps VALUES (NULL,?)''',(timestamp,))
      self.timestamps[timestamp]=self.cursor.lastrowid

  def insert_source(self, source):
    if source not in self.sources:
      self.cursor.execute('''INSERT INTO sources VALUES (NULL,?)''',(source,))
      self.sources[source]=self.cursor.lastrowid

  def insert_server(self, server, stype):
    #if server not in self.per_server_nids: FIXME ???
    if server not in self.servermap:
      print "new server:", server
      self.per_server_nids[server] = []
      self.cursor.execute('''INSERT INTO servers VALUES (NULL,?,?)''',(server,stype))
      self.servermap[server]=self.cursor.lastrowid
      self.servertype[server]=stype

  def insert_nid_server(self, server, one_nid):
      nid = one_nid.split('@')[0] #get only the name
      self.myDB.add_nid_server(server, nid_name)
  
  def insert_nids_server_old(self, server, nids):
    for rnid in nids:
      nid = rnid.split('@')[0]
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

  def insert_nid(self, server, timeStamp, source, nidvals_Tup, nidID):
      ''' methode to insert only one nid value tuple '''
      self.myDB.add_nid_values(server, timeStamp, source, nidvals_Tup, nidID)

  def insert_nids(self, server, timestamp, source, nidvals):
    stype = self.servertype[server]
    #print server, timestamp, source, stype
    # CREATE TABLE samples (id integer primary key asc, time integer, type integer, source integer, nid integer, vals integer)
    il_ost = []
    il_mdt = []
    for i in range(len(nidvals)):
      nidid = self.globalnidmap[self.per_server_nids[server][i]]
      timeid = self.timestamps[timestamp]
      sourceid = self.sources[source]
      if nidvals[i]!="":
        if stype == 'ost':
          temp = [timeid, sourceid, nidid]
          temp.extend(nidvals[i].split(','))
          il_ost.append(temp)
        if stype == 'mdt':
          il_mdt.append((timeid, sourceid, nidid, nidvals[i]))
    self.cursor.executemany('''INSERT INTO samples_ost VALUES (NULL,?,?,?,?,?,?,?)''',il_ost)
    self.cursor.executemany('''INSERT INTO samples_mdt VALUES (NULL,?,?,?,?)''',il_mdt)


   


if __name__ == "__main__":

  if len(sys.argv)<=2 or sys.argv[1] in ["-h", "--help"]:
    print "usage: %s hostmapping logfile ..." % sys.argv[0]
    sys.exit(0)


  conn = sqlite3.connect('sqlite.db')
  cursor = conn.cursor()

  create_tables(conn)

  for filename in sys.argv[2:]:
    o = logfile(cursor, filename, sys.argv[1])
    o.read()

  conn.commit()
  conn.close()
