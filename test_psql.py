#!/usr/bin/env python


# small test if SQL is an option, inserts data into a sqlite database
# Holger Berger 2014
#
# TODO
#  - read old values for some hashes from database (to update a database)
#  - insert OST and MDS values as well 
#  - add job data/user mapping
#  - MySQL/PSQL Support??

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
import time
import psycopg2

def drop_tables(c):
  c.execute('''drop TABLE timestamps;''')
  c.execute('''drop TABLE types;''')
  c.execute('''drop TABLE nids;''')
  c.execute('''drop TABLE servers;''')
  c.execute('''drop TABLE sources;''')
  c.execute('''drop TABLE ost_values;''')
  c.execute('''drop TABLE ost_nid_values;''')
  c.execute('''drop TABLE mdt_values;''')
  c.execute('''drop TABLE mdt_nid_values;''')
  c.execute('''drop TABLE samples''')

def create_tables(c):
  c.execute('''CREATE TABLE IF NOT EXISTS timestamps (id serial primary key, time integer);''')
  c.execute('''CREATE TABLE IF NOT EXISTS types (id serial primary key, type text);''')
  c.execute('''CREATE TABLE IF NOT EXISTS nids (id serial primary key, nid text);''')
  c.execute('''CREATE TABLE IF NOT EXISTS servers (id serial primary key, server text);''')
  c.execute('''CREATE TABLE IF NOT EXISTS sources (id serial primary key, source text);''')
  c.execute('''CREATE TABLE IF NOT EXISTS ost_values (id serial primary key, rio integer, rb bigint, wio integer, wb bigint);''')
  c.execute('''CREATE TABLE IF NOT EXISTS ost_nid_values (id serial primary key, rio integer, rb bigint, wio integer, wb bigint);''')
  c.execute('''CREATE TABLE IF NOT EXISTS mdt_values (id serial primary key, reqs integer);''')
  c.execute('''CREATE TABLE IF NOT EXISTS mdt_nid_values (id serial primary key, reqs integer);''')
  c.execute('''CREATE TABLE IF NOT EXISTS samples (id serial primary key, time integer, type integer, source integer, nid integer, vals integer);''')
  c.execute('''CREATE INDEX samples_time_index ON samples (time);''')
  c.execute('''CREATE INDEX time_index ON timestamps (time);''')


class logfile:

  def __init__(self, cursor, filename):

    self.filename = filename
    self.cursor = cursor

    self.globalnidmap = {}
    self.servermap = {}
    self.per_server_nids = {}
    self.timestamps = {}
    self.sources = {}
    self.servertype = {}

    #self.read_globalnids()
    # FIXME read state from DB here
    
  ########################
  def read(self):
    ''' action is HERE'''
    f = open(self.filename,"r")

    #1.0;hmds1;time;mdt;reqs;
    #1.0;hoss3;time;ost;rio,rb,wio,wb;
    for line in f:
      sp = line[:-1].split(";") 
      if line.startswith("#"):
        server = sp[1]
        stype = sp[3]
        self.insert_server(server, stype)
        self.insert_nids_server(server, sp[5:])
      else:
        server = sp[0]
        timestamp = sp[1]
        source = sp[2]
        self.insert_timestamp(timestamp)
        self.insert_source(source)
        self.insert_nids(server, timestamp, source, sp[4:])

  ########################

  def insert_timestamp(self, timestamp):
    if timestamp not in self.timestamps:
      self.cursor.execute('''INSERT INTO timestamps VALUES (DEFAULT,%s) RETURNING ID''',(timestamp,))
      self.timestamps[timestamp]=self.cursor.fetchone()[0]

  def insert_source(self, source):
    if source not in self.sources:
      self.cursor.execute('''INSERT INTO sources VALUES (DEFAULT,%s) RETURNING ID''',(source,))
      self.sources[source]=self.cursor.fetchone()[0]

  def insert_server(self, server, stype):
    if server not in self.per_server_nids:
      print "new server:", server
      self.per_server_nids[server] = []
      self.cursor.execute('''INSERT INTO servers VALUES (DEFAULT,%s) RETURNING ID''',(server,))
      self.servermap[server]=self.cursor.fetchone()[0]
      self.servertype[server]=stype
  
  def insert_nids_server(self, server, nids):
    for nid in nids:
      if nid not in self.globalnidmap:
        self.cursor.execute('''INSERT INTO nids VALUES (DEFAULT,%s) RETURNING ID''',(nid,))
        self.globalnidmap[nid]=self.cursor.fetchone()[0]
      if nid not in self.per_server_nids[server]:
        self.per_server_nids[server].append(nid)

  def insert_nids(self, server, timestamp, source, nidvals):
    stype = self.servertype[server]
    #print server, timestamp, source, stype
    # CREATE TABLE samples (id integer primary key asc, time integer, type integer, source integer, nid integer, vals integer)
    for i in range(len(nidvals)):
      nidid = self.globalnidmap[self.per_server_nids[server][i]]
      timeid = self.timestamps[timestamp]
      sourceid = self.sources[source]
      if nidvals[i]!="":
        if stype == 'ost':
          self.cursor.execute('''INSERT INTO ost_nid_values VALUES (DEFAULT,%s,%s,%s,%s) RETURNING ID''',nidvals[i].split(','))
          id = self.cursor.fetchone()[0]
          self.cursor.execute('''INSERT INTO samples VALUES (DEFAULT,%s,%s,%s,%s,%s)''',(timeid, 0, sourceid, nidid, id))
        if stype == 'mdt':
          self.cursor.execute('''INSERT INTO mdt_nid_values VALUES (DEFAULT,%s) RETURNING ID''',(nidvals[i],))
          id = self.cursor.fetchone()[0]
          self.cursor.execute('''INSERT INTO samples VALUES (DEFAULT,%s,%s,%s,%s,%s)''',(timeid, 1, sourceid, nidid, id))



conn = psycopg2.connect("dbname=lustre user=berger")
cursor = conn.cursor()

drop_tables(cursor)
create_tables(cursor)
conn.commit()

o = logfile(cursor, sys.argv[1])
o.read()

cursor.close()
conn.commit()
conn.close()
