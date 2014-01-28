#!/usr/bin/env python

import sys
import time
import sqlite3

def create_tables(c):
  c.execute('''CREATE TABLE timestamps (id integer primary key asc, time text)''')
  c.execute('''CREATE TABLE types (id integer primary key asc, type text)''')
  c.execute('''CREATE TABLE nids (id integer primary key asc, nid text)''')
  c.execute('''CREATE TABLE servers (id integer primary key asc, server text)''')
  c.execute('''CREATE TABLE sources (id integer primary key asc, source text)''')
  c.execute('''CREATE TABLE ost_values (id integer primary key asc, rio integer, rb integer, wio integer, wb integer)''')
  c.execute('''CREATE TABLE ost_nid_values (id integer primary key asc, rio integer, rb integer, wio integer, wb integer)''')
  c.execute('''CREATE TABLE mdt_values (id integer primary key asc, reqs integer)''')
  c.execute('''CREATE TABLE mdt_nid_values (id integer primary key asc, reqs integer)''')
  c.execute('''CREATE TABLE samples (id integer primary key asc, time integer, type integer, source integer, nid integer, vals integer)''')


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

  def insert_timestamp(self, timestamp):
    if timestamp not in self.timestamps:
      self.cursor.execute('''INSERT INTO timestamps VALUES (NULL,?)''',(timestamp,))
      self.timestamps[timestamp]=self.cursor.lastrowid

  def insert_source(self, source):
    if source not in self.sources:
      self.cursor.execute('''INSERT INTO sources VALUES (NULL,?)''',(source,))
      self.sources[source]=self.cursor.lastrowid

  def insert_server(self, server, stype):
    if server not in self.per_server_nids:
      print "new server:", server
      self.per_server_nids[server] = []
      self.cursor.execute('''INSERT INTO servers VALUES (NULL,?)''',(server,))
      self.servermap[server]=self.cursor.lastrowid
      self.servertype[server]=stype
  
  def insert_nids_server(self, server, nids):
    for nid in nids:
      if nid not in self.globalnidmap:
        self.cursor.execute('''INSERT INTO nids VALUES (NULL,?)''',(nid,))
        self.globalnidmap[nid]=self.cursor.lastrowid
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
          self.cursor.execute('''INSERT INTO ost_nid_values VALUES (NULL,?,?,?,?)''',nidvals[i].split(','))
          id = self.cursor.lastrowid
          self.cursor.execute('''INSERT INTO samples VALUES (NULL,?,?,?,?,?)''',(timeid, 0, sourceid, nidid, id))
        if stype == 'mdt':
          self.cursor.execute('''INSERT INTO mdt_nid_values VALUES (NULL,?)''',(nidvals[i],))
          id = self.cursor.lastrowid
          self.cursor.execute('''INSERT INTO samples VALUES (NULL,?,?,?,?,?)''',(timeid, 1, sourceid, nidid, id))



conn = sqlite3.connect('sqlite.db')
cursor = conn.cursor()

create_tables(conn)

o = logfile(cursor, sys.argv[1])
o.read()

conn.commit()
conn.close()
