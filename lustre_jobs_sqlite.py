
# helper to add data to sqlite database, using some preexisting tables
# create_tables only creates new tables for this routine
# uses NIDS table created by data importer.

# this routine imports names as coming from batch system,
# so in case of torque, it is hostnames, not IP addresses.

import sqlite3



def create_tables(c):
  c.execute('''CREATE TABLE IF NOT EXISTS jobs (id integer primary key asc, 
                                  jobid text, 
                                  start integer, 
                                  end integer, 
                                  owner integer,
                                  nodelist text,
                                  cmd text
                                  )''')
  c.execute('''CREATE TABLE IF NOT EXISTS users (id integer primary key asc, 
                                  username text
                                  )''')
  c.execute('''CREATE TABLE IF NOT EXISTS nodelist (id integer primary key asc, 
                                  job integer,
                                  nid integer
                                  )''')
  c.execute('''CREATE INDEX IF NOT EXISTS jobid_index ON jobs (jobid,start,end,owner)''')


def insert_job(c, jobid, start, end, owner, nids, cmd):
  print jobid, start, end, owner, nids, cmd
  # check if job is already in DB
  c.execute('''SELECT jobid FROM jobs WHERE jobid = ?''',(jobid,))
  if not c.fetchone():
    # check if user is already in DB
    c.execute('''SELECT id FROM users WHERE users.username = ?''',(owner,))
    r=c.fetchone()
    if r:
      userid=r[0]
    else:
      c.execute('''INSERT INTO users VALUES (NULL,?)''',(owner,))
      userid=c.lastrowid
    c.execute('''INSERT INTO jobs VALUES (NULL,?,?,?,?,?,?)''',(jobid,start,end,userid,nids,cmd))
    # nodes - expand cray name compression with ranges 
    nl=[]
    for node in nids.split(','):
      if "-" in node:
        (s,e) = node.split("-")
        try:  # in case a hostname is not NUMERIC-NUMERIC, we assume it is just a hostname with a - and append it
          nl.extend(map(str,range(int(s),int(e)+1)))
        except:
          nl.append(node)
      else:
        nl.append(node)
    # insert into db
    # check if node is already in DB
    for node in nl:
      c.execute('''SELECT id FROM nids WHERE nid = ?''',(node,))
      r=c.fetchone()
      if r:
        nodeid=r[0]
      else:
        c.execute('''INSERT INTO nids VALUES (NULL,?)''',(node,))
        nodeid=c.lastrowid
      c.execute('''INSERT INTO nodelist VALUES (NULL,?,?)''',(jobid,nodeid))
        
