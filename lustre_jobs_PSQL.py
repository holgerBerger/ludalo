

# helper to add data to MySQL database, using some preexisting tables
# create_tables only creates new tables for this routine
# uses NIDS table created by data importer.

# this routine imports names as coming from batch system,
# so in case of torque, it is hostnames, not IP addresses.

import psycopg2


class DB:
  def __init__(self, dbname=None):
    self.conn = psycopg2.connect("dbname=lustre user=berger")
    self.c = self.conn.cursor()

  def close(self):
    self.conn.commit()
    self.conn.close()

  def create_tables(self):
    self.c.execute('''CREATE TABLE IF NOT EXISTS jobs (id serial primary key, 
                                    jobid varchar(30), 
                                    starttime integer, 
                                    endtime integer, 
                                    owner integer,
                                    nodelist text,
                                    cmd text
                                    )''')
    self.c.execute('''CREATE TABLE IF NOT EXISTS users (id serial primary key, 
                                    username text
                                    )''')
    self.c.execute('''CREATE TABLE IF NOT EXISTS nodelist (id serial primary key, 
                                    job integer,
                                    nid integer
                                    )''')
    try:
      self.c.execute('''CREATE INDEX jobid_index ON jobs (jobid,starttime,endtime,owner)''')
    except:
      self.conn.rollback()
    try:
      self.c.execute('''CREATE INDEX nodelist_index ON nodelist (job,nid)''')
    except:
      self.conn.rollback()


  def insert_job(self, jobid, start, end, owner, nids, cmd):
    #print jobid, start, end, owner, nids, cmd
    # check if job is already in DB
    self.c.execute("""SELECT jobid FROM jobs WHERE jobid = %s;""",(jobid,))
    if not self.c.fetchone():
      # check if user is already in DB
      self.c.execute('''SELECT id FROM users WHERE users.username = %s''',(owner,))
      r=self.c.fetchone()
      if r:
        userid=r[0]
      else:
        self.c.execute('''INSERT INTO users VALUES (DEFAULT,%s) RETURNING ID''',(owner,))
        userid=self.c.fetchone()[0]
      self.c.execute('''INSERT INTO jobs VALUES (DEFAULT,%s,%s,%s,%s,%s,%s) RETURNING ID''',(jobid,start,end,userid,nids,cmd))
      jobkey = self.c.fetchone()[0]
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
        self.c.execute('''SELECT id FROM nids WHERE nid = %s''',(node,))
        r=self.c.fetchone()
        if r:
          nodeid=r[0]
        else:
          self.c.execute('''INSERT INTO nids VALUES (DEFAULT,%s) RETURNING ID''',(node,))
          nodeid = self.c.fetchone()[0]
        self.c.execute('''INSERT INTO nodelist VALUES (DEFAULT,%s,%s)''',(jobkey,nodeid))
          
