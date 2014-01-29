

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
                                  username text,
                                  )''')
  c.execute('''CREATE TABLE IF NOT EXISTS nodelist (id integer primary key asc, 
                                  job integer,
                                  nid integer
                                  )''')
  c.execute('''CREATE INDEX jobid_index ON jobs (jobid,start,end,owner)''')


def insert_job(c, jobid, start, end, owner, nids, cmd):
  print jobid, start, end, owner, nids, cmd
