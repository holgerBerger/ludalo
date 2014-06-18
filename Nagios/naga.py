#!/usr/bin/env python
'''
Created on 18.06.2014

@author: uwe
'''
import argparse
import time
import sys
import MySQLdb
# ----------- Setup -----------------

dbhost = ''
dbuser = ''
dbpassword = ''
dbname = ''


def check_timestamps(a):
    # checks last timestamps if 10 timestamps given it and all
    # in range of 60s then it returns 10.0 in adition it checks how long
    # the last timesamp is away. so anything between 9.001 and 10.0 are ok
    t_total = 0
    for i in range(len(a) - 1):
        t_total = t_total + (a[i] - a[i + 1])

    t_total = t_total + time.time() - a[0]
    return float(t_total) / 60


def get_last_timestamp(fs):
    conn = MySQLdb.connect(
                           passwd=dbpassword,
                           db=dbname,
                           host=dbhost,
                           user=dbuser)
    c = conn.cursor()
    query = '''
                select distinct
                    timestamps.c_timestamp
                from
                    timestamps,
                    targets,
                    filesystems,
                    ost_values
                where
                    targets.fsid = filesystems.id
                        and ost_values.timestamp_id = timestamps.id
                        and ost_values.target = targets.id
                        and filesystems.filesystem = %s
                order by timestamps.c_timestamp desc
                limit 10;
            '''
    c.execute(query, (fs,))
    query_result = c.fetchall()
    return query_result

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("-fs", "--filesystem",
                        help="test timestamp for one filesystem", type=str)

    args = parser.parse_args()
    if args.filesystem:
        # connect to db
        # get last time stamp for fs
        a = get_last_timestamp(args.filesystem)
        # compare timestamps with
        t_since = check_timestamps(a)
        # last 10 min timestamps between 9 min and 10.5 min okey
        if (t_since > 9) and (t_since < 10.5):
            print 'OK - db-time=', t_since
            sys.exit(0)

        elif (t_since > 10.6) and (t_since < 12):
            print 'WARNING - db-time=', t_since
            sys.exit(1)

        else:
            print 'CRITICAL - db-time=', t_since
            sys.exit(2)

#    elif args.user:
#        print 'user=', args.user
    else:
        parser.print_help()
