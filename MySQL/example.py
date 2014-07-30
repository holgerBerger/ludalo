#!/usr/bin/python
import time
import numpy as np
import MySQLdb
import sys
from ConfigParser import ConfigParser


class Benchmark(object):

    def __init__(self):

        # get config
        self.config = ConfigParser()
        try:
            self.config.readfp(open("db.conf"))
        except IOError:
            print "no db.conf file found."
            sys.exit(1)

        # set DB connection
        self.dbname = self.config.get("database", "name")
        self.dbpassword = self.config.get("database", "password")
        self.ip = self.config.get("database", "host")
        self.dbuser = self.config.get("database", "user")
        self.dbport = self.config.get("database", "port")

        # Database connection
        self.conn = MySQLdb.connect(
            passwd=self.dbpassword,
            db=self.dbname,
            host=self.ip,
            port=int(self.dbport),
            user=self.dbuser)

        # Database cursor
        self.c = self.conn.cursor()
#------------------------------------------------------------------------------

    def plainSQL(self, fs):
        # This funktion opereate only on the Database
        query = '''select
                        sum(wb)/(1024*1024*60) as wb_sum
                    from
                        timestamps,
                        ost_values,
                        filesystems,
                        targets
                    where
                        timestamps.id = ost_values.timestamp_id
                            and ost_values.target = targets.id
                            and targets.fsid = filesystems.id
                            and filesystems.filesystem = %s
                    group by timestamp_id
                    order by wb_sum desc
                    limit 1'''

        self.c.execute(query, (fs,))
        result = self.c.fetchall()
        return result[0][0]
#------------------------------------------------------------------------------

    def sqlPyton(self, fs):
        # This funktion use the python max funktion
        query = '''select
                        sum(wb)/(1024*1024*60) as wb_sum
                    from
                        timestamps,
                        ost_values,
                        filesystems,
                        targets
                    where
                        timestamps.id = ost_values.timestamp_id
                            and ost_values.target = targets.id
                            and targets.fsid = filesystems.id
                            and filesystems.filesystem = %s
                    group by timestamp_id'''

        self.c.execute(query, (fs,))
        result = self.c.fetchall()

        # find max
        tmp = max(result)
        return tmp[0]
#------------------------------------------------------------------------------

    def pythonNumpy(self, fs):
        # This funktion use numpy arrays and the numpy max funktion

        query = '''select
                        sum(wb)/(1024*1024*60) as wb_sum
                    from
                        timestamps,
                        ost_values,
                        filesystems,
                        targets
                    where
                        timestamps.id = ost_values.timestamp_id
                            and ost_values.target = targets.id
                            and targets.fsid = filesystems.id
                            and filesystems.filesystem = %s
                    group by timestamp_id'''

        result = self.query_to_npArray(query, (fs,))

        result = np.max(result[:, 0])
        return result
#------------------------------------------------------------------------------

    def query_to_npArray(self, query, options=None):
        ''' execute the query with the given options and returns
            a numpy matrix of the output
        '''
        self.c.execute(query, options)

        # fetchall() returns a nested tuple (one tuple for each table row)
        results = self.c.fetchall()
        if results:
            # 'num_rows' needed to reshape the 1D NumPy array returend
            # by 'fromiter' in other words, to restore original dimensions
            # of the results set
            num_rows = int(self.c.rowcount)

            # recast this nested tuple to a python list and flatten it
            # so it's a proper iterable:
            x = (row[0] for row in results)

            # D is a 1D NumPy array
            D = np.fromiter(iter=x, dtype=np.float_, count=-1)

            # 'restore' the original dimensions of the result set:
            D = D.reshape(num_rows, -1)
            return D
        else:
            return None
#------------------------------------------------------------------------------

    def testIt(self, f, name, fs):
        # This funktion benchmarks other funktions
        timings = []

        for x in xrange(0, 10):
            t1 = time.time()

            result = f(fs)

            t2 = time.time()
            tr = t2 - t1
            timings.append(tr)

        result_time = np.average(timings)
        print timings
        print str(
            'runtime of '
            + str(name)
            + ' methode: '
            + str(result_time)
            + '[sec]\n')

        return result
#------------------------------------------------------------------------------

if __name__ == '__main__':
    fs = 'lnec'

    bm = Benchmark()
    f1 = bm.testIt(bm.plainSQL, 'plain SQL', fs)
    f2 = bm.testIt(bm.sqlPyton, 'SQL and Python', fs)
    f3 = bm.testIt(bm.pythonNumpy, 'Python and Numpy', fs)

    print 'f1=', f1, 'f2=', f2, 'f3=', f3
