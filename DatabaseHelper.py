'''
Created on 21.01.2014

@author: uwe schilling
'''
from SQLiteObject import SQLiteObject
from RRDToolObject import RRDToolObject
import datetime
import time


class DatabaseHelper(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.databases = set()

    def addSQLite(self, fileName):
        self.databases.add(SQLiteObject(fileName))

    def addRRDB(self, folder):
        self.databases.add(RRDToolObject(folder))

    def addUser(self, userName, timeStamp, WR_MB, RD_MB, REQS):
        for db in self.databases:
            db.addUser(userName, timeStamp, WR_MB, RD_MB, REQS)

    def addJob(self, jobID, timeStamp, WR_MB, RD_MB, REQS):
        for db in self.databases:
            db.addJob(jobID, timeStamp, WR_MB, RD_MB, REQS)

    def addGlobal(self, timeStamp, WR_MB, RD_MB, REQS):
        for db in self.databases:
            db.addGlobal(timeStamp, WR_MB, RD_MB, REQS)

    def addMDS(self, MDS_name, timeStamp, WR_MB, RD_MB, REQS):
        for db in self.databases:
            db.addMDS(MDS_name, timeStamp, WR_MB, RD_MB, REQS)

    def addOSS(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        for db in self.databases:
            db.addOSS(OSS_name, timeStamp, WR_MB, RD_MB, REQS)

    def addMDT(self, MDT_name, timeStamp, WR_MB, RD_MB, REQS):
        for db in self.databases:
            db.addOSS(MDT_name, timeStamp, WR_MB, RD_MB, REQS)

    def addOST(self, OST_name, timeStamp, WR_MB, RD_MB, REQS):
        for db in self.databases:
            db.addOSS(OST_name, timeStamp, WR_MB, RD_MB, REQS)

    def insert_nids(self, server, timestamp, source, nidvals):
        for db in self.databases:
            db.insert_nids(self, server, timestamp, source, nidvals)

    def insert_nids_server(self, server, nids):
        raise NotImplementedError()
#------------------------------------------------------------------------------

    def dateToTimeStamp(self, year, month, day, houer, minute):
        ''' convert from year day month to time stamp '''
        dateTimeInput = datetime.datetime(year, month, day, houer, minute)
        timeStamp = time.mktime(dateTimeInput.timetuple())
        timeStamp = int(timeStamp)
        return timeStamp
#------------------------------------------------------------------------------

    def timeStampToDate(self, timeStamp):
        ''' converts form time stamp to year day month '''
        return datetime.datetime.fromtimestamp(
                                float(timeStamp)).strftime('%Y-%m-%d %H:%M:%S')
#------------------------------------------------------------------------------
