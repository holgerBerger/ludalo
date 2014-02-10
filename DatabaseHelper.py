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

    def closeConnection(self):
        for db in self.databases:
            db.closeConnection

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
            
    def insert_timestamp(self, timestamp):
        for db in self.databases:
            db.insert_timestamp(timestamp)
            
    def insert_source(self, source):
        for db in self.databases:
            db.insert_source(source)

    def insert_server(self, server, stype):
        for db in self.databases:
            db.insert_server(server, stype)
            
    def add_nid_server(self, server, nid_name):
        for db in self.databases:
            db.add_nid_server(server, nid_name)
            
    def getNidID(self, server, i):
        for db in self.databases:
            db.getNidID(server, i)

    def insert_nid(self, server, timestamp, source, nidvals, nidid):
        for db in self.databases:
            db.insert_nids(server, timestamp, source, nidvals, nidid)

    def insert_nid_server(self, server, nid_name):
        for db in self.databases:
            db.insert_nids(server, timestamp, source, nidvals)

    def insert_ost_samples(self, il_ost):
        for db in self.databases:
            db.insert_ost_samples(il_ost)

    def insert_mdt_samples(self, il_mdt):
        for db in self.databases:
            db.insert_mdt_samples(il_mdt)
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
