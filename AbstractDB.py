'''
Created on 21.01.2014

@author: uwe schilling
'''


class AbstractDB(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        return None

    def addUser(self, userName, timeStamp, WR_MB, RD_MB, REQS):
        print self.__class__.__name__
        raise NotImplementedError()

    def addJob(self, jobID, timeStamp, WR_MB, RD_MB, REQS):
        print self.__class__.__name__
        raise NotImplementedError()

    def addGlobal(self, timeStamp, WR_MB, RD_MB, REQS):
        print self.__class__.__name__
        raise NotImplementedError()

    def addMDS(self, MDS_name, timeStamp, WR_MB, RD_MB, REQS):
        print self.__class__.__name__
        raise NotImplementedError()

    def addOSS(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        print self.__class__.__name__
        raise NotImplementedError()

    def addMDT(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        print self.__class__.__name__
        raise NotImplementedError()

    def addOST(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        print self.__class__.__name__
        raise NotImplementedError()

    def insert_timestamp(self, timestamp):
        print self.__class__.__name__
        raise NotImplementedError()

    def insert_source(self, source):
        print self.__class__.__name__
        raise NotImplementedError()

    def insert_server(self, server, stype):
        print self.__class__.__name__
        raise NotImplementedError()

    def add_nid_server(self, server, nid_name):
        print self.__class__.__name__
        raise NotImplementedError()

    def getNidID(self, server, i):
        print self.__class__.__name__
        raise NotImplementedError()

    def insert_nid(self, server, timestamp, source, nidvals, nidid):
        print self.__class__.__name__
        raise NotImplementedError()

    def insert_nis_server(self, server, nid_name):
        print self.__class__.__name__
        raise NotImplementedError()
