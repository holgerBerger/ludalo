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
        raise NotImplementedError()

    def addJob(self, jobID, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()

    def addGlobal(self, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()

    def addMDS(self, MDS_name, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()

    def addOSS(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()

    def addMDT(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()

    def addOST(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        raise NotImplementedError()

    def insert_nids(self, server, timestamp, source, nidvals):
        raise NotImplementedError()

    def insert_nids_server(self, server, nids):
        raise NotImplementedError()
