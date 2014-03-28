'''
Created on 24.03.2014

@author: uwe.schilling[at]hlrs.de
'''

import time


class Job(object):
    '''
    classdocs
    '''

    def __init__(self, JobName):
        '''
        Constructor
        '''

        self.Name = JobName
        self.t_Start
        self.t_End

        self.nidList = []
        self.perNidMap = {}

        self.WR_dict = {}
        self.RD_dict = {}
        self.WQ_dict = {}
        self.RQ_dict = {}

    def getReadList(self):
        'returns read values'
        returnList = []
        for key in sorted(self.RD_dict.keys()):
            returnList.append(self.RD_dict.get(key))
        return returnList

    def getWriteList(self):
        'returns write values'
        returnList = []
        for key in sorted(self.WR_dict.keys()):
            returnList.append(self.WR_dict.get(key))
        return returnList

    def getReadRequestList(self):
        'returns read request values'
        returnList = []
        for key in sorted(self.RQ_dict.keys()):
            returnList.append(self.RQ_dict.get(key))
        return returnList

    def getWriteRequestList(self):
        'returns write request values'
        returnList = []
        for key in sorted(self.WQ_dict.keys()):
            returnList.append(self.WQ_dict.get(key))
        return returnList

    def getName(self):
        return self.Name

    def getEndTime(self):
        if not self.isEnded():
            t_End = time.time()
        else:
            t_End = self.t_End
        return t_End

    def getDuration(self):
        t_End = self.getEndTime()
        return  t_End - self.t_Start

    def isEnded(self):
        if self.t_End < 0:
            return False
        else:
            return True

    def isValid(self):
        returnValue = False
        minMinutes = 15 * 60
        duration = self.getDuration()
        if duration > minMinutes:
            returnValue = True
        return returnValue

    def addNidName(self, nidName):
        self.nidList.append(nidName)

    def insertValusToDict(self, dict_org, dict_insert):
        if not dict_insert:
            dict_insert = dict_org
        else:
            dict_org = dict(dict_org)
            for key in dict_org.keys():
                insert = dict_insert(key, 0) + dict_org.get(key)
                dict_insert[key] = insert
        return dict_insert

    def add_Values(self, timeMapRB, timeMapWB, timeMapRIO, timeMapWIO, nidName):

        self.perNidMap[nidName] = (timeMapRB, timeMapWB, timeMapRIO, timeMapWIO)
        self.nidList.append(nidName)

        self.WR_dict = self.insertValusToDict(self.WR_dict, timeMapWB)
        self.WQ_dict = self.insertValusToDict(self.WQ_dict, timeMapWIO)
        self.RD_dict = self.insertValusToDict(self.RD_dict, timeMapRB)
        self.RQ_dict = self.insertValusToDict(self.RQ_dict, timeMapRIO)
