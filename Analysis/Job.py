'''
Created on 24.03.2014

@author: uwe
'''


class Job(object):
    '''
    classdocs
    '''

    def __init__(self, JobName, UserName):
        '''
        Constructor
        '''
        self.Name = JobName
        self.User = UserName
        self.t_Start = None
        self.t_End = None

        self.nidList = []

        self.WR_list = {}
        self.RD_list = {}
        self.WQ_list = {}
        self.RQ_list = {}

    def getReadList(self):
        'returns read values'
        returnList = []
        for key in self.RD_list.keys():
            returnList.append(self.RD_list.get(key))
        return returnList

    def getWriteList(self):
        'returns read values'
        returnList = []
        for key in self.WR_list.keys():
            returnList.append(self.WR_list.get(key))
        return returnList

    def getReadRequestList(self):
        'returns read values'
        returnList = []
        for key in self.RQ_list.keys():
            returnList.append(self.RQ_list.get(key))
        return returnList

    def getWriteRequestList(self):
        'returns read values'
        returnList = []
        for key in self.WQ_list.keys():
            returnList.append(self.WQ_list.get(key))
        return returnList