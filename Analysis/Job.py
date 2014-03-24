'''
Created on 24.03.2014

@author: uwe.schilling[at]hlrs.de
'''


class Job(object):
    '''
    classdocs
    '''

    def __init__(self, JobName, WR_dict, WQ_dict, RD_dict, RQ_dict):
        '''
        Constructor
        '''
        tmp_list = WR_dict.keys()

        self.Name = JobName
        self.t_Start = min(tmp_list)
        self.t_End = max(tmp_list)

        self.nidList = []

        self.WR_dict = WR_dict
        self.RD_dict = RD_dict
        self.WQ_dict = WQ_dict
        self.RQ_dict = RQ_dict

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
