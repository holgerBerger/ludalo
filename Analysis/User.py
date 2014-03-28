'''
Created on 21.03.2014

@author: uwe.schilling[at]hlrs.de
'''

from Job import Job


class User(object):
    '''
    classdocs
    '''

    def __init__(self, userName):
        '''
        Constructor
        '''
        self.Jobs = {}
        self.Name = userName
        self.WR_list = []
        self.RD_list = []
        self.RQ_list = []
        self.WQ_list = []

    def addJob(self, jobObject):
        jobObject = Job(jobObject)
        jobObject_w_list = jobObject.getWriteList()
        jobObject_r_list = jobObject.getReadList()
        jobObject_rq_list = jobObject.getReadRequestList()
        jobObject_wq_list = jobObject.getWriteRequestList()

        for w in jobObject_w_list:
            self.WR_list.append(w)
        for r in jobObject_r_list:
            self.RD_list.append(r)
        for rq in jobObject_rq_list:
            self.RQ_list.append(rq)
        for wq in jobObject_wq_list:
            self.WQ_list.append(wq)

        self.Jobs[jobObject.getName()] = jobObject

    def __str__(self):
        return 'user name = ', self.Name

    def __repr__(self):
        return self.__str__()
