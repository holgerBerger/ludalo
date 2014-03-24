'''
Created on 24.03.2014

@author: uwe
'''


class Job(object):
    '''
    classdocs
    '''

    def __init__(self, params):
        '''
        Constructor
        '''
        self.Name = None
        self.User = None
        self.t_Start = None
        self.t_End = None

        self.WR_list = {}
        self.RD_list = {}
        self.RQ_list = {}
