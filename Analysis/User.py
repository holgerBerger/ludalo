'''
Created on 21.03.2014

@author: uwe
'''


class User(object):
    '''
    classdocs
    '''

    def __init__(self, params):
        '''
        Constructor
        '''
        self.Jobs = {}
        self.Name = None
        self.WR_list = []
        self.RD_list = []
        self.RQ_list = []
