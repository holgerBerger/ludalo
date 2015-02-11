""" @brief      This is a server for sharing objects between Processes
    @details    Expose get put and getkeys from a shared dict.
    @author     Holger Berger
"""


from multiprocessing.managers import BaseManager
#dic = {}
#class QueueManager(SyncManager): pass
#QueueManager.register('get_dict', callable=lambda:dic)

globaldict = {}


class RDict(object):

    """ @brief      Shared Dict
    """

    def __init__(self):
        print "called init"
        self.dict = globaldict

    def get(self, name):
        return self.dict[name]

    def put(self, name, value):
        print "called put"
        self.dict[name] = value

    def getkeys(self):
        print self.dict.keys()
        return self.dict.keys()


class MyManager(BaseManager):
    pass
MyManager.register('RDict', RDict)
m = MyManager(address=('', 50000), authkey='ludalo')
s = m.get_server()
s.serve_forever()
