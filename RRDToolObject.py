'''
Created on 21.01.2014

@author: uwe schilling
'''
from AbstractDB import AbstractDB
import rrdtool
import os


class RRDToolObject(AbstractDB):
    '''
    classdocs
    '''

    def __init__(self, folder):
        '''
        Constructor
        '''
        self.folder = folder

#------------------------------------------------------------------------------

    def addUser(self, userName, timeStamp, WR_MB, RD_MB, REQS):
        fileRRD = str(self.folder) + '/' + str(userName) + '.rrd'
        self._addElement(userName, timeStamp,
                         WR_MB, RD_MB, REQS, fileRRD)

#------------------------------------------------------------------------------

    def addJob(self, jobID, timeStamp, WR_MB, RD_MB, REQS):
        fileRRD = str(self.folder) + '/' + str(jobID) + '.rrd'
        self._addElement(jobID, timeStamp,
                         WR_MB, RD_MB, REQS, fileRRD)
#------------------------------------------------------------------------------

    def addGlobal(self, timeStamp, WR_MB, RD_MB, REQS):
        dbName = 'global'
        fileRRD = str(self.folder) + '/' + str(dbName) + '.rrd'
        self._addElement(dbName, timeStamp,
                         WR_MB, RD_MB, REQS, fileRRD)
#------------------------------------------------------------------------------

    def addMDS(self, MDS_name, timeStamp, WR_MB, RD_MB, REQS):
        fileRRD = str(self.folder) + '/' + str(MDS_name) + '.rrd'
        self._addElement(MDS_name, timeStamp,
                         WR_MB, RD_MB, REQS, fileRRD)
#------------------------------------------------------------------------------

    def addOSS(self, OSS_name, timeStamp, WR_MB, RD_MB, REQS):
        fileRRD = str(self.folder) + '/' + str(OSS_name) + '.rrd'
        self._addElement(OSS_name, timeStamp,
                         WR_MB, RD_MB, REQS, fileRRD)
#------------------------------------------------------------------------------

    def _addElement(self, name, timeStamp, WR_MB, RD_MB, REQS, fileRRD):
        if os.path.exists(fileRRD):
            lastTimeStamp = rrdtool.last(fileRRD)
            if lastTimeStamp >= timeStamp:
                print ('timeStamp is to smal skip: ' +
                       str(name) +
                       ' ' + str(timeStamp))
            else:
                self._addEntry(name, timeStamp, WR_MB, RD_MB, REQS, fileRRD)
        else:
            if not os.path.exists(self.folder):
                os.mkdir(self.folder)
            self._generateRRD(name, timeStamp, WR_MB, RD_MB, REQS, fileRRD)
#------------------------------------------------------------------------------

    def _addEntry(self, name, timeStamp, WR_MB, RD_MB, REQS, fileRRD):
        updateString = (str(timeStamp) +
                        ':' + str(WR_MB) +
                        ':' + str(RD_MB) +
                        ':' + str(REQS) +
                        ':' + str(WR_MB) +
                        ':' + str(RD_MB) +
                        ':' + str(REQS))
        #print updateString
        rrdtool.update(str(fileRRD), updateString)
#------------------------------------------------------------------------------

    def _generateRRD(self, name, timeStamp, WR_MB, RD_MB, REQS, fileRRD):
        timeRealStart = int(timeStamp) - 1
        rrdStart = '-b ' + str(timeRealStart)

        ''' xff The xfiles factor defines what part of a consolidation
        interval may be made up from *UNKNOWN* data while the consolidated
        value is still regarded as known. It is given as the ratio of allowed
        *UNKNOWN* PDPs to the number of PDPs in the interval.
         Thus, it ranges from 0 to 1 (exclusive).'''
        xff = 0.5  # default 0.5

        ''' steps defines how many of these primary data points are
        used to build a consolidated data point which then
        goes into the archive. '''
        steps = 1  # 1min used to build one point

        ''' rows defines how many generations of data values are kept
        in an RRA. Obviously, this has to be greater than zero.'''
        rows = 1440  # 0ne day

        # Absulut
        ds0 = 'DS:WR_MB_GAUGE:GAUGE:90:U:U'
        ds1 = 'DS:RD_MB_GAUGE:GAUGE:90:U:U'
        ds2 = 'DS:REQS_GAUGE:GAUGE:90:U:U'

        # Speed
        ds3 = 'DS:WR_MB:ABSOLUTE:90:U:U'
        ds4 = 'DS:RD_MB:ABSOLUTE:90:U:U'
        ds5 = 'DS:REQS:ABSOLUTE:90:U:U'

        rrdtool.create(str(fileRRD), rrdStart, '-s 60',
                       ds0, ds1, ds2,
                       ds3, ds4, ds5,
                        # stor 1 minutes steps 7 days = 4320 values per ds
                       ('RRA:MAX:' +
                            str(xff) + ':' +
                            str(steps) + ':' +
                            str(rows * 7)),
                       # stor 5 minutes steps 7 days = 2016 values per ds
                       ('RRA:AVERAGE:' +
                            str(xff) + ':' +
                            str(steps * 5) + ':' +
                            str(2016)),
                       # stor 1 houer steps 84 days = 2016 values per ds
                       ('RRA:AVERAGE:' +
                            str(xff) + ':' +
                            str(steps * 60) + ':' +
                            str(2016))
                        )
        self._addEntry(name, timeStamp, WR_MB, RD_MB, REQS, fileRRD)
        '''except rrdtool.error:
            print 'File exists: ' + str(fileRRD)'''
