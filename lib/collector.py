import multiprocessing
import threading
import Queue
import json
import sys
import time
import subprocess
import database

'''

this modul handles the collection of performance data
special class CollectorInserterPair handels the tupple of inserter and
collector. ther are to each collector a list of inserters.

support for:
- mongo db (full)

partially support for:
- mysql
- sqlite3


'''


class CollectorInserterPair(object):

    """docstring for CollectorInserterPair"""

    def __init__(self, ssh, cfg, numberOfInserterPerDatabase):
        super(CollectorInserterPair, self).__init__()

        # pipe to send signals to the collector
        (self.pipeIn, self.pipeOut) = multiprocessing.Pipe()

        # queue to communitcat between collector and inserters
        self.comQueue = multiprocessing.Queue()

        # ssh comando to execute and start collector on remot device
        self.ssh = ssh

        # the connection to the databese
        self.cfg = cfg
        self.numberOfInserterPerDatabase = numberOfInserterPerDatabase

        # all inserter are in this list. more than one inserter per collctor
        self.inserterList = []

        # generate inserter's
        for x in xrange(0, self.numberOfInserterPerDatabase):
            nIns = database.DatabaseInserter(self.comQueue, self.cfg)
            self.inserterList.append(nIns)

        # generate collector
        self.collector = Collector(self.ssh, self.pipeOut, self.comQueue)

    def inserter_is_alive(self):
        for inserter in self.inserterList:
            print inserter
            if not inserter.is_alive():
                return False
        return True

    def collector_is_alive(self):
        return self.collector.is_alive()

    def inserter_reconnect(self):
        # find crashed
        for inserter in self.inserterList[:]:
            if not inserter.is_alive():
                inserter.shutdown()
                self.inserterList.remove(inserter)
                print 'generating new inserter'
                newInserter = database.DatabaseInserter(
                    self.comQueue, self.cfg)
                self.inserterList.append(newInserter)

    def collector_reconnect(self):
        if not self.collector.is_alive():
            print 'new collector... old is not alive.', self.collector.name
            self.collector.shutdown()
            del self.collector
            self.collector = Collector(self.ssh, self.pipeOut, self.comQueue)

    def collect(self, insertTimestamp):
        self.pipeIn.send(insertTimestamp)


class AsynchronousFileReader(threading.Thread):

    '''
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''

    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        '''The body of the tread: read lines and decode json
        then put them on the queue.'''
        # print 'AsynchronousFileReader RUN','pid',os.getpid(),self.name,
        # 'ppid',os.getppid(),'tid',gettid()

        for line in iter(self._fd.readline, ''):
            # if not json print the exeption and the string
            try:
                # print "inserted into queue:" ,line
                self._queue.put(json.loads(line))
            except Exception, e:
                # print "inserted into queue:" ,line
                self._queue.put(line)
                print e

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()


class DummyCollector(multiprocessing.Process):

    """docstring for DummyCollector"""

    def __init__(self, ip, sOut, oIn, mds=1, ost=2, nid=10):
        super(DummyCollector, self).__init__()
        self.ip = ip
        self.sOut = sOut
        self.oIn = oIn
        self.mds = mds
        self.ost = ost
        self.nid = nid

        # Launch Tread
        self.start()
        print 'created DUMMY', self.name

    def run(self):
        '''
        Consume standard output and standard error of
        a subprocess asynchronously without risk on deadlocking.
        '''
        print 'started DUMMY:', self.name

        # main loop
        while True:
            ts = self.sOut.recv()
            jObject = self.sendRequest()
            self.oIn.send((ts, jObject))

    def sendRequest(self):
        return self.getDummyData()

    def getDummyData(self):

        data = {}

        mdsNames = []
        ostNames = []
        nidNames = []
        ostValues = [81680085, 81680085, 81680085, 81680085]

        # Please append mds to map !!!

        # mdsValues = 81680085

        for x in xrange(1, self.mds):
            mdsNames.append('dummyfs-MDS_' + '{0:06}'.format(x))  # DUMMY-1

        for x in xrange(1, self.ost):
            ostNames.append('dummyfs-OST_' + '{0:06}'.format(x))  # DUMMY-1

        for x in xrange(1, self.nid):
            # DUMMY-1
            nidNames.append('Nid_DUMMY-' + '{0:06}'.format(x) + '@alpha')

        for ost in ostNames:
            tmp = {}
            for nid in nidNames:
                tmp[str(nid)] = ostValues
            data[str(ost)] = tmp

        # self.insertQueue.put(json.dumps(data))
        return data


class Collector(multiprocessing.Process):

    '''
        This class is to manage connections over ssh and copy
        the real collector to the machines over scp
        this create 2 more Threads one
    '''

    def __init__(self, ip, sOut, queue):
        super(Collector, self).__init__()
        self.ip = ip
        self.command = ['ssh', '-C', self.ip, '/tmp/collector']
        self.out = sys.stdout
        self.sOut = sOut
        self.queue = queue

        # use this to end job from outside!
        self.exit = multiprocessing.Event()

        # Copy collector
        subprocess.call(['scp', 'collector', ip + ':/tmp/'])
        # Launch Tread
        self.start()
        print 'created', self.name

    def run(self):
        '''
        Consume standard output and standard error of
        a subprocess asynchronously without risk on deadlocking.
        '''

        # Launch the command as subprocess.
        self.process = subprocess.Popen(
            self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Launch the asynchronous readers of the process' stdout and stderr.
        self.stdout_queue = Queue.Queue()
        self.stdout_reader = AsynchronousFileReader(
            self.process.stdout, self.stdout_queue)

        self.stdout_reader.start()
        self.stderr_queue = Queue.Queue()
        self.stderr_reader = AsynchronousFileReader(
            self.process.stderr, self.stderr_queue)

        self.stderr_reader.start()

        try:
            self.name = self.stdout_queue.get(True, 10).rstrip()
        except Exception, e:
            print 'got no correct name from ssh', e, self.name

        print 'started:', self.name

        # Check the queues if we received some output (until there is nothing more
        # to get).
        while not self.stdout_reader.eof() or not self.stderr_reader.eof():

            # exit if demanded
            if self.exit.is_set():
                print 'exiting collector', self.name
                break

            # Show what we received from standard output.

            # wait for signal to send request
            ts = self.sOut.recv()  # this blocks until a send from main
            print self.name, 'getting send from main start collect'
            self.sendRequest()

            while self.stdout_queue.empty():
                time.sleep(0.1)

            while not self.stdout_queue.empty():
                # print "QL", self.stdout_queue.qsize()
                line = self.stdout_queue.get()
                # queue for inserter
                self.queue.put((ts, line))
            print self.name + 'inserter queue len:', self.queue.qsize()

            # Show what we received from standard error.
            while not self.stderr_queue.empty():
                line = self.stderr_queue.get()
                print self.name + 'Received line on standard error: ' + repr(line)

        # Let's be tidy and join the threads we've started.
        self.stdout_reader.join()
        self.stderr_reader.join()

        # Close subprocess' file descriptors.
        self.process.stdout.close()
        self.process.stderr.close()

    def shutdown(self):
            self.exit.set()

    def sendRequest(self):
        # getting data form collector
        self.process.stdin.write('\n')
