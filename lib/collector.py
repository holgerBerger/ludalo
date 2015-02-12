""" @brief      Data Collector class with multiprocessing support
    @details    This modul handles the collection of performance data
                special class CollectorInserterPair handels the tupple
                of inserter and collector. ther are to each collector a
                list of inserters.
                support for:
                    - mongo db (full)

                partially support for:
                    - mysql
                    - sqlite3

    @author     Uwe Schilling uweschilling[at]hlrs.de
"""


import multiprocessing
import threading
import Queue
import json
import sys
import time
import subprocess
import database


class CollectorInserterPair(object):

    """ @brief      This class connectes a collector with inserters
        @details    To hold reference of this tuple is this class necessery.
                    it also provides funktions to controle this processes.
    """

    def __init__(self, ssh, cfg, numberOfInserterPerDatabase, sharedDict):
        """ @brief      Init of the Class
            @details    This part builds the \em DatabaseInserter and the
                        \em Collector object to start up the programm
            @param      ssh is the comando to start the remot collecotr
            @param      cfg \em DatabaseConfigurator object for commutication
            @param      numberOfInserterPerDatabase info form config file
            @param      sharedDict shared object
        """
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

        # all inserter are in this list. more than one inserter per collector
        self.inserterList = []

        self.sharedDict = sharedDict

        # generate inserter's
        for x in xrange(0, self.numberOfInserterPerDatabase):
            nIns = database.DatabaseInserter(
                self.comQueue, self.cfg, self.sharedDict)
            self.inserterList.append(nIns)

        # generate collector
        self.collector = Collector(self.ssh, self.pipeOut, self.comQueue)

    def inserter_is_alive(self):
        """ @brief      Test if all inserter are up and running
            @return     True if alive
                        False if dead
        """
        for inserter in self.inserterList:
            if not inserter.is_alive():
                return False
        return True

    def collector_is_alive(self):
        """ @brief      Test if collector is alive
            @return     True if alive
                        False if dead
        """
        return self.collector.is_alive()

    def inserter_reconnect(self):
        """ @brief      Try to remove dead inserters and restarts them
        """
        # find crashed
        for inserter in self.inserterList[:]:
            if not inserter.is_alive():
                inserter.shutdown()
                self.inserterList.remove(inserter)
                print 'generating new inserter'
                newInserter = database.DatabaseInserter(
                    self.comQueue, self.cfg, self.sharedDict)
                self.inserterList.append(newInserter)

    def collector_reconnect(self):
        """ @brief      Try to remove dead collecotr and restarts them
        """
        if not self.collector.is_alive():
            print 'new collector... old is not alive.', self.collector.name
            self.collector.shutdown()
            del self.collector
            self.collector = Collector(self.ssh, self.pipeOut, self.comQueue)

    def collect(self, insertTimestamp):
        """ @brief      Sends signal to the collector
            @details    this triggers the collect signal
            @param      insertTimestamp the time when collection start
        """
        if self.comQueue.qsize() < 128:
            self.pipeIn.send(insertTimestamp)
        else:
            print 'queue size is to large, mayby the db server is down. Skip frame:', insertTimestamp

    def shutdown(self):
        print self.name, 'sending shutdown'
        self.collector.shutdown()

        for ins in self.inserterList:
            ins.shutdown()


class AsynchronousFileReader(threading.Thread):

    """ @brief      This handles the terminal read and the json decode
        @details    As part of the collecter it runs in the same process as
                    and decode the json and give it back to the processe via
                    pipe. \em Collector
                    Based on:
                    http://stefaanlippens.net/python-asynchronous-subprocess-pipe-reading
                    Helper class to implement asynchronous reading of a file
                    in a separate thread. Pushes read lines on a queue to
                    be consumed in another thread.
    """

    def __init__(self, fd, queue):
        """ @brief      Class init
            @param      fd      stdin or stderror
            @param      queue   write back queue
        """
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        """ @brief      Therad run Methode
            @details    this should run infinetly to read the lines and decode
                        the json and push the results back in the queue.
        """
        # print 'AsynchronousFileReader RUN','pid',os.getpid(),self.name,
        # 'ppid',os.getppid(),'tid',gettid()

        for line in iter(self._fd.readline, ''):
            # if not json print the exeption and the string
            try:
                # print "inserted into queue:" ,line
                # t1 = time.time()
                self._queue.put(json.loads(line))
                # print 'json decode time:', time.time() - t1
            except Exception, e:
                # print "inserted into queue:" ,line
                self._queue.put(line)
                print e

    def eof(self):
        """ @briefCheck whether there is no more content to expect.
        """
        return not self.is_alive() and self._queue.empty()


class DummyCollector(multiprocessing.Process):

    """ @brief          DummyCollector for db tests.
        @deprecated     not longer suported
    """

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

    """ @brief      This class is to manage connections over ssh
        @details    and copy the real collector to the machines over scp
                    this create 2 more Threads. Stdout and Stderr
    """

    def __init__(self, ip, sOut, queue):
        """ @brief      Class init
            @param      ip      ip to remote comuter
            @param      sOut    Controlling pipe
            @param      queue   queue to inserter
            @see        \em CollectorInserterPair
        """
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
        # print 'created', self.name

    def run(self):
        """ @brief      Process to handle stdout
            @details    Consume standard output and standard error of
                        a subprocess asynchronously without risk on deadlocking.
                        Wait for pipe to send collect signal form main.
                        then trigger remot collecotr to send data as json.
                        get the decoded json from \em AsynchronousFileReader
                        and push it into the inserter queue. \em DatabaseInserter

        """

        # Launch the command as subprocess.
        self.process = subprocess.Popen(
            self.command, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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

        # print 'started:', self.name

        # Check the queues if we received some output (until there is nothing more
        # to get).
        print '  ', self.name, 'Starting loop'
        while not self.stdout_reader.eof() or not self.stderr_reader.eof():

            # exit if demanded
            if self.exit.is_set():
                print 'exiting collector', self.name
                break

            # Show what we received from standard output.
            # print '  ', self.name, 'waiting for collection signal'
            # wait for signal to send request
            ts = self.sOut.recv()  # this blocks until a send from main
            print '  ', self.name, 'inserter queue len:', self.queue.qsize()
            # print self.name, 'getting send from main start collect'
            # print '  ', self.name, 'sending request to C collector'
            self.sendRequest()

            while self.stdout_queue.empty():
                time.sleep(0.1)

            while not self.stdout_queue.empty():
                # print "QL", self.stdout_queue.qsize()
                # print '  ', self.name, 'getting data from C collector'
                line = self.stdout_queue.get()
                # queue for inserter
                # print '  ', self.name, 'append data to inserter queue'
                self.queue.put((ts, line))

            # Show what we received from standard error.
            while not self.stderr_queue.empty():
                line = self.stderr_queue.get()
                print self.name, ' Received line on standard error:', repr(line)

        # Let's be tidy and join the threads we've started.
        self.stdout_reader.join()
        self.stderr_reader.join()

        # Close subprocess' file descriptors.
        self.process.stdout.close()
        self.process.stderr.close()

    def shutdown(self):
        """ @brief      should shutdown process
        """
        self.exit.set()

    def sendRequest(self):
        """ @brief      write a linefeed to the stdin to trigger remote
                        collecotr
        """
        # getting data form collector
        self.process.stdin.write('\n')
