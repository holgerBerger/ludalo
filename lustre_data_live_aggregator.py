'''
    this programm is based on this thread:
    http://stefaanlippens.net/python-asynchronous-subprocess-pipe-reading
'''


import subprocess
import time
import threading
import Queue
import json
import sys


class DatabeseInserter(object):

    def __init__(self):
        # self.conn
        # self.c
        pass

    def _execute(query, data):
        pass

    def insert(jsonDict):
        pass


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

        for line in iter(self._fd.readline, ''):
            # if not json print the exeption and the string
            try:
                self._queue.put(json.loads(line))
            except Exception, e:
                self._queue.put(line)
                print e
                # print 'in:', line

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()


class Collector(threading.Thread):

    def __init__(self, command, insertQueue):
        threading.Thread.__init__(self)
        self.command = command
        self.out = sys.stdout
        self.insertQueue = insertQueue

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

        self.start()
        print 'created', self.name

    def run(self):
        '''
        Example of how to consume standard output and standard error of
        a subprocess asynchronously without risk on deadlocking.
        '''

        try:
            self.name = self.stdout_queue.get(True, 10)
        except Exception, e:
            print 'no correct name get form ssh', e
            pass

        print 'started:', self.name

        # Check the queues if we received some output (until there is nothing more
        # to get).
        while not self.stdout_reader.eof() or not self.stderr_reader.eof():
            # Show what we received from standard output.
            while not self.stdout_queue.empty():
                line = self.stdout_queue.get()

                # Do Stuff!!!!
                self.insertQueue.put(line)

            # Show what we received from standard error.
            while not self.stderr_queue.empty():
                line = self.stderr_queue.get()
                print self.name + 'Received line on standard error: ' + repr(line)

            # self.out.flush()
            # Sleep a bit before asking the readers again.
            time.sleep(0.1)

        # Let's be tidy and join the threads we've started.
        self.stdout_reader.join()
        self.stderr_reader.join()

        # Close subprocess' file descriptors.
        self.process.stdout.close()
        self.process.stderr.close()


if __name__ == '__main__':

    # read names and ip-adress
    cfg = open('collector.cfg', 'r')
    ips = json.loads(cfg)

    ts_delay = 60
    data = Queue.Queue()     # create dataqueue
    db = DatabeseInserter()  # create DB connection
    sshObjects = []

    # for all ip's creat connections to the collector

    for key in ips.keys():
        command = ['ssh', '-C', ips[key], '/tmp/collector']
        sshObjects.append(Collector(command, data))

    # loop over all connections look if they alive
    while True:
        t_start = time.time()
        for t in sshObjects:
            if not t.t.isAlive():
                pass
                # remove thread from list
                # recover thred....
            else:
                t.sendRequest()
        if not db.isAlive():
            pass
            # recover database connection

        # look at db connectionen if this is alaive
        t_end = time.time()
        insertTimestamp = int(t_end)

        time.sleep(ts_delay - (t_end - t_start))
