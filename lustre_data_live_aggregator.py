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

    def __init__(self, command, insertQueue, waitTime=60):
        threading.Thread.__init__(self)
        # self._name
        self.command = command
        self.out = sys.stdout
        self.waitTime = waitTime
        self.insertQueue = insertQueue

        self.start()

    def run(self):
        '''
        Example of how to consume standard output and standard error of
        a subprocess asynchronously without risk on deadlocking.
        '''

        # Launch the command as subprocess.
        process = subprocess.Popen(
            self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Launch the asynchronous readers of the process' stdout and stderr.
        stdout_queue = Queue.Queue()
        stdout_reader = AsynchronousFileReader(process.stdout, stdout_queue)
        stdout_reader.start()
        stderr_queue = Queue.Queue()
        stderr_reader = AsynchronousFileReader(process.stderr, stderr_queue)
        stderr_reader.start()

        print 'vor stdout_queue.get()'
        try:
            self.name = stdout_queue.get(True, 10)
        except Exception, e:
            print 'no correct name get form ssh', e
            pass

        print 'started:', self.name

        # time.sleep(60)

        # Check the queues if we received some output (until there is nothing more
        # to get).
        while not stdout_reader.eof() or not stderr_reader.eof():
            t1 = time.time()
            # Show what we received from standard output.
            while not stdout_queue.empty():
                line = stdout_queue.get()

                # Do Stuff!!!!
                self.insertQueue.put(line)
                # print self.name, json.loads(line)
                # print self.name + 'Received line on standard output: ' +
                # repr(line)

            # Show what we received from standard error.
            while not stderr_queue.empty():
                line = stderr_queue.get()
                print self.name + 'Received line on standard error: ' + repr(line)

            process.stdin.write('\n')
            # self.out.flush()
            # Sleep a bit before asking the readers again.
            time.sleep(3)
            t2 = time.time()
            sleepTime = self.waitTime - (t2 - t1)
            time.sleep(sleepTime)

        # Let's be tidy and join the threads we've started.
        stdout_reader.join()
        stderr_reader.join()

        # Close subprocess' file descriptors.
        process.stdout.close()
        process.stderr.close()


if __name__ == '__main__':
    # names and ip-adress
    #cfg = open('collector.cfg', 'r')

    db = DatabeseInserter()

    insertQueue = Queue.Queue()
    c1 = Collector(['ssh', '-C', 'hoss1', '/tmp/collector'], insertQueue, 4)

    counter = 0
    while True:
        if not insertQueue.empty():
            print len(insertQueue)
            # db.insert(insertQueue.get())
            counter += 1
        time.sleep(1)


    # a = {}
    # a = json.loads(cfg.read())
    # c1 = Collector(['python', 'subprozess_test.py'], 'a', 4)
    # c2 = Collector(['python', 'subprozess_test.py'], 'b', 4)
    # blub
    # c1.start()
    # c2.start()
