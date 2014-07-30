#!/usr/bin/env python

# data collector for OSS
#
# tested with lustre 1.8.4
# has to work with python 2.4 for redhat 5.x based OSS
#
# collect nr of IO requests and size of IO when asked
# and return as list via RPC, only if any data, otherwise empty list.
# samples are tuples of 4 values #read io request,read bytes,#write io requests, written bytes
# name of ost; sample for ost; sampel for 1. nid; sample for 2. nid; ...
# legend starts with # and can change over time, as nid list changes. The legend contains the names of the nids
# sorted as the samples are.
# nid list changes and ost number changes should be covered. Empty (no activity) nid samples are empty
# empty OST samples (= no IO) are left out

# Holger Berger 2014

from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import SocketServer
import socket
import signal
import os
import time
import sys
import getopt


def mystr(i):
    if i == 0:
        return ''
    else:
        return str(i)


class MDS_RPCSERVER:

    def __init__(self):
        self.old = {}
        self.current = {}

    def get_type(self):
        return "mds"

    def get_hostname(self):
        return socket.gethostname().split(".")[0]

    def get_mdt(self):
        return [x for x in os.listdir("/proc/fs/lustre/mds/") if 'MDT' in x]

    def __get_mdt_data(self, mdt):
        f = open("/proc/fs/lustre/mds/" + mdt + "/stats", "r")
        reqs = 0
        for l in f:
            if "samples" in l:
                reqs += int(l.split()[1])
        f.close()
        return reqs

    def __get_nid_data(self, mdt, nid):
        """get nid data, return 0 for non existing nid"""
        reqs = 0
        try:
            f = open("/proc/fs/lustre/mds/" +
                     mdt + "/exports/" + nid + "/stats", "r")
            for l in f:
                if "samples" in l:
                    reqs += int(l.split()[1])
            f.close()
        except:
            pass
        return reqs

    def get_sample(self):
        nids = {}
        allnids = set()

        result = []

        mdtlist = self.get_mdt()

        # form explanation comment and return as first element in list of
        # results
        explanation = []
        allnids = set()
        for mdt in mdtlist:
            nids[mdt] = [x for x in os.listdir(
                "/proc/fs/lustre/mds/" + mdt + "/exports") if '@' in x]
            allnids = allnids | set(nids[mdt])
        explanation.append("time;mdt;reqs")
        for nid in sorted(allnids):
            explanation.append(nid)
        result.append(";".join(explanation))

        anydata = True
        # form mdt statistics lines and append to results
        for mdt in sorted(mdtlist):
            self.current[mdt] = self.__get_mdt_data(mdt)
            if not self.old.has_key(mdt) or self.current[mdt] - self.old[mdt] > 0:
                for nid in nids[mdt]:
                    key = mdt + "/" + nid
                    if self.current.has_key(key):
                        self.old[key] = self.current[key]
                    self.current[key] = self.__get_nid_data(mdt, nid)

            mdtdata = []
            if self.old.has_key(mdt):
                if self.current[mdt] - self.old[mdt] > 0:
                    anydata = True
                    mdtdata.append(mdt)
                    mdtdata.append(mystr(self.current[mdt] - self.old[mdt]))
                    for nid in sorted(allnids):
                        if self.old.has_key(mdt + "/" + nid):
                            mdtdata.append(
                                mystr(self.current[mdt + "/" + nid] - self.old[mdt + "/" + nid]))
                        else:
                            mdtdata.append(
                                mystr(self.current[mdt + "/" + nid]))
                    result.append(";".join(mdtdata))
                    self.old[mdt] = self.current[mdt]
            else:
                self.old[mdt] = self.current[mdt]

        if anydata:
            return result
        else:
            return ""


#


class OSS_RPCSERVER:

    def __init__(self):
        self.old = {}
        self.current = {}

    def get_type(self):
        return "ost"

    def get_hostname(self):
        return socket.gethostname().split(".")[0]

    def get_osts(self):
        return [x for x in os.listdir("/proc/fs/lustre/obdfilter/") if 'OST' in x]

    def __get_ost_data(self, ost):
        f = open("/proc/fs/lustre/obdfilter/" + ost + "/stats", "r")
        rio = 0
        wio = 0
        rb = 0
        wb = 0
        for l in f:
            if l.startswith("read_bytes"):
                s = l.split()
                rb = int(s[6])
                rio = int(s[1])
            if l.startswith("write_bytes"):
                s = l.split()
                wb = int(s[6])
                wio = int(s[1])
        f.close()
        return (rio, rb, wio, wb)

    def __get_nid_data(self, ost, nid):
        """get nid data, return 0 for non existing nid"""
        rio = 0
        wio = 0
        rb = 0
        wb = 0
        try:
            f = open("/proc/fs/lustre/obdfilter/" +
                     ost + "/exports/" + nid + "/stats", "r")
            for l in f:
                if l.startswith("read_bytes"):
                    s = l.split()
                    rb = int(s[6])
                    rio = int(s[1])
                if l.startswith("write_bytes"):
                    s = l.split()
                    wb = int(s[6])
                    wio = int(s[1])
            f.close()
        except:
            pass
        return (rio, rb, wio, wb)

    def __delta(self, data, old):
        """write out a tuple, write delta and nothing if only zeros"""
        l = [str(data[i] - old[i]) for i in (0, 1, 2, 3)]
        if l != ["0", "0", "0", "0"]:
            return ",".join(l)
        else:
            return ""

    def get_sample(self):
        nids = {}

        ostlist = []

        allnids = set()

        result = []

        ostlist = self.get_osts()

        # form explanation comment and return as first element in list of
        # results
        explanation = []
        allnids = set()
        for ost in ostlist:
            nids[ost] = [x for x in os.listdir(
                "/proc/fs/lustre/obdfilter/" + ost + "/exports") if '@' in x]
            allnids = allnids | set(nids[ost])
        explanation.append("time;ost;rio,rb,wio,wb")
        for nid in sorted(allnids):
            explanation.append(nid)
        result.append(";".join(explanation))

        anydata = True
        # form ost statistics lines and append to results
        for ost in sorted(ostlist):
            self.current[ost] = self.__get_ost_data(ost)
            if not self.old.has_key(ost) or self.current[ost][0] - self.old[ost][0] > 0 or self.current[ost][2] - self.old[ost][2] > 0:
                for nid in nids[ost]:
                    key = ost + "/" + nid
                    if self.current.has_key(key):
                        self.old[key] = self.current[key]
                    self.current[key] = self.__get_nid_data(ost, nid)

            ostdata = []
            if self.old.has_key(ost):
                if self.current[ost][0] - self.old[ost][0] > 0 or self.current[ost][2] - self.old[ost][2] > 0:
                    anydata = True
                    ostdata.append(ost)
                    ostdata.append(
                        self.__delta(self.current[ost], self.old[ost]))
                    for nid in sorted(allnids):
                        if self.old.has_key(ost + "/" + nid):
                            ostdata.append(
                                self.__delta(self.current[ost + "/" + nid], self.old[ost + "/" + nid]))
                        else:
                            ostdata.append(
                                self.__delta(self.current[ost + "/" + nid], (0, 0, 0, 0)))
                    result.append(";".join(ostdata))
                    self.old[ost] = self.current[ost]
            else:
                self.old[ost] = self.current[ost]

        if anydata:
            return result
        else:
            return []


# Threaded mix-in
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    pass


def signalhandler(signum, frame):
    '''Signalhandler to exit broker'''
    if (signum != signal.SIGUSR1):
        sys.exit()


def rpc_server(version, servertype):
    '''main loop, provides XML-RPC server'''
    signal.signal(signal.SIGTERM, signalhandler)
    signal.signal(signal.SIGUSR1, signalhandler)
    signal.signal(signal.SIGINT, signalhandler)

    if servertype == 'oss':
        rpcserver = OSS_RPCSERVER()
    if servertype == 'mds':
        rpcserver = MDS_RPCSERVER()

    server = AsyncXMLRPCServer(('', 8000), logRequests=False)
    server.register_introspection_functions()
    server.register_instance(rpcserver)

    # we add a endless loop here to handle interrupts, we want to be able to
    # quit, other signals should be resumed
    while (True):
        try:
            server.serve_forever()
        except SystemExit:
            server.server_close()
            break
        except:
            pass


if __name__ == "__main__":
    while True:
            # wait until lustre is mounted, otherwise wait 60s
        try:
            line = open("/proc/fs/lustre/version", "r").readline()[:-1]
            version = line.split()[1]
        except:
            print "no lustre mounted. Waiting..."
            time.sleep(60)
            continue

        servertype = ""
        if os.path.exists("/proc/fs/lustre/mds"):
            servertype = "mds"
        if os.path.exists("/proc/fs/lustre/ost"):
            servertype = "oss"

        if servertype == "":
            print "unknown server type. Waiting..."
            time.sleep(60)
            continue

        print "Running on %s of lustre version %s" % (servertype, version)
        rpc_server(version, servertype)
