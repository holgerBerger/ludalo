#!/usr/bin/env python

import sys
import time


def fm(x):
    return ",".join(x)

servers = {}
nrsamples = 0
timestamps = set()
sources = set()
values = 0
nids = 0

f = open(sys.argv[1], "r")

# 1.0;hmds1;time;mdt;reqs;
# 1.0;hoss3;time;ost;rio,rb,wio,wb;

for line in f:
    sp = line[:-1].split(";")
    if line.startswith("#"):
        servers[sp[1]] = sp
        nids = max(nids, len(sp) - 4)
    else:
        nrsamples += 1
        timestamps.add(sp[1])
        sources.add(sp[2])
        for x in sp[4:]:
            if x != "":
                values += 1

f.close()

print "File contains:"
print " samples from servers:", fm(servers.keys())
print " oss servers:", fm([x for x in servers.keys() if servers[x][3] == "ost"])
print " mdt servers:", fm([x for x in servers.keys() if servers[x][3] == "mdt"])
print " sources:", fm(list(sources))
print " #sources:", len(sources)
print " #samples:", nrsamples
print " #time samples:", len(timestamps)
print " #values:", values
print " #max values:", len(timestamps) * len(sources) * nids
print " fill in:", float(values) / float(len(timestamps) * len(sources) * nids) * 100.0, "%"
ts = sorted(list(map(int, timestamps)))
print " first sample:", time.ctime(ts[0])
print " last sample:", time.ctime(ts[-1])
