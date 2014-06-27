'''
Created on 27.06.2014

@author: uwe
'''
import sys


if __name__ == '__main__':

    if len(sys.argv) != 2:
        '[File Path]'
        exit(1)

    f = open(sys.argv[1], 'r')
    # first 4 lines are info output

    tmp = 0
    classes = {}
    for line in f:
        if not line.startswith('job') and line[:-2].endswith(']'):
            sp = line[:-2].split(';')
            if not sp[0] in classes:
                classes[sp[0]] = 1
            else:
                classes[sp[0]] = classes[sp[0]] + 1

    orderdKeys = sorted(classes.keys())
    for key in orderdKeys:
        print key, classes[key]
