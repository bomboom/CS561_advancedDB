#!/usr/bin/env python
import re
import sys
import os


#get filename

filePath = os.environ['map_input_file'] 
fileName = os.path.split(filePath)[-1]

for line in sys.stdin:
    line = line.strip()
    splits = line.split(',')

    if fileName == "Customers.csv":
        ID = splits[0]
        name = splits[1]
        #filtter country code == 5
        if re.match('5',splits[3]):
            print '%s\t%s\t%s'% (ID, '0', name)
    else:
        ID = splits[1]
        print '%s\t%s\t%s'% (ID,'1', 1)
