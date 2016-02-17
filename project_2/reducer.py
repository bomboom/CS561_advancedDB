#!/usr/bin/env python
import sys

last_id = None
name = None
count = 0
ID = None

for line in sys.stdin:
    line = line.strip()
    splits = line.split('\t')
    ID = splits[0]

    if ID != last_id:
        if splits[1] == '1':
	    count = 0
            _count = int(splits[2])
            count += _count
            last_id = ID
        else:
            if name:
                print '%s\t%s\t%s'% (last_id, name, count)
            count = 0
            last_id = ID
            name = splits[2]
    elif ID == last_id:
        if splits[1] == '1':
            _count = int(splits[2])
            count += _count
        else:
            name = splits[2]

#last id
if name:
    print '%s\t%s\t%s'% (last_id, name, count)

