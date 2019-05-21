#!/usr/bin/env python

import sys

with open(sys.argv[1]) as f:
    s = 0
    for d in f.readlines():
        s += int(d)
    print(s)
