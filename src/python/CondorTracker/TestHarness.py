#!/usr/bin/env python

import logging
import os
import sys
import getopt
import logging
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

import CondorTracker.Trackers
from CondorTracker.Registry import retrieveTracker


if __name__ == '__main__':
    usage = "Usage: TestHarness.py --tracker=<TrackerPlugin>\n"

    valid = ['tracker=']
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", valid)
    except getopt.GetoptError, ex:
        print usage
        print str(ex)
        sys.exit(1)

        
    tracker = None
    for opt, arg in opts:
        if opt == "--tracker":
            tracker = arg


    trackerInst = retrieveTracker(tracker)

    trackerInst()
    
    
