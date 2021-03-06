#!/usr/bin/env python
"""
Util to test the RM component startup for development by
starting the component as an interactive process so that you can get
stdout/stderr etc

"""

import os
import sys
import getopt

from CondorTracker.CondorTrackerComponent import CondorTrackerComponent

def usage():
    usage = \
    """
    Usage: TestComponent.py <opts>
    Start this component interactively for development testing
    Options:
    PollInterval, JobCache, ComponentDir
    """
    print usage

argsDict = {"PollInterval" : 10,
            "JobCache" : None,
            "ComponentDir" : os.getcwd(),
            }

valid = [ 'PollInterval=', 'JobCache=', 'ComponentDir=', ]

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print str(ex)
    usage()
    sys.exit(1)

for opt, arg in opts:
    argsDict[opt.replace('--', '')] = arg


for key, val in argsDict.items():
    if argsDict[key] == None:
        msg = "Error: Parameter %s not set:\n" % key
        msg += "You must provide the --%s=<value> option" % key
        print msg
        sys.exit(1)


comp = CondorTrackerComponent(**argsDict)
comp.startComponent()
