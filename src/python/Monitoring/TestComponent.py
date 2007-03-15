#!/usr/bin/env python
"""
Util to test the Monitoring component startup for development by
starting the component as an interactive process so that you can get
stdout/stderr etc

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import sys
import getopt

# MergeSensor
from Monitoring.MonitoringComponent import MonitoringComponent

##########################################################################
# usage message
##########################################################################

def usage():
    """
    __usage__
    
    display usage message
    
    """
    
    usageMsg = \
    """
    Usage: TestComponent.py <opts>
    Start this component interactively for development testing

    """
    
    print usageMsg

# arguments
argsDict = {"PollInterval" : 3600,
            }

# options
valid = ["PollInterval=",
         ]

# get options
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

# start component
comp = MonitoringComponent(**argsDict)
comp.startComponent()

