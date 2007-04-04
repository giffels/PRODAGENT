#!/usr/bin/env python
"""
Util to test the MergeSensor component startup for development by
starting the component as an interactive process so that you can get
stdout/stderr etc

"""

__revision__ = "$Id: TestComponent.py,v 1.7 2007/03/28 16:46:13 ckavka Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os
import sys
import getopt

# MergeSensor
from MergeSensor.MergeSensorComponent import MergeSensorComponent

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
argsDict = {"DBSURL" : None,
            "ComponentDir" : os.getcwd(),
            "PollInterval" : 30,
            "StartMode" : 'warm',
            "MaxMergeFileSize" : 2000000000,
            "MinMergeFileSize" : 1500000000,
            "MergeSiteBlacklist" : "",
            "MergeSiteWhitelist" : "",
            "FastMerge" : "yes",
            "MaxInputAccessFailures" : 1
            }

# options
valid = ['DBSURL=', 'ComponentDir=', 'MaxMergeFileSize=',
         'MinMergeFileSize=', 'PollInterval=', 'StartMode=',
         'MergeSiteBlacklist=', 'MergeSiteWhitelist=',
         'FastMerge=', 'MaxInputAccessFailures='
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
comp = MergeSensorComponent(**argsDict)
comp.startComponent()
