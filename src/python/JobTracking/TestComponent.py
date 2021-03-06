#!/usr/bin/env python
"""
Util to test the JobTracking component startup for development by
starting the component as an interactive process so that you can get
stdout/stderr etc

"""
__revision__ = "$Id: TestComponent.py,v 1.2.14.1 2008/03/28 15:35:25 gcodispo Exp $"

import os
import sys
import getopt

from JobTracking.TrackingComponent import TrackingComponent

def usage():
    usage = \
    """
    Usage: TestComponent.py <opts>
    Start this component interactively for development testing
    Options:
    BOSSDIR, BOSSPATH, BOSSVERSION, ComponentDir
    """
    print usage

argsDict = {
            "ComponentDir" : os.getcwd(),
            }

valid = ['BOSSDIR=', 'BOSSPATH=', 'BOSSVERSION=', 'ComponentDir=']

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


comp = TrackingComponent(**argsDict)
comp.startComponent()
