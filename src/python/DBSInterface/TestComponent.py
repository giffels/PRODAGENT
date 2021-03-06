#!/usr/bin/env python
"""
Util to test the DBS component startup for development by
starting the component as an interactive process so that you can get
stdout/stderr etc

"""

import os
import sys
import getopt

from DBSInterface.DBSComponent import DBSComponent

def usage():
    usage = \
    """
    Usage: TestComponent.py <opts>
    Start this component interactively for development testing
    Options:
    DBSURL , DBSAddress, DBSType ComponentDir
    """
    print usage

argsDict = {
            "DBSURL" : None,
            "DBSAddress" : None,
            "DBSType" : "CGI",
            "DLSAddress" : None,
            "DLSType" : "DLS_TYPE_MYSQL",
            "ComponentDir" : os.getcwd(),
            }

valid = ['DBSURL=', 'DBSAddress=', 'DBSType=', 'DLSAddress=', 'DLSType=', 'ComponentDir=']

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


comp = DBSComponent(**argsDict)
comp.startComponent()
