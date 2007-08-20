#!/usr/bin/env python2.4
"""
Util to test the StatTracker component startup for development by
starting the component as an interactive process so that you can get
stdout/stderr etc

"""

import os
import sys
import getopt

from RepackerInjector.T0RepackerInjectorComponent import T0RepackerInjectorComponent

def usage():
    usage = \
    """
    Usage: TestComponent.py <opts>
    Start this component interactively for development testing
    Options:
    ComponentDir
    """
    print usage

argsDict = {
            "ComponentDir" : os.getcwd(),
            "Logfile"      : "T0RepackerInjector.log",
            "CMSSW_arch" : "slc4_ia32_gcc345",
            "CMSSW_version"  : "CMSSW_1_6_0_pre9",
            "CMSSW_path"  : "/uscmst1/prod/sw/cms/",
            "LumiServerUrl" : "http://cmsmon.cern.ch/lumi/servlet/LumiServlet",
            "LogStreamHandler" : "1",
            "LogLevel" : "debug",
            'dbName':'CMSCALD',
            'host':'cmscald',
            'user':'REPACK_DEV',
            'passwd':'****',
            'socketFileLocation':'',
            'portNr':'',
            'refreshPeriod' : 4*3600 ,
            'maxConnectionAttempts' : 5,
            'dbWaitingTime' : 10,
            'dbType' : 'oracle'
            }

valid = [ 'ComponentDir=',]

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


comp = T0RepackerInjectorComponent(**argsDict)
comp.startComponent()
