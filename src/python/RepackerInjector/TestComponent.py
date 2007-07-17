#!/usr/bin/env python2.4
"""
Util to test the StatTracker component startup for development by
starting the component as an interactive process so that you can get
stdout/stderr etc

"""

import os
import sys
import getopt

from RepackerInjector.RepackerInjectorComponent import RepackerInjectorComponent

def usage():
    usage = \
    """
    Usage: TestComponent.py <opts>
    Start this component interactively for development testing
    Options:
    ComponentDir
    """
    print usage

#           "DbsDbName"    : "dbs_new_era_mysql",
#           "DbsDbName"    : "dbs_new_era_v30",
#           "DbsDbHost"    : "cmssrv17.fnal.gov",
#           "DbsDbPort"    : "",
#           "DbsDbType"    : "mysql",
#           "DbsDbUser"    : "anzar",
#           "DbsDbPasswd"  : "prodagentpass",
#           "Logfile"      : "RepackerInjector.log",
#https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet
#https://cmsdbsprod.cern.ch:8443/cms_dbs_int_tier0_writer/servlet/DBSServlet
#https://cmssrv17.fnal.gov:8443/DBS/servlet/DBSServlet
#http://cmssrv17.fnal.gov:8989/DBSMySQL/servlet/DBSServlet

argsDict = {
            "ComponentDir" : os.getcwd(),
            "Logfile"      : "RepackerInjector.log",
            "CMSSW_Arch" : "slc4_ia32_gcc345",
            "CMSSW_Ver"  : "CMSSW_1_5_0",
            "CMSSW_Dir"  : "/uscmst1/prod/sw/cms/",
            "RepackerCfgTmpl" : "/home/kss/cmswork/projects/PA/LumiServer/PRODAGENT/src/python/RepackerInjector/test/testRepackerTmpl.cfg",
            "JobGroup" : "ankylosis",
            "JobLabel" : "RepackerInjectorTest",
            "DbsUrl" : "http://cmssrv17.fnal.gov:8989/DBS_1_0_5_STABLE/servlet/DBSServlet",
            "DbsLevel" : "ERROR",
#            "LumiServerUrl" : "http://cmssrv18.fnal.gov:8080/lumi/servlet/LumiServlet",
            "LogStreamHandler" : "1",
            "LogLevel" : "debug"
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


comp = RepackerInjectorComponent(**argsDict)
comp.startComponent()
