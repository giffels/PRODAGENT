#!/usr/bin/env python
"""
_DBSInjectReport_
                                                                                
Command line tool to inject a "JobSuccess" event into DBS reading a FrameworkJobReport file.

"""

from MessageService.MessageService import MessageService
import sys,os,getopt,time


usage="\n Usage: python DBSInjectReport.py <options> \n Options: \n --report=<FrameworkJobReport.xml> \t\t FrameworkJobReport file \n"

valid = ['report=']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

report = None

for opt, arg in opts:
    if opt == "--report":
        report = arg

if report == None:
    print "--report option not provided"
    print usage
    sys.exit(1)
if not os.path.exists(report):
    print "FrameWorkJobReport not found: %s" % report
    sys.exit(1)

## use MessageService
ms = MessageService()
## register message service instance as "Test"
ms.registerAs("Test")

ms.publish("DBSInterface:StartDebug",'')
ms.commit()
ms.publish("JobSuccess",report)
ms.commit()
