#!/usr/bin/env python
"""
_DBSInjectReport_
                                                                                
Command line tool to inject a "JobSuccess" event into DBS reading a FrameworkJobReport file.

"""

from MessageService.MessageService import MessageService
from FwkJobRep.ReportState import checkSuccess
import string,sys,os,getopt,time


usage="\n Usage: python DBSInjectReport.py <options> \n Options: \n --report=<FrameworkJobReport.xml> \t\t single FrameworkJobReport file \n --reportFileList=<filewithFWJReportlist> \t\t File with the list of FrameworkJobReport files \n "
valid = ['report=','reportFileList=']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

report = None
reportFileList = None

for opt, arg in opts:
    if opt == "--report":
        report = arg
for opt, arg in opts:
    if opt == "--reportFileList":
        reportFileList = arg

if (report == None) and (reportFileList == None) :
    print "\n either --report or --reportFileList option has to be provided"
    print usage
    sys.exit(1)
if (report != None) and (reportFileList != None) :
    print "\n options --report or --reportFileList are mutually exclusive"
    print usage
    sys.exit(1)

## use MessageService
ms = MessageService()
## register message service instance as "Test"
ms.registerAs("Test")
## set debug level 
ms.publish("DBSInterface:StartDebug",'')
ms.commit()


if (report != None):
 expand_report=os.path.expandvars(os.path.expanduser(report))
 if not os.path.exists(expand_report):
    print "FrameWorkJobReport not found: %s" % expand_report
    sys.exit(1)
 if checkSuccess(expand_report):
 ## fire JobSuccess event
   print "** Send \"JobSuccess\" Event with : %s"%expand_report
   ms.publish("JobSuccess",expand_report)
   ms.commit()
 else:
   print "** Do nothing because JobFailed for : %s "%expand_report

if (reportFileList != None) :
 expand_reportFileList=os.path.expandvars(os.path.expanduser(reportFileList))
 if not os.path.exists(expand_reportFileList):
    print "File not found: %s" % expand_reportFileList
    sys.exit(1)

 reportlist_file = open(expand_reportFileList,'r')
 for line in reportlist_file.readlines():
  expand_report=os.path.expandvars(os.path.expanduser(string.strip(line)))
  if checkSuccess(expand_report):
  ## fire JobSuccess event
    print "** Send \"JobSuccess\" Event with : %s"%expand_report
    ms.publish("JobSuccess",expand_report)
    ms.commit()
  else:
    print "** Do nothing because JobFailed for : %s "%expand_report

 reportlist_file.close()



