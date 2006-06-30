#!/usr/bin/env python
"""
_DBSInjectNewDataset_
                                      
Command line tool to inject a "NewDataset" into DBS reading a workflow specification file.                                          

"""

from MessageService.MessageService import MessageService
import sys,os,getopt,time

usage="\n Usage: python DBSInjectNewDataset.py <options> \n Options: \n --workflow=<workflow.xml> \t\t workflow file \n"

valid = ['workflow=']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

workflow = None

for opt, arg in opts:
    if opt == "--workflow":
        workflow = arg

if workflow == None:
    print "--workflow option not provided"
    print usage
    sys.exit(1)
if not os.path.exists(workflow):
    print "workflow not found: %s" % workflow
    sys.exit(1)

## use MessageService
ms = MessageService()
## register message service instance as "Test"
ms.registerAs("Test")

ms.publish("DBSInterface:StartDebug",'')
ms.commit()
ms.publish("NewDataset",workflow)
ms.commit()
