#!/usr/bin/env python
"""
Generate jobs for the workflow provided

"""
from MessageService.MessageService import MessageService
import sys,os,getopt,time


usage="\n Usage: python InjectTest.py <options> \n Options: \n --workflow=<workflow.xml> \t\t workflow file \n --run=<firstRun> \t\t first run number \n --nevts=<NumberofEvent> \t\t number of events per job \n --njobs=<NumberofEvent> \t\t number of jobs \n"

valid = ['workflow=', 'run=', 'nevts=' , 'njobs=']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

workflow = None
run = None
nevts = None
njobs = None

for opt, arg in opts:
    if opt == "--workflow":
        workflow = arg
    if opt == "--run":
        run = int(arg)
    if opt == "--nevts":
        nevts = int(arg)
    if opt == "--njobs":
        njobs = int(arg)


if workflow == None:
    print "--workflow option not provided"
    print usage
    sys.exit(1)
if not os.path.exists(workflow):
    print "Workflow not found: %s" % workflow
    sys.exit(1)
if run == None:
    print "--run option not provided."
    print usage
    sys.exit(1)
if nevts == None:
    print "--nevts option not provided."
    print usage
    sys.exit(1)
if njobs == None:
    print "--njobs option not provided."
    print usage
    sys.exit(1)


## use MessageService
ms = MessageService()
## register message service instance as "Test"
ms.registerAs("Test")

## Debug level
ms.publish("RequestInjector:StartDebug","none")
ms.publish("JobCreator:StartDebug","none")
ms.publish("JobSubmitter:StartDebug","none")
ms.commit()
ms.publish("TrackingComponent:StartDebug","none")
ms.commit()
## Set Creator
ms.publish("JobCreator:SetCreator","LCGCreator")
## Set Submitter
ms.publish("JobSubmitter:SetSubmitter","LCGSubmitter")
## Set Workflow and NewDataset
ms.publish("RequestInjector:SetWorkflow", workflow)
ms.commit()
time.sleep(2)
ms.publish("RequestInjector:NewDataset",'')
ms.commit()
## Set first run and number of events per job
ms.publish("RequestInjector:SetInitialRun", str(run))
ms.commit()
ms.publish("RequestInjector:SetEventsPerJob", str(nevts))
ms.commit()
time.sleep(2)
## Loop over jobs
print " Trying to submit %s jobs with %s events each starting from run %s"%(njobs,str(nevts),str(run))
njobs=njobs+1
for i in range(1, njobs):
 ms.publish("ResourcesAvailable","none")
 ms.commit()
