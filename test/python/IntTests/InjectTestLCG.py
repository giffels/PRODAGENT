#!/usr/bin/env python
"""
Generate jobs for the workflow provided

"""
from MessageService.MessageService import MessageService
import sys,os,getopt,time


usage="\n Usage: python InjectTest.py <options> \n Options: \n --workflow=<workflow.xml> \t\t workflow file \n --nevts=<NumberofEvent> \t\t number of events per job \n --njobs=<NumberofEvent> \t\t number of jobs \n --site-pref=<StorageElement name>  storage element name \n --run=<firstRun> \t\t\t first run number \n\n *Note* that if no run number is provided the last run from the given workflow is taken [This assume that the given workflow already exists]."

valid = ['workflow=', 'run=', 'nevts=' , 'njobs=', 'site-pref=']
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
sitePref = None

for opt, arg in opts:
    if opt == "--workflow":
        workflow = arg
    if opt == "--run":
        run = int(arg)
    if opt == "--nevts":
        nevts = int(arg)
    if opt == "--njobs":
        njobs = int(arg)
    if opt == "--site-pref":
        sitePref = arg
 

if workflow == None:
    print "--workflow option not provided"
    print usage
    sys.exit(1)
if not os.path.exists(workflow):
    print "Workflow not found: %s" % workflow
    sys.exit(1)
## run is nolonger a compulsory option:
#if run == None:
#    print "--run option not provided."
#    print usage
#    sys.exit(1)
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

## Set Workflow 
if run != None: 
 ## if run number is provided set the workflow and its initial run
  ms.publish("RequestInjector:SetWorkflow", workflow)
  ms.commit()
  time.sleep(0.1)
  ms.publish("RequestInjector:SetInitialRun", str(run))
  ms.commit()
else:
 ## if no run number is provided, reload the Workflow and start run from there
  ms.publish("RequestInjector:LoadWorkflows",'')
  ms.commit()
  time.sleep(0.1)
  workflowBase=os.path.basename(workflow)
  ms.publish("RequestInjector:SelectWorkflow", workflowBase)
  ms.commit()


if sitePref != None:
   ms.publish("RequestInjector:SetSitePref", sitePref)
   ms.commit()
   time.sleep(0.1)

ms.publish("RequestInjector:SetEventsPerJob", str(nevts))
ms.commit()
time.sleep(2)

## Set New Dataset
ms.publish("RequestInjector:NewDataset",'')
ms.commit()

## Loop over jobs
if run != None:
  runcomment=" run %s"%str(run)
else:
  runcomment=" last run for %s "%workflowBase
print " Trying to submit %s jobs with %s events each starting from %s"%(njobs,str(nevts),runcomment)

njobs=njobs+1
for i in range(1, njobs):
 ms.publish("RequestInjector:ResourcesAvailable","none")
 ms.commit()
