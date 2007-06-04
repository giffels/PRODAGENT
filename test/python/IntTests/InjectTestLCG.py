#!/usr/bin/env python
"""
Generate jobs for the workflow provided

"""
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from MessageService.MessageService import MessageService
from ProdAgentCore.Configuration import loadProdAgentConfiguration

import sys,os,getopt,time


usage="\n Usage: python InjectTest.py <options> \n Options: \n --workflow=<workflow.xml> \t\t workflow file \n --nevts=<NumberofEvent> \t\t number of events per job \n --njobs=<NumberofEvent> \t\t number of jobs \n --site-pref=<StorageElement name>  storage element name \n [ --run=<firstRun> \t\t\t first run number effective only for New Workflow]\n\n *Note* that the run number option is effective only when a New workflow is created and it overwrites the FirstRun default in $PRODAGENT_CONFIG if set"

valid = ['workflow=', 'run=', 'nevts=' , 'njobs=', 'site-pref=']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

workflow = None
run = "None"
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
workflow=os.path.expandvars(os.path.expanduser(workflow))
if not os.path.exists(workflow):
    print "Workflow not found: %s" % workflow
    sys.exit(1)

if nevts == None:
    print "--nevts option not provided."
    print usage
    sys.exit(1)
if njobs == None:
    print "--njobs option not provided."
    print usage
    sys.exit(1)

def getRequestInjectorConfig():
   """
   get the RequestInjector Component dir and the optional FirstRun
   """
   try:
     config = loadProdAgentConfiguration()
   except StandardError, ex:
     msg = "Error: error reading configuration:\n"
     msg += str(ex)
     print msg
     sys.exit(1)

   if not config.has_key("RequestInjector"):
      msg = "Error: Configuration block RequestInjector is missing from $PRODAGENT_CONFIG"
      print msg
      sys.exit(1)

   ReqInjConfig = config.getConfig("RequestInjector")
   #if not ReqInjConfig.has_key("ComponentDir"):
   #   msg = "Error: Configuration block RequestInjector is missing ComponentDir in $PRODAGENT_CONFIG"
   #   print msg
   #   sys.exit(1)

   return ReqInjConfig.get("ComponentDir", None),ReqInjConfig.get("FirstRun", "None")

def checkWorkflow(workflow):
   """
   Check if the provided workflow already exists in WorkflowCache area
   """
   WorkflowExists=False
   
   workflowBase = os.path.basename(workflow)
   RequestDir,firstrun = getRequestInjectorConfig()
   workflowCache="%s/WorkflowCache"%RequestDir
   if not os.path.exists(workflowCache):
      msg = "Error: there is no WorkflowCache area ==> %s"%workflowCache
      print msg
      sys.exit(1) 
   workflowCacheFile = os.path.join(workflowCache, "%s"%workflowBase)
   if os.path.exists(workflowCacheFile):
      WorkflowExists=True
      msg=" Workflow %s already exists"%(workflowBase,)
      print msg
   else:
      msg=" Workflow %s is NEW since the %s doesn't exist"%(workflowBase,workflowCacheFile)
      print msg
   return WorkflowExists,firstrun


def GoodWorkflow(workflow):
   """
   Check if workflow can be loaded
   """
   RequestDir,firstrun = getRequestInjectorConfig()
   workflowCache="%s/WorkflowCache"%RequestDir
   workflowSpec = WorkflowSpec()
   try:
      workflowSpec.load(workflow)
   except:
      return False
   return True

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

## Set Workflow and run 
WorkflowExists,firstrun=checkWorkflow(workflow)

if WorkflowExists:
 ## reload the Workflow and start from last run 
  run="None"
  ms.publish("RequestInjector:LoadWorkflows",'')
  ms.commit()
  time.sleep(0.1)
  workflowBase=os.path.basename(workflow)
  ms.publish("RequestInjector:SelectWorkflow", workflowBase)
  ms.commit()
else:
 ## set the workflow for the first time and set the compulsory the initial run 
  if run == "None":  run = firstrun
  if run == "None":
     msg="Error: This is a NEW Workflow so it's compulsory to provide an initial Run number! You can: \n a) use the --run option \n b) set FirstRun in the RequestInjector configuration block in $PRODAGENT_CONFIG"
     print msg
     sys.exit(1)
  ms.publish("RequestInjector:SetWorkflow", workflow)
  ms.commit()
  time.sleep(1)
  GoodWf=GoodWorkflow(workflow)
  if not GoodWf:
     print "Error: failed to set the Workflow %s. \nCheck the $PRODAGENT_WORKDIR/RequestInjector/ComponentLog"%workflow
     sys.exit()
  ms.publish("RequestInjector:SetInitialRun", str(run))
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
if run != "None":
  runcomment=" run %s"%str(run)
else:
  runcomment=" last run for %s "%workflowBase
print " Trying to submit %s jobs with %s events each starting from %s"%(njobs,str(nevts),runcomment)

njobs=njobs+1
for i in range(1, njobs):
 ms.publish("RequestInjector:ResourcesAvailable","none")
 ms.commit()
