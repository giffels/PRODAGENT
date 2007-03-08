#!/usr/bin/env python
"""
Generate jobs for the workflow provided

"""
from MessageService.MessageService import MessageService
from DatasetInjector.DatasetInjectorDB import ownerIndex
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

import sys,os,getopt,time


usage="\n Usage: python InjectTestSkimLCG.py <options> \n Options: \n --workflow=<workflow.xml> \t\t workflow \n --njobs=<NumberofJobs> \t\t number of jobs \n"

valid = ['workflow=','njobs=']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

workflow = None
njobs = None

for opt, arg in opts:
    if opt == "--workflow":
        workflow = arg
    if opt == "--njobs":
        njobs = arg

if workflow == None:
    print "--workflow option not provided"
    print usage
    sys.exit(1)
if njobs  == None:
    print "--njobs option not provided"
    print usage
    sys.exit(1)


## check workflow existing on disk 
workflow=os.path.expandvars(os.path.expanduser(workflow))
if not os.path.exists(workflow):
    print "Workflow not found: %s" % workflow
    sys.exit(1)

## get the workflow name
workflowSpec = WorkflowSpec()
workflowSpec.load(workflow)
workflowName = workflowSpec.workflowName()
workflowBase=os.path.basename(workflow)

## use MessageService
ms = MessageService()
## register message service instance as "Test"
ms.registerAs("TestSkim")

## Debug level
ms.publish("DatasetInjector:StartDebug","none")
ms.publish("JobCreator:StartDebug","none")
ms.publish("JobSubmitter:StartDebug","none")
ms.publish("DBSInterface:StartDebug","none")
ms.publish("TrackingComponent:StartDebug","none")
ms.commit()
## Set Creator
ms.publish("JobCreator:SetCreator","LCGCreator")
## Set Submitter
ms.publish("JobSubmitter:SetSubmitter","LCGSubmitter")

## Set Workflow 

## if workflow alredy exists in DB, reload and select it
if ownerIndex(workflowName) != None:
  ms.publish("DatasetInjector:LoadWorkflows",'')
  ms.commit()
  time.sleep(0.1)
  ms.publish("DatasetInjector:SelectWorkflow", workflowBase)
  ms.commit()
else:
 ## set the workflow for the first time
  ms.publish("DatasetInjector:SetWorkflow", workflow)
  ms.commit()
  time.sleep(1)
  ms.publish("DatasetInjector:SelectWorkflow", workflowBase)
  ms.commit()

time.sleep(2)

## Set New Dataset
ms.publish("NewDataset",workflow)
ms.commit()

print " Trying to submit %s jobs for workflow %s"%(njobs,workflowName)

ms.publish("DatasetInjector:ReleaseJobs",njobs)
ms.commit()
