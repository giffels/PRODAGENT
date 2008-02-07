#!/usr/bin/env python
"""
Generate jobs for the workflow provided

"""
from MessageService.MessageService import MessageService
from DatasetInjector.DatasetInjectorDB import ownerIndex
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

import sys,os,getopt,time


usage="\n Usage: python InjectProcessing.py <options> \n Options: \n --workflow=<workflow.xml> \t\t workflow \n --njobs=<NumberofJobs> \t\t number of jobs  --plugin=<Submission type> \t type of creation/submission plugin\n"

valid = ['workflow=','njobs=','nevts=','plugin=']
admitted_vals = ['LCGAdvanced', 'LCG','GliteBulk','T0LSF']

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

workflow = None
njobs = None
nevts = None
submissiontype = "LCG"

for opt, arg in opts:
    if opt == "--workflow":
        workflow = arg
    if opt == "--njobs":
        njobs = arg
    if opt == "--nevts":
        nevts = arg
    if opt == "--plugin":
        submissiontype = arg
        if submissiontype not in admitted_vals :
           print "Submission plugin: %s is not supported \nSupported values are: %s" % (submissiontype, admitted_vals)
           sys.exit(1)

if workflow == None:
    print "--workflow option not provided"
    print usage
    sys.exit(1)
if njobs  == None:
    print "--njobs option not provided"
    print usage
    sys.exit(1)
if nevts  == None:
    print "--nevts option not provided \n The default of the workflow will be used."
if submissiontype == "GliteBulk":
   if int(njobs) <= 1 :
     print "--njobs need to be greater than 1 for GliteBulk submission"
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
ms.publish("ErrorHandler:StartDebug","none")
ms.commit()

## Set Creator/Submitter for plugin
if submissiontype == "LCG":
 ms.publish("JobCreator:SetCreator","LCGCreator")
 ms.publish("JobSubmitter:SetSubmitter","LCGSubmitter")
if submissiontype == "GliteBulk":
 ms.publish("JobCreator:SetGenerator","Bulk")
 ms.commit()
 time.sleep(0.1)
 ms.publish("JobCreator:SetCreator","LCGBulkCreator")
 ms.publish("JobSubmitter:SetSubmitter","GLiteBulkSubmitter")
 ms.publish("RequestInjector:SetBulkMode",'')
if submissiontype == "T0LSF":
 ms.publish("JobCreator:SetCreator","T0LSFCreator")
 ms.publish("JobSubmitter:SetSubmitter","T0LSFSubmitter")
ms.commit()

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
 #
 # Modify the workflow to force using the split-type event and the split-size to nevts
 #
  if nevts  != None:
    newworkflowSpec = workflowSpec
    SplitType=workflowSpec.parameters['SplitType']
    SplitSize=workflowSpec.parameters['SplitSize']
    print " original      SplitType: %s SplitSize: %s"%(SplitType,SplitSize)
    print " replaced with SplitType: %s SplitSize: %s"%('event',int(nevts))
    newworkflowSpec.parameters['SplitType']='event'
    newworkflowSpec.parameters['SplitSize']=int(nevts)
    backup = "%s.BAK.%s" % (workflow,time.strftime("%d-%M-%Y"))
    os.system("/bin/mv %s %s" % (workflow, backup))
    newworkflowSpec.save(workflow)

  ms.publish("DatasetInjector:SetWorkflow", workflow)
  ms.commit()
  time.sleep(1)
  ms.publish("DatasetInjector:SelectWorkflow", workflowBase)
  ms.commit()

time.sleep(2)

## Set New Dataset: that's triggered by the DatasetInjector
#ms.publish("NewDataset",workflow)
#ms.commit()

print " Trying to submit %s jobs for workflow %s"%(njobs,workflowName)

ms.publish("DatasetInjector:ReleaseJobs",njobs)
ms.commit()
