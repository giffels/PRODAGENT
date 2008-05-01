#!/usr/bin/env python
"""
_createPreProdWorkflow_

Create a preprod workflow using a configuration PSet.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.11 $"
__revision__ = "$Id: createProductionWorkflow.py,v 1.11 2008/04/09 16:09:21 swakef Exp $"


import os
import sys
import getopt
import popen2
import time

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig


valid = ['cfg=', 'py-cfg=', 'version=', 'category=', "label=",
         'channel=', 'group=', 'request-id=',
         'pileup-dataset=', 'pileup-files-per-job=',
         'selection-efficiency=', 'activity='
         ]

usage = "Usage: createProductionWorkflow.py --cfg=<cfgFile>\n"
usage += "                                  --version=<CMSSW version>\n"
usage += "                                  --channel=<Phys Channel/Primary Dataset>\n"
usage += "                                  --group=<Physics Group>\n"
usage += "                                  --request-id=<Request ID>\n"
usage += "                                  --label=<Production Label>\n"
usage += "                                  --category=<Production category>\n"
usage += "                                  --activity=<activity, i.e. Simulation, Reconstruction, Reprocessing, Skimming>\n"
usage += "\n"
usage += "You must have a scram runtime environment setup to use this tool\n"
usage += "since it will invoke EdmConfig tools\n\n"
usage += "Workflow Name is the name of the Workflow/Request/Primary Dataset\n"
usage += "to be used. \n"
usage += "It will default to the name of the cfg file if not provided\n\n"
usage += "Production category is a marker that will be added to LFNs, \n"
usage += "For example: PreProd, CSA06 etc etc. Defaults to mc\n"

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cfgFiles = []
requestId = "%s-%s" % (os.environ['USER'], int(time.time()))
physicsGroup = "Individual"
label = "Test"
version = None
category = "mc"
channel = None
cfgType = "cfg"
activity = None

pileupDS = None
pileupFilesPerJob = 1

selectionEfficiency = None

for opt, arg in opts:
    if opt == "--cfg":
        cfgFiles.append(arg)
        cfgType = "cfg"
    if opt == "--py-cfg":
        cfgFiles.append(arg)
        cfgType = "python"
    if opt == "--version":
        version = arg
    if opt == "--category":
        category = arg
    if opt == "--channel":
        channel = arg
    if opt == "--label":
        label = arg
    if opt == "--group":
        physicsGroup = arg
    if opt == "--request-id":
        requestId = arg

    if opt == "--selection-efficiency":
        selectionEfficiency = arg

    if opt == '--pileup-dataset':
        pileupDS = arg
    if opt == '--pileup-files-per-job':
        pileupFilesPerJob = arg
    if opt == '--activity':
        activity = arg
   
    
if len(cfgFiles) == 0:
    msg = "--cfg option not provided: This is required"
    raise RuntimeError, msg
elif len(cfgFiles) > 1:
    print "%s cfgs listed - chaining them" % len(cfgFiles)

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

if channel == None:
    msg = "--channel option not provided: This is required"
    raise RuntimeError, msg



for cfgFile in cfgFiles:
    if not os.path.exists(cfgFile):
        msg = "Cfg File Not Found: %s" % cfgFile
        raise RuntimeError, msg

#  //
# // Set CMSSW_SEARCH_PATH 
#//
origcmsswsearch=os.environ.get("CMSSW_SEARCH_PATH", None)
if not origcmsswsearch:
   msg = "CMSSW_SEARCH_PATH not set....you need CMSSW environment "
   raise RuntimeError, msg
cmsswsearch="/:%s"%origcmsswsearch
os.environ["CMSSW_SEARCH_PATH"]=cmsswsearch

#  //
# // Instantiate a WorkflowMaker
#//
maker = WorkflowMaker(requestId, channel, label )
maker.setPhysicsGroup(physicsGroup)
maker.changeCategory(category)

if selectionEfficiency != None:
    maker.addSelectionEfficiency(selectionEfficiency)

# loop over cfg's provided and add to workflow
# first cmsRun node created implicitly by WorkflowMaker
firstNode = True
for cfgFile in cfgFiles:
    
    if cfgType == "cfg":
        from FWCore.ParameterSet.Config import include
        cmsCfg = include(cfgFile)
    else:
        modRef = imp.find_module( os.path.basename(cfgFile).replace(".py", ""),  os.path.dirname(cfgFile))
        cmsCfg = modRef.process
        
    cfgWrapper = CMSSWConfig()
    cfgWrapper.originalCfg = file(cfgFile).read()
    cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
    cfgInt.validateForProduction()
    
    if not firstNode:
        maker.appendCmsRunNode()
        
    maker.setCMSSWVersion(version)
    maker.setConfiguration(cfgWrapper, Type = "instance")
    
    #TODO: What about pset hash
    maker.setPSetHash(WorkflowTools.createPSetHash(cfgFile))
    
    firstNode = False

#  //
# // Pileup sample?
#//
if pileupDS != None:
    maker.addPileupDataset( pileupDS, pileupFilesPerJob)
    
  
spec = maker.makeWorkflow()

if activity is not None:
    spec.setActivity(activity)

spec.save("%s-Workflow.xml" % maker.workflowName)


print "Created: %s-Workflow.xml" % maker.workflowName
print "From: %s " % cfgFile
print "Output Datasets:"
[ sys.stdout.write(
     "/%s/%s/%s\n" % (
       x['PrimaryDataset'],
       x['ProcessedDataset'],
       x['DataTier'])) for x in spec.outputDatasets()]




