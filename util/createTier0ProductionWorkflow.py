#!/usr/bin/env python
"""
_createTier0ProductionWorkflow_

Create a workflow to create streamer MC files for Tier0 processing.

"""
__version__ = "$Revision: 1.3 $"
__revision__ = "$Id: createTier0ProductionWorkflow.py,v 1.3 2009/07/05 18:44:59 hufnagel Exp $"


import os
import sys
import time
import getopt
import imp

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
#from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig


valid = ['py-cfg=', 'version=',
         'acquisition_era=',
         'conditions=',
         'processing_version=',
         ]

usage = "Usage: createProductionWorkflow.py --py-cfg=<cfgFile>\n"
usage += "                                  --version=<CMSSW version>\n"
usage += "                                  --acquisition_era=<Acquisition Era>\n"
usage += "                                  --conditions=<Conditions>\n"
usage += "                                  --processing_version=<Processing version>\n"
usage += "\n"
usage += "You must have a scram runtime environment setup to use this tool\n"
usage += "since it will invoke EdmConfig tools\n\n"
usage += "Workflow Name is the name of the Workflow/Request/Primary Dataset\n"
usage += "to be used. \n"
usage += "It will default to the name of the cfg file if not provided\n\n"

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cfgFile = None
version = None
acquisitionEra = "Test"
conditions = "Bad"
processingVersion = None

for opt, arg in opts:
    if opt == "--py-cfg":
        cfgFile = arg
    if opt == "--version":
        version = arg
    if opt == "--acquisition_era":
        acquisitionEra = arg
    if opt == "--conditions":
        conditions = arg
    if opt == "--processing_version":
        processingVersion = arg
    
if cfgFile == None:
    msg = "--cfg option not provided: This is required"
    raise RuntimeError, msg
if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

if not os.path.exists(cfgFile):
    msg = "Cfg File Not Found: %s" % cfgFile
    raise RuntimeError, msg


#
# create workflow
#

workflowName = "Spring09Testing-Tier0Feed-hufnagel-1246816871"
scramArch = "slc4_ia32_gcc345"
cmsPath = "/afs/cern.ch/cms/sw"

workflow = WorkflowSpec()
workflow.setWorkflowName(workflowName)
workflow.setRequestCategory("mc")
workflow.setRequestTimestamp(int(time.time()))
workflow.parameters["WorkflowType"] = "Processing"
workflow.parameters["CMSSWVersion"] = version
workflow.parameters["ScramArch"] = scramArch
workflow.parameters["CMSPath"] = cmsPath

# needed for streamed index stageout
workflow.parameters['StreamerIndexDir'] = "vocms13:/data/hufnagel/parepack/StreamerIndexDir"

cmsRunNode = workflow.payload
cmsRunNode.name = "cmsRun1"
cmsRunNode.type = "CMSSW"
cmsRunNode.application["Version"] = version
cmsRunNode.application["Executable"] = "cmsRun"
cmsRunNode.application["Project"] = "CMSSW"
cmsRunNode.application["Architecture"] = scramArch

# special runtime script
cmsRunNode.scriptControls["PostExe"].append(
    "JobCreator.RuntimeTools.RuntimeStreamerToFJR"
    )

#
# build the configuration template for the workflow
#
loader = CMSSWAPILoader(scramArch, version, cmsPath)

try:
    loader.load()
except Exception, ex:
    msg = "Couldn't load CMSSW libraries: %s" % ex
    raise RuntimeError, msg

loadedModule = imp.load_source( os.path.basename(cfgFile).replace(".py", ""), cfgFile )

cmsRunNode.cfgInterface = CMSSWConfig()
loadedConfig = cmsRunNode.cfgInterface.loadConfiguration(loadedModule.process)
loadedConfig.validateForProduction()

loader.unload()

# generate Dataset information for workflow from cfgInterface
for moduleName,outMod in cmsRunNode.cfgInterface.outputModules.items():

    lfnBase = "/T0/hufnagel/Tier0Feed/RAW/v1/0000"

    outMod["LFNBase"] = lfnBase
    outMod["logicalFileName"] = os.path.join(
        lfnBase, "%s.root" % moduleName
        )

WorkflowTools.addStageOutNode(cmsRunNode, "stageOut1")

workflow.save("%s-Workflow.xml" % workflowName)

print "Created: %s-Workflow.xml" % workflowName
print "From: %s " % cfgFile

