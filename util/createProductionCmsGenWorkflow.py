#!/usr/bin/env python2.4

"""
_createProductionCmsGenWorkflow_

Create a cmsGen and cmsRun workflow

"""

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker

from ProdCommon.MCPayloads.CmsGenWorkflowMaker import CmsGenWorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

import os
import getopt
import sys
import time

valid = ['cmsRunCfg=', 'cmsGenCfg=', 'version=', 'category=', "label=",
         'channel=', 'group=', 'request-id=', 'selection-efficiency=', 'help',
         'activity=',
         ]

usage =  "Usage: createProductionCmsGenWorkflow.py --cmsRunCfg=<cmsRunCfg>\n"
usage += "                        		 --cmsGenCfg=<cmsGenCfg>\n"
usage += "                        		 --version=<CMSSW version>\n"
usage += "                        		 --category=<Production category>\n"
usage += "                        		 --label=<Production Label>\n"
usage += "                        		 --channel=<Phys Channel/Primary Dataset>\n"
usage += "                        		 --group=<Physics Group>\n"
usage += "                        		 --request-id=<Request ID>\n"
usage += "                               --activity=<activity, i.e. Simulation, Reconstruction, Reprocessing, Skimming>\n"


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cmsRunCfg    = "/home/ceballos/PRODAGENT_0_3_X/work/cfg/config_alpgen_cmsrun.cfg"
cmsGenCfg    = "/home/ceballos/PRODAGENT_0_3_X/work/cfg/myConfig_alpgen.cfg"
version      = "CMSSW_1_4_3"
category     = "Generators"
label        = "CSA07"
channel      = "alpgen-z2j"
physicsGroup = "Individual"
requestId    = "%s-%s" % (os.environ['USER'], int(time.time()))
cfgType      = "cfg"
selectionEfficiency = None
activity = None

for opt, arg in opts:
    if opt == "--help":
        print usage
        sys.exit(1)
    if opt == "--cmsRunCfg":
        cmsRunCfg = arg
    if opt == "--cmsGenCfg":
        cmsGenCfg = arg
    if opt == "--version":
        version = arg
    if opt == "--category":
        category = arg
    if opt == "--label":
        label = arg
    if opt == "--channel":
        channel = arg
    if opt == "--group":
        physicsGroup = arg
    if opt == "--request-id":
        requestId = arg
    if opt == "--selection-efficiency":
        selectionEfficiency = arg
    if opt == "--activity":
        activity = arg

if cmsRunCfg == None:
    msg = "--cmsRunCfg option not provided: This is required"
    raise RuntimeError, msg

if cmsGenCfg == None:
    msg = "--cmsGenCfg option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

if channel == None:
    msg = "--channel option not provided: This is required"
    raise RuntimeError, msg

if not os.path.exists(cmsRunCfg):
    msg = "cmsRunCfg File Not Found: %s" % cmsRunCfg
    raise RuntimeError, msg

if not os.path.exists(cmsGenCfg):
    msg = "cmsGenCfg File Not Found: %s" % cmsGenCfg
    raise RuntimeError, msg

# 
# Set CMSSW_SEARCH_PATH 
#
origcmsswsearch=os.environ.get("CMSSW_SEARCH_PATH", None)
if not origcmsswsearch:
   msg = "CMSSW_SEARCH_PATH not set....you need CMSSW environment "
   raise RuntimeError, msg
cmsswsearch="/:%s"%origcmsswsearch
os.environ["CMSSW_SEARCH_PATH"]=cmsswsearch

if cfgType == "cfg":
    from FWCore.ParameterSet.Config import include
    cmsCfg = include(cmsRunCfg) 
else:
    modRef = imp.find_module( os.path.basename(cmsRunCfg).replace(".py", ""),  os.path.dirname(cmsRunCfg))
    cmsCfg = modRef.process
    
cfgWrapper = CMSSWConfig()
cfgWrapper.originalCfg = file(cmsRunCfg).read()
cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
cfgInt.validateForProduction()

# First line in the cmsGenCfg file is the generator
# Second line in the cmsGenCfg file is the executable
fileCmsGen = open(cmsGenCfg, 'r')
generatorLine    = fileCmsGen.readline()
generatorString  = generatorLine.split('\n')
##executableLine   = fileCmsGen.readline()
##executableString = executableLine.split('\n')
# redefine primary dataset
channel = "%s-%s"%(channel,generatorString[0])

maker = CmsGenWorkflowMaker(requestId, channel, label)
maker.setCMSSWVersion(version)
maker.setPhysicsGroup(physicsGroup)
maker.setConfiguration(cfgWrapper,  Type = "instance")
maker.setCmsGenConfiguration(file(cmsGenCfg).read())
maker.setPSetHash(WorkflowTools.createPSetHash(cmsRunCfg))
maker.changeCategory(category)
maker.setCmsGenParameters(generator  = generatorString[0])

if selectionEfficiency != None:
    maker.addSelectionEfficiency(selectionEfficiency)
    maker.addCmsGenSelectionEfficiency(selectionEfficiency)

#                         , executable = executableString[0]
#                          )

wfspec = maker.makeWorkflow()
if activity is not None:
    wfspec.setActivity(activity)
wfspec.save("%s-Workflow.xml" % maker.workflowName)

print "Created: %s-Workflow.xml" % maker.workflowName
print "From: %s &" % cmsRunCfg
print "      %s" % cmsGenCfg
print "Output Datasets:"
[ sys.stdout.write(
     "/%s/%s/%s\n" % (
       x['PrimaryDataset'],
       x['ProcessedDataset'],
       x['DataTier'])) for x in wfspec.outputDatasets()]
