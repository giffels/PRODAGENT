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
         'activity=', 'stageout-intermediates=', 'chained-input='
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
usage += "                               --stageout-intermediates=<true|false>\n"
usage += "                                  --chained-input=comma,separated,list,of,output,module,names\n"


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cmsRunCfgs    = [] #"/home/ceballos/PRODAGENT_0_3_X/work/cfg/config_alpgen_cmsrun.cfg"
cmsGenCfg    = None #"/home/ceballos/PRODAGENT_0_3_X/work/cfg/myConfig_alpgen.cfg"
versions      = [] #"CMSSW_1_4_3"
stageoutOutputs = []
chainedInputs = []
category     = "Generators"
label        = "CSA07"
channel      = "alpgen-z2j"
physicsGroup = "Individual"
requestId    = "%s-%s" % (os.environ['USER'], int(time.time()))
cfgTypes      = [] #"cfg"
selectionEfficiency = None
activity = None

for opt, arg in opts:
    if opt == "--help":
        print usage
        sys.exit(1)
    if opt == "--cmsRunCfg":
        cmsRunCfgs.append(arg)
        cfgTypes.append("cfg")
    if opt == "--cmsGenCfg":
        cmsGenCfg = arg
    if opt == "--version":
        versions.append(arg)
    if opt == "--stageout-intermediates":
        if arg.lower() in ("true", "yes"):
            stageoutOutputs.append(True)
        else:
            stageoutOutputs.append(False)
    if opt == '--chained-input':
        chainedInputs.append([x.strip() for x in arg.split(',') if x!=''])
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

if not len(cmsRunCfgs):
    msg = "--cmsRunCfg option not provided: This is required"
    raise RuntimeError, msg
elif len(cmsRunCfgs) > 1:
    print "%s cmsRun cfgs listed - chaining them" % len(cmsRunCfgs)
if len(stageoutOutputs) != len(cmsRunCfgs) - 1:
    msg = "Need one less --stageout-intermediates than --cfg arguments"
    raise RuntimeError, msg
if len(chainedInputs) and len(chainedInputs) != len(cfgFiles) - 1:
    msg = "Need one less chained-input than --cfg arguments"
    raise RuntimeError, msg

if cmsGenCfg == None:
    msg = "--cmsGenCfg option not provided: This is required"
    raise RuntimeError, msg

if len(versions) != len(cmsRunCfgs):
    msg = "Need same number of --cmsRunCfg and --version arguments"
    raise RuntimeError, msg

if channel == None:
    msg = "--channel option not provided: This is required"
    raise RuntimeError, msg

for cfgFile in cmsRunCfgs:
    if not os.path.exists(cfgFile):
        msg = "cmsRunCfg File Not Found: %s" % cfgFile
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
maker.setPhysicsGroup(physicsGroup)
# set cmssw version to same as first cmsRun
maker.setCmsGenCMSSWVersion(versions[0])
maker.setCmsGenConfiguration(file(cmsGenCfg).read())
maker.setCmsGenParameters(generator  = generatorString[0])
maker.changeCategory(category)

# loop over cfg's provided and add to workflow
# first cmsRun node created implicitly by WorkflowMaker
nodeNumber = 0
for cmsRunCfg in cmsRunCfgs:
    
    if cfgTypes[nodeNumber] == "cfg":
        from FWCore.ParameterSet.Config import include
        cmsCfg = include(cmsRunCfg) 
    else:
        import imp
        modRef = imp.load_source( os.path.basename(cfgFile).replace(".py", ""),  cfgFile)
        cmsCfg = modRef.process

    cfgWrapper = CMSSWConfig()
    #cfgWrapper.originalCfg = file(cmsRunCfg).read()
    cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
    cfgInt.validateForProduction()
    
    if nodeNumber:
        try:
            inputModules = chainedInputs[nodeNumber-1]
        except IndexError:
            inputModules = []
        maker.chainCmsRunNode(stageoutOutputs[nodeNumber-1], *inputModules)

    maker.setConfiguration(cfgWrapper,  Type = "instance")
    maker.setCMSSWVersion(versions[nodeNumber])
    maker.setOriginalCfg(file(cmsRunCfg).read())
    maker.setPSetHash(WorkflowTools.createPSetHash(cmsRunCfg))

    nodeNumber += 1

if selectionEfficiency != None:
    maker.addSelectionEfficiency(selectionEfficiency)
    maker.addCmsGenSelectionEfficiency(selectionEfficiency)

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
