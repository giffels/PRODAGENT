#!/usr/bin/env python
"""
_createProductionCmsGenWorkflow_

Create a cmsGen and cmsRun workflow

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: createProductionCmsGenWorkflow_CSA08Hack.py,v 1.2 2008/09/15 08:15:58 ceballos Exp $"



import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker

from ProdCommon.MCPayloads.CmsGenWorkflowMaker import CmsGenWorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

import os
import getopt
import sys
import time
import popen2

valid = ['cmsRunCfg=', 'cmsGenCfg=', 'version=', 'category=', "label=",
         'channel=', 'group=', 'request-id=', 'selection-efficiency=', 'help',
         'activity=', 'stageout-intermediates=', 'chained-input=',
         'pileup-dataset=', 'pileup-files-per-job=','only-sites=',
         'starting-run=', 'starting-event=', 'totalevents=', 'eventsperjob=',
         'acquisition_era=', 'conditions=', 'processing_version=',
         'workflow_tag=', 'override-initial-event='
         ]

usage  = "Usage: createProductionCmsGenWorkflow.py\n"
usage += "                                  --cmsRunCfg=<cmsRunCfg>\n"
usage += "                     	            --cmsGenCfg=<cmsGenCfg>\n"
usage += "                                  --version=<CMSSW version>\n"
usage += "                                  --category=<Production category>\n"
#usage += "                                  --label=<Production Label>\n" #Replaced by --acquisition_era
usage += "                                  --channel=<Phys Channel/Primary Dataset>\n"
usage += "                                  --group=<Physics Group>\n"
#usage += "                                  --request-id=<Request ID>\n" #Replaced by --conditions and --processing_version
usage += "                                  --selection-efficiency=<efficiency>\n"
usage += "                                  --activity=<activity, i.e. Simulation, Reconstruction, Reprocessing, Skimming>\n"
usage += "                                  --stageout-intermediates=<true|false>\n"
usage += "                                  --chained-input=comma,separated,list,of,output,module,names\n"
usage += "                                  --pileup-dataset=<Input pileup dataset>\n"
usage += "                                  --pileup-files-per-job=<file per job> Default: 1\n"
usage += "                                  --only-sites=<Site>\n"
usage += "                                  --starting-run=<Starting run>\n"
usage += "                                  --starting-event=<Initial event>\n"
usage += "                                  --totalevents=<Total Events>\n"
usage += "                                  --eventsperjob=<Events/job>\n"
usage += "                                  --acquisition_era=<Acquisition Era>\n"
usage += "                                  --conditions=<Conditions>\n"
usage += "                                  --processing_version=<Processing version>\n"
usage += "                                  --workflow_tag=<Workflow tag>\n"
usage += "                                  --override-initial-event=<Override Initial event>\n"
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

cmsRunCfgs          = []
cmsGenCfg           = None
versions            = []
stageoutOutputs     = []
chainedInputs       = []
category            = "mc"
label               = "Test"
channel             = None
physicsGroup        = "Individual"
requestId           = "%s" % (int(time.time()))
cfgTypes            = []
selectionEfficiency = None
activity            = None
startingRun         = 1
initialEvent        = 1
totalEvents         = 1000
eventsPerJob        = 100
acquisitionEra      = "Test"
conditions          = "Bad"
processingVersion   = "666"
onlySites           = None
workflow_tag        = None
pileupDS            = None
pileupFilesPerJob   = 1
overrideInitialEvent= None


for opt, arg in opts:
    if opt == "--help":
        print usage
        sys.exit(1)
    if opt == "--cmsRunCfg":
        cmsRunCfgs.append(arg)
        cfgTypes.append("python")
    if opt == "--cmsGenCfg":
        cmsGenCfg = arg
    if opt == "--version":
        versions.append(arg)
    if opt == "--category":
        category = arg
#    if opt == "--label":
#        label = arg
    if opt == "--channel":
        channel = arg
    if opt == "--group":
        physicsGroup = arg
#    if opt == "--request-id":
#        requestId = arg
    if opt == "--selection-efficiency":
        selectionEfficiency = arg
    if opt == "--activity":
        activity = arg
    if opt == "--stageout-intermediates":
        if arg.lower() in ("true", "yes"):
            stageoutOutputs.append(True)
        else:
            stageoutOutputs.append(False)
    if opt == '--chained-input':
        chainedInputs.append([x.strip() for x in arg.split(',') if x!=''])
    if opt == '--pileup-dataset':
        pileupDS = arg
    if opt == '--pileup-files-per-job':
        pileupFilesPerJob = arg
    if opt == '--only-sites':
        onlySites = arg
    if opt == "--starting-run":
        startingRun = arg
    if opt == "--starting-event":
        initialEvent = arg
    if opt == "--totalevents":
        totalEvents = arg
    if opt == "--eventsperjob":
        eventsPerJob = arg
    if opt == "--acquisition_era":
        acquisitionEra = arg
    if opt == "--conditions":
        conditions = arg
    if opt == "--processing_version":
        processingVersion = arg
    if opt == '--workflow_tag':
        workflow_tag = arg
    if opt == "--override-initial-event":
        overrideInitialEvent = arg
   
if not len(cmsRunCfgs):
    msg = "--cmsRunCfg option not provided: This is required"
    raise RuntimeError, msg
elif len(cmsRunCfgs) > 1:
    print "%s cmsRun cfgs listed - chaining them" % len(cmsRunCfgs)

if len(stageoutOutputs) != len(cmsRunCfgs) - 1:
    msg = "Need one less --stageout-intermediates than --cfg arguments"
    raise RuntimeError, msg

if len(chainedInputs) and len(chainedInputs) != len(cmsRunCfgs) - 1:
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

if workflow_tag in (None,""):
   requestId="%s_%s" % (conditions,processingVersion)
else:
   requestId="%s_%s_%s" % (conditions,workflow_tag,processingVersion)

#requestId="%s_%s" % (conditions,processingVersion)
label=acquisitionEra

#  //
# // Set CMSSW_SEARCH_PATH 
#//
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
        modRef = imp.load_source( os.path.basename(cmsRunCfg).replace(".py", ""),  cmsRunCfg)
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
    maker.setPhysicsGroup(physicsGroup)
    maker.changeCategory(category)
    maker.setAcquisitionEra(acquisitionEra)
    maker.workflow.parameters['Conditions'] = conditions
    maker.workflow.parameters['ProcessingVersion'] = processingVersion

    nodeNumber += 1

if selectionEfficiency != None:
    maker.addSelectionEfficiency(selectionEfficiency)
    maker.addCmsGenSelectionEfficiency(selectionEfficiency)

#  //
# // Pileup sample?
#//
if pileupDS != None:
    maker.addPileupDataset( pileupDS, pileupFilesPerJob)
 
spec = maker.makeWorkflow()

spec.parameters['TotalEvents']=totalEvents
spec.parameters['EventsPerJob']=eventsPerJob
spec.parameters['InitialEvent']=initialEvent
spec.parameters['InitialRun']=startingRun
if overrideInitialEvent not in (None, ""):
    spec.parameters['OverrideInitialEvent']=overrideInitialEvent
if onlySites != None:
   spec.parameters['OnlySites']=onlySites

if activity is not None:
    spec.setActivity(activity)
spec.save("%s-Workflow.xml" % maker.workflowName)


print "Created: %s-Workflow.xml" % maker.workflowName
print "From: %s &" % cmsRunCfg
print "      %s" % cmsGenCfg
print "Output Datasets:"
[ sys.stdout.write(
     "/%s/%s/%s\n" % (
       x['PrimaryDataset'],
       x['ProcessedDataset'],
       x['DataTier'])) for x in spec.outputDatasets()]
