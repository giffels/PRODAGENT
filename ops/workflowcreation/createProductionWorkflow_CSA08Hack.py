#!/usr/bin/env python
"""
_createPreProdWorkflow_

Create a preprod workflow using a configuration PSet.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.8 $"
__revision__ = "$Id: createProductionWorkflow_CSA08Hack.py,v 1.8 2009/02/20 23:20:05 direyes Exp $"


import os
import sys
import getopt
import popen2
import time

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig


valid = ['cfg=', 'py-cfg=', 'version=', 'category=', #"label=",
         'channel=', 'group=', #'request-id=',
         'pileup-dataset=', 'pileup-files-per-job=',
         'selection-efficiency=', 'activity=', 'stageout-intermediates=',
         'chained-input=', 'starting-run=','starting-event=','totalevents=',
         'eventsperjob=', 'acquisition_era=', 'conditions=', 'processing_version=',
         'only-sites=', 'store-fail=','workflow_tag='
         ]

usage = "Usage: createProductionWorkflow.py --cfg=<cfgFile>\n"
usage += "                                  --version=<CMSSW version>\n"
usage += "                                  --channel=<Phys Channel/Primary Dataset>\n"
usage += "                                  --group=<Physics Group>\n"
#usage += "                                  --request-id=<Request ID>\n"  #Replaced by --conditions and --processing_version 
#usage += "                                  --label=<Production Label>\n" #Replaced by --aquisition_era
usage += "                                  --category=<Production category>\n"
usage += "                                  --activity=<activity, i.e. Simulation, Reconstruction, Reprocessing, Skimming>\n"
usage += "                                  --stageout-intermediates=<true|false>\n"
usage += "                                  --chained-input=comma,separated,list,of,output,module,names\n"
usage += "                                  --store-fail=<true|false>. It forces the stageout \
                                            of output files before a faild step\n"
usage += "                                  --starting-run=<Starting lumi>\n"
usage += "                                  --starting-event=<Starting event>\n"
usage += "                                  --totalevents=<Total Events>\n"
usage += "                                  --eventsperjob=<Events/job>\n"
usage += "                                  --acquisition_era=<Acquisition Era>\n"
usage += "                                  --conditions=<Conditions>\n"
usage += "                                  --processing_version=<Processing version>\n"
usage += "                                  --only-sites=<Site>\n"
usage += "                                  --workflow_tag=<Tag in workflow name to distinguish e.g. RAW and RECO workflows for a given channel>\n"
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
stageoutOutputs = []
chainedInputs = []
#requestId = "%s-%s" % (os.environ['USER'], int(time.time()))
physicsGroup = "Individual"
#label = "Test"
versions = []
category = "mc"
channel = None
cfgTypes = []
activity = None
startingRun = None
startingEvent = None
totalEvents = None
eventsPerJob = None
acquisitionEra = "Test"
conditions = "Bad"
processingVersion = None
onlySites=None
storeFail = False
workflow_tag=None

pileupDS = None
pileupFilesPerJob = 1

selectionEfficiency = None

for opt, arg in opts:
    if opt == "--cfg":
        cfgFiles.append(arg)
        cfgTypes.append("cfg")
    if opt == "--py-cfg":
        cfgFiles.append(arg)
        cfgTypes.append("python")
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
    if opt == "--channel":
        channel = arg
#    if opt == "--label":
#        label = arg
    if opt == "--group":
        physicsGroup = arg
#    if opt == "--request-id":
#        requestId = arg
    if opt == "--starting-run":
        startingRun = arg
    if opt == "--starting-event":
        startingEvent = arg
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
    if opt == '--only-sites':
        onlySites = arg

    if opt == "--selection-efficiency":
        selectionEfficiency = arg

    if opt == '--pileup-dataset':
        pileupDS = arg
    if opt == '--pileup-files-per-job':
        pileupFilesPerJob = arg
    if opt == '--activity':
        activity = arg
    if opt == '--store-fail':
        if arg.lower() in ("true", "yes"):
            storeFail = True
        else:
            storeFail = False

    if opt == '--workflow_tag':
        workflow_tag = arg
    
if len(cfgFiles) == 0:
    msg = "--cfg option not provided: This is required"
    raise RuntimeError, msg
elif len(cfgFiles) > 1:
    print "%s cfgs listed - chaining them" % len(cfgFiles)
if versions == []:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg
if len(versions) != len(cfgFiles):
    msg = "Need same number of --cfg and --version arguments"
    raise RuntimeError, msg
if len(stageoutOutputs) != len(cfgFiles) - 1:
    msg = "Need one less --stageout-intermediates than --cfg arguments"
    raise RuntimeError, msg
if len(chainedInputs) and len(chainedInputs) != len(cfgFiles) - 1:
    msg = "Need one less chained-input than --cfg arguments"
    raise RuntimeError, msg

if channel == None:
    msg = "--channel option not provided: This is required"
    raise RuntimeError, msg

#  //
# // Set requestId and label
#//
#requestId="%s_%s" % (conditions,processingVersion)
if workflow_tag in (None,""):
   requestId="%s_%s" % (conditions,processingVersion)
else:
   requestId="%s_%s_%s" % (conditions,workflow_tag,processingVersion)

label=acquisitionEra

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
nodeNumber = 0
for cfgFile in cfgFiles:

    if cfgTypes[nodeNumber] == "cfg":
        from FWCore.ParameterSet.Config import include
        cmsCfg = include(cfgFile) 
    else:
        import imp
        modRef = imp.load_source( os.path.basename(cfgFile).replace(".py", ""),  cfgFile)
        cmsCfg = modRef.process
    
    cfgWrapper = CMSSWConfig()
    cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
    cfgInt.validateForProduction()

    if nodeNumber:
        try:
            inputModules = chainedInputs[nodeNumber-1]
        except IndexError:
            inputModules = []
        maker.chainCmsRunNode(stageoutOutputs[nodeNumber-1], *inputModules)
            
    maker.setCMSSWVersion(versions[nodeNumber])
    maker.setConfiguration(cfgWrapper, Type = "instance")
    maker.setOriginalCfg(file(cfgFile).read())
    maker.setPSetHash(WorkflowTools.createPSetHash(cfgFile))

    nodeNumber += 1

#  //
# // Pileup sample?
#//
if pileupDS != None:
    maker.addPileupDataset( pileupDS, pileupFilesPerJob)

maker.changeCategory(category)
maker.setAcquisitionEra(acquisitionEra)
maker.workflow.parameters['Conditions'] = conditions
maker.workflow.parameters['ProcessingVersion'] = processingVersion


  
spec = maker.makeWorkflow()

if activity is not None:
    spec.setActivity(activity)
if totalEvents is not None :
    spec.parameters['TotalEvents']=totalEvents
else :
    print "Warning: totalEvents parameter is not set!"
if eventsPerJob is not None :
    spec.parameters['EventsPerJob']=eventsPerJob
else :
    print "Warning: EventsPerJob parameter is not set!"
if startingRun is not None :
    spec.parameters['InitialRun']=startingRun
else :
    print "Warning: InitialRun (lumi) parameter is not set!"
if startingEvent is not None :
    spec.parameters['InitialEvent']=startingEvent
else :
    print "Warning: InitialEvent parameter is not set!"
if onlySites is not None:
    spec.parameters['OnlySites']=onlySites
if storeFail :
    spec.parameters['UseStoreFail'] = "True"

spec.save("%s-Workflow.xml" % maker.workflowName)


print "Created: %s-Workflow.xml" % maker.workflowName
print "From: %s " % cfgFile
print "Output Datasets:"
[ sys.stdout.write(
     "/%s/%s/%s\n" % (
       x['PrimaryDataset'],
       x['ProcessedDataset'],
       x['DataTier'])) for x in spec.outputDatasets()]




