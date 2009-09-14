#!/usr/bin/env python
"""
_createProductionCmsGenWorkflow_

Create a cmsGen and cmsRun workflow

"""
__version__ = "$Revision: 1.15 $"
__revision__ = "$Id: createProductionCmsGenWorkflow.py,v 1.15 2009/07/29 15:22:45 direyes Exp $"



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
         'processing_string=', 'workflow_tag=', 'override-initial-event=',
         'dbs-status=', 'datamixer-pu-ds='
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
usage += "                                  --datamixer-pu-ds=<Input Pile Up Dataset for DataMixing /PrimDS/ProcDS/Tier>\n"
usage += "                                  --only-sites=<Site>\n"
usage += "                                  --starting-run=<Starting run>\n"
usage += "                                  --starting-event=<Initial event>\n"
usage += "                                  --totalevents=<Total Events>\n"
usage += "                                  --eventsperjob=<Events/job>\n"
usage += "                                  --acquisition_era=<Acquisition Era>\n"
usage += "                                  --conditions=<Conditions>\n"
usage += "                                  --processing_version=<Processing version>\n"
usage += "                                  --processing_string=<Processing string>\n"
usage += "                                  --workflow_tag=<Workflow tag>\n"
usage += "                                  --override-initial-event=<Override Initial event>\n"
usage += "                                  --dbs-status=<VALID|PRODUCTION> Default: PRODUCTION\n"
usage += "\n"
usage += "You must have a scram runtime environment setup to use this tool\n"
usage += "since it will invoke EdmConfig tools\n\n"
usage += "Workflow Name is the name of the Workflow/Request/Primary Dataset\n"
usage += "to be used. \n"
usage += "It will default to the name of the cfg file if not provided\n\n"
usage += "Production category is a marker that will be added to LFNs, \n"
usage += "For example: PreProd, CSA06 etc etc. Defaults to mc\n"

usage += "\n  Options:\n"

options = \
"""
  --acquisition_era sets the aquisition era and the Primary Dataset name

  --activity=<activity>, The activity represented but this workflow
    i.e. Reprocessing, Skimming etc. (Default: PrivateProduction)

  --category is the processing category, eg PreProd, SVSuite, Skim etc. It
    defaults to 'mc' if not provided

  --channel allows you to specify a different primary dataset
    for the output.

  --chained-input=comma,separated,list,of,output,module,names Optional param
    that specifies the output modules to chain to the next input module. Defaults
    to all modules in a step, leave blank for all. If given should be specified
    for each step

  --cmsGenCfg generator config. file

  --cmsRunCfg python configuration file

  --conditions Deprecated

  --datamixer-pu-ds input dataset for the data mixer module.

  --dbs-status is the status flag the output datasets will have in DBS. If
    VALID, the datasets will be accesible by the physicists, If PRODUCTION, 
    the datasets will be hidden.

  --eventsperjob is the number of events to produce in a single batch job

  --group is the Physics group

  --only-sites allows you to restrict which fileblocks are used based on
    the site name, which will be the SE Name for that site
    Eg: --only-sites=site1,site2 will process only files that are available
    at the specified list of sites.

  --pileup-dataset is the input pileup dataset

  --pileup-files-per-job is the pileup files per job

  --processing_string sets the processing string

  --processing_version sets the processing version

  --selection-efficiency sets the efficiency

  --stageout-intermediates=<true|false>, Stageout intermediate files in
    chained processing

  --starting-run sets the first LUMI number.

  --starting-event sets the first event number.

  --totalevents sets the total number of events to be produced.

  --version is the version of the CMSSW to be used, you should also have done
    a scram runtime setup for this version

  --workflow_tag sets the workflow tag to distinguish e.g. RAW and RECO workflows
    for a given channel

"""

usage += options

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cmsRunCfgs = []
cmsGenCfg = None
versions = []
stageoutOutputs = []
chainedInputs = []
category = "mc"
label = "Test"
channel = None
physicsGroup = "Individual"
requestId = "%s" % (int(time.time()))
cfgTypes = []
selectionEfficiency = None
activity = 'PrivateProduction'
startingRun = 1
initialEvent = 1
totalEvents = 1000
eventsPerJob = 100
acquisitionEra = "Test"
conditions = "Bad"
processingVersion = "666"
onlySites = None
workflow_tag = None
pileupDS = None
pileupFilesPerJob = 1
dataMixDS = None
overrideInitialEvent = None
processingString = None
dbsStatus = 'PRODUCTION'

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
    if opt == '--datamixer-pu-ds':
        dataMixDS = arg
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
    if opt == "--processing_string":
        processingString = arg
    if opt == '--workflow_tag':
        workflow_tag = arg
    if opt == "--override-initial-event":
        overrideInitialEvent = arg
    if opt == '--dbs-status':
        if arg in ("VALID", "PRODUCTION"):
            dbsStatus = arg
   
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

requestId = processingVersion
if workflow_tag:
    requestId = "%s_%s" % (workflow_tag, requestId)
if processingString:
    requestId = "%s_%s" % (processingString, requestId)

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

    maker.setConfiguration(cfgWrapper, Type = "instance")
    maker.setCMSSWVersion(versions[nodeNumber])
    maker.setOriginalCfg(file(cmsRunCfg).read())
    maker.setPSetHash(WorkflowTools.createPSetHash(cmsRunCfg))

    nodeNumber += 1
    
maker.changeCategory(category)
maker.setNamingConventionParameters(acquisitionEra, processingString, processingVersion)
maker.workflow.parameters['Conditions'] = conditions
maker.setOutputDatasetDbsStatus(dbsStatus)
    

if selectionEfficiency != None:
    maker.addSelectionEfficiency(selectionEfficiency)
    maker.addCmsGenSelectionEfficiency(selectionEfficiency)

#  //
# // Pileup sample?
#//
if pileupDS != None:
    maker.addPileupDataset( pileupDS, pileupFilesPerJob)

#  //
# // DataMix pileup sample?
#//
if dataMixDS:
     maker.addPileupDataset(dataMixDS, 1, 'DataMixingModule')
 
spec = maker.makeWorkflow()

spec.parameters['TotalEvents']=totalEvents
spec.parameters['EventsPerJob']=eventsPerJob
spec.parameters['InitialEvent']=initialEvent
spec.parameters['InitialRun']=startingRun
if overrideInitialEvent not in (None, ""):
    spec.parameters['OverrideInitialEvent']=overrideInitialEvent
if onlySites != None:
   spec.parameters['OnlySites']=onlySites

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
