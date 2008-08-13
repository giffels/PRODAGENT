#!/usr/bin/env python
"""
_createPreProdWorkflow_

Create a preprod workflow using a configuration PSet.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: createProductionWorkflow_CSA08Hack.py,v 1.1 2008/06/18 20:00:42 dmason Exp $"


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
         'pileup-dataset=', 'pileup-files-per-job=','only-sites=',
         'selection-efficiency=','starting-run=','totalevents=',
         'eventsperjob=', 'acquisition_era=', 'conditions=', 'processing_version='
         ]

usage = "Usage: createProductionWorkflow.py --cfg=<cfgFile>\n"
usage += "                                  --version=<CMSSW version>\n"
usage += "                                  --channel=<Phys Channel/Primary Dataset>\n"
usage += "                                  --group=<Physics Group>\n"
#usage += "                                  --request-id=<Request ID>\n"
#usage += "                                  --label=<Production Label>\n"
usage += "                                  --category=<Production category>\n"
usage += "                                  --starting-run=<Starting run>\n"
usage += "                                  --totalevents=<Total Events>\n"
usage += "                                  --eventsperjob=<Events/job>\n"
usage += "                                  --acquisition_era=<Acquisition Era>\n"
usage += "                                  --conditions=<Conditions>\n"
usage += "                                  --processing_version=<Processing version>\n"
usage += "                                  --only-sites=<Site>\n"
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

cfgFile = None
requestId = "%s" % (int(time.time()))
physicsGroup = "Individual"
label = "Test"
version = None
category = "mc"
channel = None
cfgType = "cfg"
startingRun=1
totalEvents=1000
eventsPerJob=100
acquisitionEra="Test"
conditions="Bad"
processingVersion=666
onlySites=None

pileupDS = None
pileupFilesPerJob = 1

selectionEfficiency = None

for opt, arg in opts:
    if opt == "--cfg":
        cfgFile = arg
        cfgType = "cfg"
    if opt == "--py-cfg":
        cfgFile = arg
        cfgType = "python"
    if opt == "--version":
        version = arg
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

    if opt == "--selection-efficiency":
        selectionEfficiency = arg

    if opt == '--pileup-dataset':
        pileupDS = arg
    if opt == '--pileup-files-per-job':
        pileupFilesPerJob = arg   
    if opt == '--only-sites':
        onlySites = arg
   
    
if cfgFile == None:
    msg = "--cfg option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

if channel == None:
    msg = "--channel option not provided: This is required"
    raise RuntimeError, msg


requestId="%s_%s" % (conditions,processingVersion)
label=acquisitionEra

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

if cfgType == "cfg":
    from FWCore.ParameterSet.Config import include
    cmsCfg = include(cfgFile) 
else:
    import imp
    modRef = imp.load_source( os.path.basename(cfgFile).replace(".py", ""),  cfgFile)
    cmsCfg = modRef.process
    
cfgWrapper = CMSSWConfig()
cfgWrapper.originalCfg = file(cfgFile).read()
cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
cfgInt.validateForProduction()

#  //
# // Instantiate a WorkflowMaker
#//
maker = WorkflowMaker(requestId, channel, label )

maker.setCMSSWVersion(version)
maker.setPhysicsGroup(physicsGroup)
maker.setConfiguration(cfgWrapper, Type = "instance")
#from tempfile import NamedTemporaryFile
#tmpCfg = NamedTemporaryFile()
#tmpCfg.write(cmsCfg.dumpConfig())
#tmpCfg.flush()
maker.setPSetHash(WorkflowTools.createPSetHash(cfgFile))
maker.changeCategory(category)
maker.setAcquisitionEra(acquisitionEra)
maker.workflow.parameters['Conditions'] = conditions
maker.workflow.parameters['ProcessingVersion'] = processingVersion


if selectionEfficiency != None:
    maker.addSelectionEfficiency(selectionEfficiency)


#  //
# // Pileup sample?
#//
if pileupDS != None:
    maker.addPileupDataset( pileupDS, pileupFilesPerJob)
  
spec = maker.makeWorkflow()

spec.parameters['TotalEvents']=totalEvents
spec.parameters['EventsPerJob']=eventsPerJob
spec.parameters['InitialRun']=startingRun
if onlySites != None:
   spec.parameters['OnlySites']=onlySites

spec.save("%s-Workflow.xml" % maker.workflowName)


print "Created: %s-Workflow.xml" % maker.workflowName
print "From: %s " % cfgFile
print "Output Datasets:"
[ sys.stdout.write(
     "/%s/%s/%s\n" % (
       x['PrimaryDataset'],
       x['ProcessedDataset'],
       x['DataTier'])) for x in spec.outputDatasets()]




