#!/usr/bin/env python
"""
_createTier0ProductionWorkflow_

Create a workflow to create streamer MC files for Tier0 processing.

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: createTier0ProductionWorkflow.py,v 1.2 2009/04/07 23:27:41 hufnagel Exp $"


import os
import sys
import time
import getopt
import imp

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker

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

#  //
# // Instantiate a WorkflowMaker
#//

maker = WorkflowMaker("%s-%s" % (os.environ['USER'], int(time.time())),
                      "Tier0Feed", acquisitionEra)

# ?
maker.setPhysicsGroup("DummyPhysicsGroup")

# part of the LFN ?
maker.changeCategory("mc")

#
# load CMSSW libraries
#
loader = CMSSWAPILoader("slc4_ia32_gcc345",
                        version,
                        "/afs/cern.ch/cms/sw")

try:
    loader.load()
except Exception, ex:
    msg = "Couldn't load CMSSW libraries: %s" % ex
    raise RuntimeError, msg

loadedModule = imp.load_source( os.path.basename(cfgFile).replace(".py", ""), cfgFile )

cfgInterface = CMSSWConfig()
loadedConfig = cfgInterface.loadConfiguration(loadedModule.process)
loadedConfig.validateForProduction()

loader.unload()

maker.cmsRunNodes[0].scriptControls["PostExe"].append(
    "JobCreator.RuntimeTools.RuntimeStreamerToFJR"
    )

maker.setCMSSWVersion(version)
maker.setConfiguration(cfgInterface, Type = "instance")
maker.setOriginalCfg(file(cfgFile).read())
maker.setPSetHash("NO_PSET_HASH")

maker.setNamingConventionParameters(acquisitionEra, None, processingVersion)

maker.workflow.parameters['Conditions'] = conditions

spec = maker.makeWorkflow()

maker.workflow.parameters['MergedLFNBase'] = "/T0/hufnagel/"
maker.workflow.parameters['UnmergedLFNBase'] = maker.workflow.parameters['MergedLFNBase']

maker.workflow.parameters['StreamerIndexDir'] = "vocms13:/data/hufnagel/parepack/StreamerIndexDir"


spec.save("%s-Workflow.xml" % maker.workflowName)

print "Created: %s-Workflow.xml" % maker.workflowName
print "From: %s " % cfgFile
print "Output Datasets:"
[ sys.stdout.write(
     "/%s/%s/%s\n" % (
       x['PrimaryDataset'],
       x['ProcessedDataset'],
       x['DataTier'])) for x in spec.outputDatasets()]

