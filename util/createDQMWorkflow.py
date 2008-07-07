#!/usr/bin/env python
"""
_createPreProdWorkflow_

Create a preprod workflow using a configuration PSet.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: createDQMWorkflow.py,v 1.1 2008/04/10 16:17:16 evansde Exp $"


import os
import sys
import getopt
import popen2
import time
import imp

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig


valid = ['cfg=', 'py-cfg=', 'version=', 'dataset=', "site="]


usage = \
"""

Usage: createProductionWorkflow.py --cfg=<cfgFile>
                                   --version=<CMSSW version>
                                   --dataset=<dataset name>
                                   --site=<site SE name>


cfgName - Harvesting config file
version - version of CMSSW to use for harvesting jobs
dataset - The input dataset from which histograms will be harvested
site    - location where the job will run. Note that this is used only to
          queue the job for the site, it doesnt affect the job definition,
          current logic assumes all data at the same site.

"""

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cfgFile = None
requestId = "OfflineDQM-%s" % int(time.time())
physicsGroup = "OfflineDQM"
label = "DQMHarvest"
version = os.environ['CMSSW_VERSION']
category = "DQM"
channel = "DQM"
cfgType = "cfg"
dataset = None
sitename = None

for opt, arg in opts:
    if opt == "--cfg":
        cfgFile = arg
        cfgType = "cfg"
    if opt == "--py-cfg":
        cfgFile = arg
        cfgType = "python"
    if opt == "--version":
        version = arg

    if opt == "--dataset":
        dataset = arg
    if opt == "--site":
        sitename = arg

if cfgFile == None:
    msg = "--cfg option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

if channel == None:
    msg = "--channel option not provided: This is required"
    raise RuntimeError, msg



if not os.path.exists(cfgFile):
    msg = "Cfg File Not Found: %s" % cfgFile
    raise RuntimeError, msg

if dataset == None:
    msg = "No dataset provided"
    raise RuntimeError, msg

if sitename == None:
    msg = "No site provided"
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
    modRef = imp.find_module( os.path.basename(cfgFile).replace(".py", ""),  os.path.dirname(cfgFile))
    cmsCfg = modRef.process

cfgWrapper = CMSSWConfig()
cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
#cfgInt.validateForProduction()

#  //
# // Instantiate a WorkflowMaker
#//
maker = WorkflowMaker(requestId, channel, label )

maker.setCMSSWVersion(version)
maker.setPhysicsGroup(physicsGroup)
maker.setConfiguration(cfgWrapper, Type = "instance")
maker.changeCategory(category)
maker.setPSetHash("NO_HASH")
maker.addInputDataset(dataset)

spec = maker.makeWorkflow()
spec.parameters['DBSURL'] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
spec.parameters['OnlySites'] = sitename
spec.payload.scriptControls['PostTask'].append("JobCreator.RuntimeTools.RuntimeOfflineDQM")

spec.save("%s-Workflow.xml" % maker.workflowName)


print "Created: %s-Workflow.xml" % maker.workflowName
print "From: %s " % cfgFile



