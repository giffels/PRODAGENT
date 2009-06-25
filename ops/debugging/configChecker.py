#!/usr/bin/env python
"""
_configChecker_

Do the minimal bit that create*Workflow.py do to check if a config works for us

"""

import os
import sys
import getopt
import popen2
import time
import imp


import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions

usage="python configChecker.py --cfg=configname.py"

valid = ['cfg=']

cfgFiles = []

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

if len(opts)==0:
    print "gimme arguments: %s"%usage
    sys.exit(1)

for opt, arg in opts:
    if opt == "--cfg":
        cfgFiles.append(arg)


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


for cfgFile in cfgFiles:
    try:
        print "trying: %s"%cfgFile
        modRef = imp.load_source( os.path.basename(cfgFile).replace(".py", ""), 
 cfgFile)
        cmsCfg = modRef.process
        cfgWrapper = CMSSWConfig()
        cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
        cfgInt.validateForProduction()
        print "worked..."
    except Exception, ex:
#        print "%s failed with \n\n%s" % (cfgFile,str(ex))
        print "%s failed"%cfgFile

