#!/usr/bin/env python
"""
_createPreProdWorkflow_

Create a preprod workflow using a configuration PSet.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: createPreProdWorkflow.py,v 1.1 2006/05/02 14:32:09 evansde Exp $"


import os
import sys
import getopt
import popen2

from MCPayloads.WorkflowSpec import WorkflowSpec
from CMSConfigTools.CfgInterface import CfgInterface



valid = ['cfg=', 'version=' ]
usage = "Usage: createPreProdWorkflow.py --cfg=<cfgFile>\n"
usage += "                                --version=<CMSSW version>\n"
usage += "You must have a scram runtime environment setup to use this tool\n"
usage += "since it will invoke EdmConfig tools\n"

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cfgFile = None
prodName = None
version = None

for opt, arg in opts:
    if opt == "--cfg":
        cfgFile = arg
    if opt == "--version":
        version = arg

if cfgFile == None:
    msg = "--cfg option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

prodName = os.path.basename(cfgFile)
prodName = prodName.replace(".cfg", "")
pycfgFile = "%s.pycfg" % prodName
hashFile = "%s.hash" % prodName

if not os.path.exists(cfgFile):
    msg = "Cfg File Not Found: %s" % cfgFile
    raise RuntimeError, msg

#  //
# // Cleanup existing files
#//
if os.path.exists(pycfgFile):
    os.remove(pycfgFile)
if os.path.exists(hashFile):
    os.remove(hashFile)

#  //
# // Generate python cfg file
#//
pop = popen2.Popen4("EdmConfigToPython < %s > %s " % (cfgFile, pycfgFile))
while pop.poll() == -1:
    exitStatus = pop.poll()
exitStatus = pop.poll()
if exitStatus:
    msg = "Error creating Python cfg file:\n"
    msg += pop.fromchild.read()
    raise RuntimeError, msg


#  //
# // Generate PSet Hash
#//
pop = popen2.Popen4("EdmConfigHash < %s > %s " % (cfgFile, hashFile))
while pop.poll() == -1:
    exitStatus = pop.poll()
exitStatus = pop.poll()
if exitStatus:
    msg = "Error creating PSet Hash file:\n"
    msg += pop.fromchild.read()
    raise RuntimeError, msg

#  //
# // Existence checks for created files
#//
for item in (cfgFile, pycfgFile, hashFile):
    if not os.path.exists(item):
        msg = "File Not Found: %s" % item
        raise RuntimeError, msg

#  //
# // Check that python file is valid
#//
pop = popen2.Popen4("python %s" % pycfgFile) 
while pop.poll() == -1:
    exitStatus = pop.poll()
exitStatus = pop.poll()
if exitStatus:
    msg = "Error importing Python cfg file:\n"
    msg += pop.fromchild.read()
    raise RuntimeError, msg

#  // 
# // Create a new WorkflowSpec and set its name
#//
spec = WorkflowSpec()
spec.setWorkflowName(prodName)

#  //
# // This value was created by running the EdmConfigHash tool
#//  on the original cfg file.
PSetHashValue = file(hashFile).read()

cmsRun = spec.payload
cmsRun.name = "cmsRun1" # every node in the workflow needs a unique name
cmsRun.type = "CMSSW"   # Nodes that are CMSSW based should set the name
cmsRun.application["Project"] = "CMSSW" # project
cmsRun.application["Version"] = version # version
cmsRun.application["Architecture"] = "slc3_ia32_gcc323" # arch (not needed)
cmsRun.application["Executable"] = "cmsRun" # binary name
cmsRun.configuration = file(pycfgFile).read() # Python PSet file

#  //
# // Pull all the output modules from the configuration file,
#//  treat the output module name as DataTier and AppFamily,
#  //since there isnt a standard way to derive these things yet.
# //    
#//  For each module a dataset declaration is created in the spec
cfgInt = CfgInterface(cmsRun.configuration, True)
for key, val in cfgInt.outputModules.items():
    #                               primary     DT   Processed
    outDS = cmsRun.addOutputDataset(prodName, key, key)
    outDS['DataTier'] = key
    outDS["ApplicationName"] = cmsRun.application["Executable"]
    outDS["ApplicationProject"] = cmsRun.application["Project"]
    outDS["ApplicationVersion"] = cmsRun.application["Version"]
    outDS["ApplicationFamily"] = key
    outDS['PSetHash'] = PSetHashValue
    
stageOut = cmsRun.newNode("stageOut1")
stageOut.type = "StageOut"
stageOut.application["Project"] = ""
stageOut.application["Version"] = ""
stageOut.application["Architecture"] = ""
stageOut.application["Executable"] = "RuntimeStageOut.py" # binary name
stageOut.configuration = ""


spec.save("%s-Workflow.xml" % prodName)


print "Created: %s-Workflow.xml" % prodName
print "Created: %s " % pycfgFile
print "Created: %s " % hashFile
print "From: %s " % cfgFile



