#!/usr/bin/env python
"""
_createPreProdWorkflow_

Create a preprod workflow using a configuration PSet.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: createProductionWorkflow.py,v 1.2 2006/11/21 14:47:12 afanfani Exp $"


import os
import sys
import getopt
import popen2
import time

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import unmergedLFNBase, mergedLFNBase
from ProdCommon.CMSConfigTools.CfgInterface import CfgInterface
from ProdCommon.MCPayloads.DatasetExpander import splitMultiTier
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
import ProdCommon.MCPayloads.UUID as MCPayloadsUUID

valid = ['cfg=', 'version=', 'category=', 'name=', 'fake-hash',
         'pileup-dataset=', 'pileup-files-per-job=',
         'pu-dbs-address=', 'pu-dbs-url=', 'pu-dls-type=', 'pu-dls-address=',
         'pu-skip-location',]
usage = "Usage: createPreProdWorkflow.py --cfg=<cfgFile>\n"
usage += "                                --version=<CMSSW version>\n"
usage += "                                --name=<Workflow Name>\n"
usage += "                                --category=<Production category>\n"
usage += "                                --fake-hash\n"
usage += "You must have a scram runtime environment setup to use this tool\n"
usage += "since it will invoke EdmConfig tools\n\n"
usage += "Workflow Name is the name of the Workflow/Request/Primary Dataset\n"
usage += "to be used. \n"
usage += "It will default to the name of the cfg file if not provided\n\n"
usage += "Production category is a marker that will be added to LFNs, \n"
usage += "For example: PreProd, CSA06 etc etc. Defaults to PreProd\n"

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cfgFile = None
prodName = None
version = None
category = "mc"
timestamp = int(time.time())
fakeHash = False
pileupDS = None
pileupFilesPerJob = 1
dbsAddress = None
dbsUrl = None
dlsAddress = None
dlsType = None
pileupSkipLocation = False

for opt, arg in opts:
    if opt == "--cfg":
        cfgFile = arg
    if opt == "--version":
        version = arg
    if opt == "--category":
        category = arg
    if opt == "--name":
        prodName = arg
    if opt == "--fake-hash":
        fakeHash = True

    if opt == '--pileup-dataset':
        pileupDS = arg
    if opt == '--pileup-files-per-job':
        pileupFilesPerJob = arg   
    if opt == '--pu-dbs-address':
        dbsAddress = arg
    if opt == '--pu-dbs-url':
        dbsUrl = arg
    if opt == '--pu-dls-type':
        dlsType = arg
    if opt == '--pu-dls-address':
        dlsAddress = arg
    if opt == '--pu-skip-location':
        pileupSkipLocation = True
    
if cfgFile == None:
    msg = "--cfg option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

if prodName == None:
    prodName = os.path.basename(cfgFile)
    prodName = prodName.replace(".cfg", "")

pycfgFile = "%s.pycfg" % prodName
hashFile = "%s.hash" % prodName

if not os.path.exists(cfgFile):
    msg = "Cfg File Not Found: %s" % cfgFile
    raise RuntimeError, msg

#  //
# // Are we using a custom dbs/dls for pileup??
#//
dbsdlsInfo = {'--pu-dbs-address' : dbsAddress,
              '--pu-dls-address' : dlsAddress,
              '--pu-dls-type' : dlsType,
              '--pu-dbs-url' : dbsUrl}
customDBSDLS = False
if dbsdlsInfo.values() != [None, None, None, None]:
    customDBSDLS = True
    for key, val in dbsdlsInfo.items():
        if val == None:
            msg = "Missing Argument for Pileup DBS/DLS Custom setup:\n"
            msg += "%s Is not set" % key
            msg += "For Pileup DBS/DLS you must supply all of:\n"
            msg += "%s\n" % dbsdlsInfo.keys()
            raise RuntimeError, msg
    

#  //
# // Generate python cfg file
#//
pycfgFile = WorkflowTools.createPythonConfig(cfgFile)


#  //
# // Generate PSet Hash
#//
RealPSetHash = WorkflowTools.createPSetHash(cfgFile)

#  // 
# // Create a new WorkflowSpec and set its name
#//
spec = WorkflowSpec()
spec.setWorkflowName(prodName)
spec.setRequestCategory(category)
spec.setRequestTimestamp(timestamp)

#  //
# // This value was created by running the EdmConfigHash tool
#//  on the original cfg file.


cmsRun = spec.payload
cmsRun.name = "cmsRun1" # every node in the workflow needs a unique name
cmsRun.type = "CMSSW"   # Nodes that are CMSSW based should set the name
cmsRun.application["Project"] = "CMSSW" # project
cmsRun.application["Version"] = version # version
cmsRun.application["Architecture"] = "slc3_ia32_gcc323" # arch (not needed)
cmsRun.application["Executable"] = "cmsRun" # binary name
cmsRun.configuration = file(pycfgFile).read() # Python PSet file


#  //
# // Pileup sample?
#//
if pileupDS != None:
    puPrimary = pileupDS.split("/")[1]
    puTier = pileupDS.split("/")[2]
    puProc = pileupDS.split("/")[3]
    puDataset = cmsRun.addPileupDataset(puPrimary, puTier, puProc)
    puDataset['FilesPerJob'] = pileupFilesPerJob
    if customDBSDLS:
        puDataset['DBSAddress'] = dbsAddress
        puDataset['DBSURL'] = dbsUrl
        puDataset['DLSType'] = dlsType
        puDataset['DLSAddress'] = dlsAddress
    if pileupSkipLocation:
        puDataset['SkipLocation'] = pileupSkipLocation

#  //
# // Pull all the output modules from the configuration file,
#//  treat the output module name as DataTier and AppFamily,
#  //since there isnt a standard way to derive these things yet.
# //    
#//  For each module a dataset declaration is created in the spec
cfgInt = CfgInterface(cmsRun.configuration, True)
datasetList = []
for outModName, val in cfgInt.outputModules.items():
    #  //
    # // Check for Multi Tiers.
    #//  If Output module contains - characters, we split based on it
    #  //And create a different tier for each basic tier
    # //
    #//
    datasets = val.datasets()
    for outDataset in datasets:
        dataTier = outDataset['dataTier']

        processedDS = "%s-%s-%s-unmerged" % (
            cmsRun.application['Version'], outModName, timestamp)

        if outDataset.has_key("processedDataset"):
            processedDS = outDataset['processedDataset']

        primaryName = prodName
        if outDataset.has_key("primaryDataset"):
            primaryName = outDataset['primaryDataset']
        

        outDS = cmsRun.addOutputDataset(primaryName, 
                                        processedDS,
                                        outModName)
                                        
        outDS['DataTier'] = dataTier
        outDS["ApplicationName"] = cmsRun.application["Executable"]
        outDS["ApplicationProject"] = cmsRun.application["Project"]
        outDS["ApplicationVersion"] = cmsRun.application["Version"]
        outDS["ApplicationFamily"] = outModName
        if fakeHash:
            guid = MCPayloadsUUID.uuidgen()
            if guid == None:
                guid = MCPayloadsUUID.uuid()
            hashValue = "hash=%s;guid=%s" % (RealPSetHash, guid)
            outDS['PSetHash'] = hashValue
        else:
            outDS['PSetHash'] = RealPSetHash
        datasetList.append(outDS.name())

stageOut = cmsRun.newNode("stageOut1")
stageOut.type = "StageOut"
stageOut.application["Project"] = ""
stageOut.application["Version"] = ""
stageOut.application["Architecture"] = ""
stageOut.application["Executable"] = "RuntimeStageOut.py" # binary name
stageOut.configuration = ""

mergedLFNBase(spec)
unmergedLFNBase(spec)

spec.save("%s-Workflow.xml" % prodName)


print "Created: %s-Workflow.xml" % prodName
print "Created: %s " % pycfgFile
print "Created: %s " % hashFile
print "From: %s " % cfgFile
print "Output Datasets:"
for item in datasetList:
    print " ==> %s" % item





