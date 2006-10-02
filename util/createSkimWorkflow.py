#!/usr/bin/env python
"""
_createSkimWorkflow_

Create a workflow that processes an input dataset with a cfg file

"""

import os
import sys
import getopt
import popen2
import time


from MCPayloads.WorkflowSpec import WorkflowSpec
from MCPayloads.LFNAlgorithm import mergedLFNBase,unmergedLFNBase
from CMSConfigTools.CfgInterface import CfgInterface
from MCPayloads.DatasetExpander import splitMultiTier

valid = ['cfg=', 'version=', 'category=', 'name=', 'dataset=',
         'split-type=', 'split-size=',
         'only-blocks=', 'only-sites=',
         'dbs-address=', 'dbs-url=', 'dls-type=', 'dls-address=',
         
         ]


usage = "Usage: createPreProdWorkflow.py --cfg=<cfgFile>\n"
usage += "                                --version=<CMSSW version>\n"
usage += "                                --name=<Workflow Name>\n"
usage += "                                --category=<Processing category>\n"
usage += "                                --dataset=<Dataset to process>\n"
usage += "                                --split-type=<event|file>\n"
usage += "                                --split-size=<Integer split size>\n"
usage += "   Options:\n"
usage += "                                --only-blocks=<List of fileblocks>\n"
usage += "                                --only-sites=<List of sites>\n"
usage += "                                --dbs-address=<DBSAddress>\n"
usage += "                                --dbs-url=<DBSUrl>\n"
usage += "                                --dls-address=<DLSAddress>\n"
usage += "                                --dls-type=<DLSType>\n"


options = \
"""
  --cfg is the path to the cfg file to be used for the skimming cmsRun task
  --version is the version of the CMSSW to be used, you should also have done
    a scram runtime setup for this version
  --name is the name of the request/workflow
  --category is the processing category, eg PreProd, SVSuite, Skim etc. It
    defaults to PreProd if not provided
  --dataset is the input dataset to be processed
  --split-type should be either file or event. file means split the jobs based
    on file boundaries. This means jobs with have N files per job, where N is
    the value of --split-size. If this is events, then each job will contain N
    events (regardless of file boundaries) where N is the value of --split-size
  --split-size is an integer that defines the size of jobs. If --split-type is
    set to files, then it is the number of files per job. If --split-type is
    set to events, then it is the number of events per job

  --only-blocks allows you to specify a comma seperated list of blocks that
    will be imported if you dont want the entire dataset.
    Eg: --only-blocks=blockname1,blockname2,blockname3 will process only files
    belonging to the named blocks.
  --only-sites allows you to restrict which fileblocks are used based on
    the site name, which will be the SE Name for that site
    Eg: --only-sites=site1,site2 will process only files that are available
    at the specified list of sites.

  Specifying a DBS/DLS containing the dataset. Usually data is looked up
  in the ProdAgents own DBS/DLS. If you want a dataset from different DBS/DLS
  instances (Eg the Global ones) you need to supply ALL of the following four
  arguments:

   --dbs-address=DBSInstance Eg: MCLocal/Writer
   --dbs-url=DBS Url, The URL of the DBS Service
   --dls-address=DLSInstance, the DLS address
   --dls-type=DLSType, the Type of DLS you are using.
    
"""


usage += options


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cfgFile = None
prodName = None
version = None
dataset = None
splitType = None
splitSize = None
category = "PreProd"
timestamp = int(time.time())
onlyBlocks = None
onlySites = None
dbsAddress = None
dbsUrl = None
dlsAddress = None
dlsType = None

primaryDataset = None
dataTier = None
processedDataset = None

for opt, arg in opts:
    if opt == "--cfg":
        cfgFile = arg
    if opt == "--version":
        version = arg
    if opt == "--category":
        category = arg
    if opt == "--name":
        prodName = arg
    if opt == "--dataset":
        dataset = arg
    if opt == "--split-type":
        splitType = arg
    if opt == "--split-size":
        splitSize = arg
    if opt == "--only-blocks":
        onlyBlocks = arg
    if opt == "--only-sites":
        onlySites = arg
    if opt == '--dbs-address':
        dbsAddress = arg
    if opt == '--dbs-url':
        dbsUrl = arg
    if opt == '--dls-type':
        dlsType = arg
    if opt == '--dls-address':
        dlsAddress = arg

if cfgFile == None:
    msg = "--cfg option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

if dataset == None:
    msg = "--dataset option not provided: This is required"
    raise RuntimeError, msg
if splitType == None:
    msg = "--split-type option not provided: This is required"
    raise RuntimeError, msg
if splitType not in ("event", "file"):
    msg = "Invalid argument for --split-type: %s\n" % splitType
    msg += "Must be either event or file\n"
    raise RuntimeError, msg
    
if splitSize == None:
    msg = "--split-size option not provided: This is required"
    raise RuntimeError, msg

try:
    splitSize = int(splitSize)
except ValueError, ex:
    msg = "--split-size argument is not an integer: %s\n" % splitSize
    raise RuntimeError, msg



#  //
# // Are we using a custom dbs/dls??
#//
dbsdlsInfo = {'--dbs-address' : dbsAddress,
              '--dls-address' : dlsAddress,
              '--dls-type' : dlsType,
              '--dls-url' : dbsUrl}
customDBSDLS = False
if dbsdlsInfo.values() != [None, None, None, None]:
    customDBSDLS = True
    for key, val in dbsdlsInfo.items():
        if val == None:
            msg = "Missing Argument for DBS/DLS Custom setup:\n"
            msg += "%s Is not set" % key
            msg += "For Custom DBS/DLS you must supply all of:\n"
            msg += "%s\n" % dbsdlsInfo.keys()
            raise RuntimeError, msg
    


if prodName == None:
    prodName = os.path.basename(cfgFile)
    prodName = prodName.replace(".cfg", "")

pycfgFile = "%s.pycfg" % prodName
hashFile = "%s.hash" % prodName

datasetOrig = dataset
while dataset.startswith("/"):
    dataset = dataset[1:]
datasetSplit = dataset.split("/")
if len(datasetSplit) != 3:
    msg = "Cant extract primary, processed and data tier from dataset:\n"
    msg += datasetOrig
    raise RuntimeError, msg
primaryDataset = datasetSplit[0]
dataTier = datasetSplit[1]
processedDataset = datasetSplit[2]

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



spec = WorkflowSpec()
# set its properties
spec.setWorkflowName(prodName)
spec.setRequestCategory(category)
spec.setRequestTimestamp(timestamp)

spec.parameters['SplitType'] = splitType
spec.parameters['SplitSize'] = splitSize

if onlyBlocks != None:
    spec.parameters['OnlyBlocks'] = onlyBlocks
if onlySites != None:
    spec.parameters['OnlySites'] = onlySites

if customDBSDLS:
    spec.parameters['DBSAddress'] = dbsAddress
    spec.parameters['DBSURL'] = dbsUrl
    spec.parameters['DLSType'] = dlsType
    spec.parameters['DLSAddress'] = dlsAddress

#  //
# // This value was created by running the EdmConfigHash tool
#//  on the original cfg file.
PSetHashValue = file(hashFile).read()

cmsRun = spec.payload
cmsRun.name = "cmsRun1"
cmsRun.type = "CMSSW"
cmsRun.application["Project"] = "CMSSW"
cmsRun.application["Version"] = "CMSSW_0_8_3"
cmsRun.application["Architecture"] = "slc3_ia32_gcc323"
cmsRun.application["Executable"] = "cmsRun"
cfg =  file(pycfgFile).read()
cmsRun.configuration = str(cfg)


# input dataset (primary, processed)
inputDataset = cmsRun.addInputDataset(primaryDataset, processedDataset)
inputDataset["DataTier"] = dataTier



cfgInt = CfgInterface(cmsRun.configuration, True)
datasetList = []
for outModName, val in cfgInt.outputModules.items():
    #  //
    # // Check for Multi Tiers.
    #//  If Output module contains - characters, we split based on it
    #  //And create a different tier for each basic tier
    # //
    #//

    tierList = splitMultiTier(outModName)    
    for outDataTier in tierList:
        processedDS = "%s-%s-%s" % (
            cmsRun.application['Version'], outModName, timestamp)
        outDS = cmsRun.addOutputDataset(prodName, 
                                        processedDS,
                                        outModName)
        outDS['DataTier'] = outDataTier
        outDS["ApplicationName"] = cmsRun.application["Executable"]
        outDS["ApplicationProject"] = cmsRun.application["Project"]
        outDS["ApplicationVersion"] = cmsRun.application["Version"]
        outDS["ApplicationFamily"] = outModName
        outDS['PSetHash'] = PSetHashValue
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
print "Input Dataset: %s " % datasetOrig
print "  ==> Will be split by %s in increments of %s" % (splitType, splitSize)
print "Output Datasets:"
for item in datasetList:
    print " ==> %s" % item
