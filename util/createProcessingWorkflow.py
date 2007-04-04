#!/usr/bin/env python
"""
_createProcessingWorkflow_

Create a workflow that processes an input dataset with a cfg file

"""

import os
import sys
import getopt
import popen2
import time

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions

valid = ['cfg=', 'version=', 'category=', 'label=',
         'channel=', 'group=', 'request-id=',
         'dataset=',
         'split-type=', 'split-size=',
         'only-blocks=', 'only-sites=',
         'only-closed-blocks',
         'dbs-url=', 
         'pileup-dataset=', 'pileup-files-per-job=',
         
         ]


usage = "Usage: createProcessingWorkflow.py --cfg=<cfgFile>\n"
usage += "                                  --version=<CMSSW version>\n"
usage += "                                  --channel=<Phys Channel/Primary Dataset>\n"
usage += "                                  --group=<Physics Group>\n"
usage += "                                  --request-id=<Request ID>\n"
usage += "                                  --label=<Production Label>\n"
usage += "                                  --category=<Production category>\n"
usage += "\n"
usage += "                                --dataset=<Dataset to process>\n"
usage += "                                --split-type=<event|file>\n"
usage += "                                --split-size=<Integer split size>\n"
usage += "   Options:\n"
usage += "                                --only-blocks=<List of fileblocks>\n"
usage += "                                --only-sites=<List of sites>\n"
usage += "                                --dbs-url=<DBSUrl>\n"





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

  --same-primary-dataset  Switch means that the output datasets will be
    added to the same primary dataset as the input dataset if provided.
    If not provided, then a completely new Primary dataset will be created
    using the --name value.

  --only-closed-blocks  Switch that will mean that open blocks are ignored
    by the dataset injector.

  --dbs-url=DBS Url, The URL of the DBS Service containing the input dataset
    
"""


usage += options


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cfgFile = None
version = None
requestId = "%s-%s" % (os.environ['USER'], int(time.time()))
physicsGroup = "Individual"
label = "Test"
category = "mc"
channel = None
selectionEfficiency = None

dataset = None
splitType = None
splitSize = None


onlyBlocks = None
onlySites = None
dbsUrl = None
onlyClosedBlocks = False

pileupDataset = None
pileupFilesPerJob = 1






for opt, arg in opts:
    if opt == "--cfg":
        cfgFile = arg
    if opt == "--version":
        version = arg
    if opt == "--category":
        category = arg

    if opt == "--channel":
        channel = arg
    if opt == "--label":
        label = arg
    if opt == "--group":
        physicsGroup = arg
    if opt == "--request-id":
        requestId = arg

    if opt == "--selection-efficiency":
        selectionEfficiency = arg

    if opt == "--dataset":
        dataset = arg
    if opt == "--split-type":
        splitType = arg
    if opt == "--split-size":
        splitSize = arg
    if opt == "--only-blocks":
        onlyBlocks = arg
    if opt == "--only-closed-blocks":
        onlyClosedBlocks = True
    if opt == "--only-sites":
        onlySites = arg
    if opt == '--dbs-url':
        dbsUrl = arg
    if opt == '--pileup-dataset':
        pileupDataset = arg
    if opt == '--pileup-files-per-job':
        pileupFilesPerJob = arg
        
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


if channel == None:
    #  //
    # // Assume same as input
    #//
    channel = DatasetConventions.parseDatasetPath(dataset)['Primary']
    

if not os.path.exists(cfgFile):
    msg = "Cfg File Not Found: %s" % cfgFile
    raise RuntimeError, msg



RealPSetHash = WorkflowTools.createPSetHash(cfgFile)


maker = WorkflowMaker(requestId, channel, label )

maker.setCMSSWVersion(version)
maker.setPhysicsGroup(physicsGroup)
maker.setConfiguration(cfgFile, Format = "cfg", Type = "file")
maker.setPSetHash(WorkflowTools.createPSetHash(cfgFile))
maker.changeCategory(category)

if selectionEfficiency != None:
    maker.addSelectionEfficiency(selectionEfficiency)


#  //
# // Pileup sample?
#//
if pileupDataset != None:
    maker.addPileupDataset(pileupDataset, pileupFilesPerJob)

#  //
# // Input Dataset
#//
maker.addInputDataset(dataset)

maker.inputDataset['SplitType'] = splitType
maker.inputDataset['SplitSize'] = splitSize

if onlySites != None:
    maker.inputDataset['OnlySites'] = onlySites
if onlyBlocks != None:
    maker.inputDataset['OnlyBlocks'] = onlyBlocks

if onlyClosedBlocks:
    maker.inputDataset['OnlyClosedBlocks'] = True


if dbsUrl != None:
    maker.workflow.parameters['DBSURL'] = dbsUrl



spec = maker.makeWorkflow()
spec.save("%s-Workflow.xml" % maker.workflowName)


print "Created: %s-Workflow.xml" % maker.workflowName
print "From: %s " % cfgFile
print "Input Dataset: %s " % dataset
print "  ==> Will be split by %s in increments of %s" % (splitType, splitSize)
print "Output Datasets:"

[ sys.stdout.write(
     "/%s/%s/%s\n" % (
       x['PrimaryDataset'],
       x['ProcessedDataset'],
       x['DataTier'])) for x in spec.outputDatasets()]


