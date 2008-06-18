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
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions

valid = ['cfg=', 'py-cfg=', 'version=', 'category=', "label=",
         'override-channel=', 'group=', 'request-id=',
         'dataset=',
         'split-type=', 'split-size=',
         'only-blocks=', 'only-sites=',
         'only-closed-blocks',
         'dbs-url=', 
         'pileup-dataset=', 'pileup-files-per-job=','workflow_tag=',
         'tar-up-lib','tar-up-src','split-into-primary',
         'acquisition_era=','conditions=','processing_version='
         
         ]


usage = "Usage: createProcessingWorkflow.py --cfg=<cfgFile>\n"
usage += "                                  --version=<CMSSW version>\n"
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
usage += "                                  --override-channel=<Phys Channel/Primary Dataset>\n"
usage += "                                  --acquisition_era=<Acquisition Era>\n"
usage += "                                  --conditions=<Conditions>\n"
usage += "                                  --processing_version=<Processing version>\n"
usage += "                                  --workflow_tag=<Workflow tag>\n"
usage += "                                --split-into-primary\n"
usage += "                                --tar-up-lib\n"
usage += "                                --tar-up-src\n"





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

  --override-channel allows you to specify a different primary dataset
    for the output. Default/Normal use is to use the same channel/primary
    as the input dataset.

  --only-closed-blocks  Switch that will mean that open blocks are ignored
    by the dataset injector.

  --split-into-primary  Take datasets flagged in cfg file as primary datasets

  --dbs-url=DBS Url, The URL of the DBS Service containing the input dataset

   --tar-up-lib Switch turns up tarring up and bringing along the lib
     dir.  Not for any particular reason, just to uh... be safe.

   --tar-up-src Similar to --tar-up-lib, but for src.  It makes sense
     to have this be seperate

    
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
cfgType = "cfg"
selectionEfficiency = None

dataset = None
splitType = None
splitSize = None

splitIntoPrimary = False

onlyBlocks = None
onlySites = None
dbsUrl = None
onlyClosedBlocks = False

pileupDataset = None
pileupFilesPerJob = 1

tarupLib = False
tarupSrc = False

acquisitionEra="Test"
conditions="Bad"
processingVersion=666
workflow_tag=None




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

    if opt == "--override-channel":
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

    if opt == "--acquisition_era":
        acquisitionEra = arg
    if opt == "--conditions":
        conditions = arg
    if opt == "--processing_version":
        processingVersion = arg

    if opt == "--workflow_tag":
        workflow_tag = arg


    if opt == '--split-into-primary':
        splitIntoPrimary = True

    if opt == '--tar-up-lib':
        tarupLib = True
    if opt == '--tar-up-src':
        tarupSrc = True  

if workflow_tag in (None,""):
   requestId="%s_%s" % (conditions,processingVersion)
else:
   requestId="%s_%s_%s" % (conditions,workflow_tag,processingVersion)

label=acquisitionEra


        
if cfgFile == None:
    msg = "--cfg or --py-cfg option not provided: This is required"
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

channel0 = DatasetConventions.parseDatasetPath(dataset)['Primary']

if channel == None:
    #  //
    # // Assume same as input
    #//
    channel = DatasetConventions.parseDatasetPath(dataset)['Primary']
    

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
    modRef = imp.find_module( os.path.basename(cfgFile).replace(".py", ""),  os.path.dirname(cfgFile))
    cmsCfg = modRef.process
                                                                                                      
cfgWrapper = CMSSWConfig()
cfgWrapper.originalCfg = file(cfgFile).read()
cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
cfgInt.validateForProduction()




#  //
# //  deal with user sandbox -- make tarball & stick location in workflow
# ||  Code developed more or less organically as we learned what needed to be packed along
#//

# first we make sure version is the same as where we are, otherwise bail
if tarupLib:
  CMSSWversionFromHere=((os.getcwd()).split("/"))[-2]
  if CMSSWversionFromHere!=version:
      msg="You said to tar up the lib dir,\n"
      msg+="but specified a different CMSSW version than we're sitting in..."
      raise RuntimeError, msg


  CMSSWLibPath=os.path.join(version,'lib')
  RelCMSSWLibPath=os.path.join('../../',CMSSWLibPath)
  LibSubDir=os.listdir(RelCMSSWLibPath)
  if len(LibSubDir)>1:
    msg="Seems there is more than one architecture in lib dir, and --tar-up-libs was set."
    raise RuntimeError, msg
  LibOSPath=LibSubDir[0]
  CMSSWLibSubPath=os.path.join(CMSSWLibPath,LibOSPath)
  RelCMSSWLibSubPath=os.path.join('../../',CMSSWLibSubPath)
  StuffinLib=os.listdir(RelCMSSWLibSubPath)
  if len(StuffinLib)>0:
    print "%i things in %s -- tarring the following:"% (len(StuffinLib),CMSSWLibSubPath)
  else:
    print "Nothing in CMSSW lib dir\n Nevermind..."  

#   need to do the same for the module dir...

  CMSSWModulePath=os.path.join(version,'module') 
  RelCMSSWModulePath=os.path.join('../../',CMSSWModulePath)
  ModuleSubDir=os.listdir(RelCMSSWModulePath)
  if len(ModuleSubDir)>1:
    msg="Seems there is more than one architecture in module dir, and --tar-up-libs was set."
    raise RuntimeError, msg
  ModuleOSPath=ModuleSubDir[0]
  CMSSWModuleSubPath=os.path.join(CMSSWModulePath,ModuleOSPath)
  RelCMSSWModuleSubPath=os.path.join('../../',CMSSWModuleSubPath)
  StuffinModule=os.listdir(RelCMSSWModuleSubPath)
  if len(StuffinModule)>0:
    print "%i things in %s -- tarring the following:"% (len(StuffinModule),CMSSWModuleSubPath)
  else:
    print "Nothing in CMSSW module dir\n Nevermind..."
    
#   need to do the same for the share dir...



  CMSSWSharePath=""
  CMSSWSharePath=os.path.join(version,'share')
  RelCMSSWSharePath=os.path.join('../../',CMSSWSharePath)
  if os.path.exists(RelCMSSWSharePath):
    StuffinShare=os.listdir(RelCMSSWSharePath)
    if len(StuffinShare)>0:
      print "%i things in %s -- tarring the following:"% (len(StuffinShare),CMSSWSharePath)
    else:
      print "Nothing in CMSSW share dir\n Nevermind..."

# then python path

  CMSSWPythonPath=""
  CMSSWPythonPath=os.path.join(version,'python')
  RelCMSSWPythonPath=os.path.join('../../',CMSSWPythonPath)
  if os.path.exists(RelCMSSWPythonPath):
    StuffinPython=os.listdir(RelCMSSWPythonPath)
    if len(StuffinPython)>0:
      print "%i things in %s -- tarring the following:"% (len(StuffinPython),CMSSWPythonPath)
    else:
      print "Nothing in CMSSW python dir\n Nevermind..."


#   need to do the same for the src/data dir if requested...
  CMSSWSrcPath=""
  if tarupSrc:
    CMSSWSrcPath=os.path.join(version,'src') 
    RelCMSSWSrcPath=os.path.join('../../',CMSSWSrcPath)
    StuffinSrc=os.listdir(RelCMSSWSrcPath)
    if len(StuffinSrc)>0:
      print "%i things in %s -- tarring the following:"% (len(StuffinSrc),CMSSWSrcPath)
    else:
      print "Nothing in CMSSW lib dir\n Nevermind..."

  



#  //
# // Instantiate a WorkflowMaker
#//

maker = WorkflowMaker(requestId, channel, label )

maker.setCMSSWVersion(version)
maker.setPhysicsGroup(physicsGroup)
maker.setConfiguration(cfgWrapper, Type = "instance")
maker.setPSetHash(WorkflowTools.createPSetHash(cfgFile))
maker.changeCategory(category)
maker.setAcquisitionEra(acquisitionEra)



#  //
# //  Actually make tarball & insert into workflow -- needed to do this after maker
# ||  is instantiated so that we have the workflow name to play with
#//

if tarupLib:
    
  tarball="%s.sandbox.tgz" % maker.workflowName
  systemcommand="tar -cvzf %s -C ../.. --wildcards --exclude=\"*.tgz\" --exclude \"*.root\" %s %s %s %s %s" % (tarball,CMSSWLibSubPath,CMSSWModuleSubPath,CMSSWSharePath,CMSSWSrcPath,CMSSWPythonPath)

  print "system command: %s" % systemcommand
  os.system(systemcommand)
#  systemcommand="mv ../%s ." % tarball
#  os.system(systemcommand)
  FullPathToTarfile=os.path.join(os.getcwd(),tarball)
  maker.setUserSandbox(FullPathToTarfile)
  print "Tarball %s inserted into workflow" % tarball

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

maker.workflow.parameters['Conditions'] = conditions
maker.workflow.parameters['ProcessingVersion'] = processingVersion

spec = maker.makeWorkflow()
appendedname="%s-%s" % (maker.workflowName,channel0)
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


