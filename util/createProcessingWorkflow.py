#!/usr/bin/env python
"""
_createProcessingWorkflow_

Create a workflow that processes an input dataset with a cfg file

"""
__version__ = "$Revision: 1.24 $"
__revision__ = "$Id: createProcessingWorkflow.py,v 1.24 2009/09/14 14:52:33 direyes Exp $"

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
         'pileup-dataset=', 'pileup-files-per-job=',
         'activity=', 'stageout-intermediates=', 'chained-input=',
         'acquisition_era=', 'conditions=', 'processing_version=',
         'processing_string=', 'workflow_tag=', 'split-into-primary',
         'tar-up-lib','tar-up-src', 'dbs-status=', 'datamixer-pu-ds='
         ]


usage  = "Usage: createProcessingWorkflow.py --cfg=<cfgFile>\n"
usage += "                                  --py-cfg=<python cfgFile>\n"
usage += "                                  --version=<CMSSW version>\n"
usage += "                                  --group=<Physics Group>\n"
#usage += "                                  --request-id=<Request ID>\n"
#usage += "                                  --label=<Production Label>\n"
usage += "                                  --category=<Production category>\n"
usage += "                                  --dataset=<Dataset to process>\n"
usage += "                                  --split-type=<event|file>\n"
usage += "                                  --split-size=<Integer split size>\n"
usage += "                                  --only-blocks=<List of fileblocks>\n"
usage += "                                  --only-sites=<List of sites>\n"
usage += "                                  --only-closed-blocks=<True|False>\n"
usage += "                                  --dbs-url=<DBSUrl>\n"
usage += "                                  --pileup-dataset=<Input Pile Up Dataset>\n"
usage += "                                  --pileup-files-per-job=<Integer pile up files per job>\n"
usage += "                                  --datamixer-pu-ds=<Input Pile Up Dataset for DataMixing /PrimDS/ProcDS/Tier>\n"
usage += "                                  --override-channel=<Phys Channel/Primary Dataset>\n"
usage += "                                  --selection-efficiency=<Selection efficiency>\n"
usage += "                                  --activity=<activity>\n"
usage += "                                  --stageout-intermediates=<true|false>\n"
usage += "                                  --chained-input=comma,separated,list,of,output,module,names\n"
usage += "                                  --acquisition_era=<Acquisition Era>\n"
usage += "                                  --conditions=<Conditions>\n"
usage += "                                  --processing_version=<Processing version>\n"
usage += "                                  --processing_string=<Processing string>\n"
usage += "                                  --dbs-status=<VALID|PRODUCTION> Default: PRODUCTION\n"
usage += "                                  --workflow_tag=<Workflow tag>\n"
usage += "                                  --split-into-primary\n"
usage += "                                  --tar-up-lib\n"
usage += "                                  --tar-up-src\n"
usage += "\n  Options:\n"

options = \
"""
  --acquisition_era sets the aquisition era and the Primary Dataset name

  --activity=<activity>, The activity represented but this workflow
    i.e. Reprocessing, Skimming etc. (Default: PrivateReprocessing)

  --category is the processing category, eg PreProd, SVSuite, Skim etc. It
    defaults to 'mc' if not provided

  --cfg is the path to the cfg file to be used for the skimming cmsRun task

  --chained-input=comma,separated,list,of,output,module,names Optional param
    that specifies the output modules to chain to the next input module. Defaults
    to all modules in a step, leave blank for all. If given should be specified
    for each step

  --conditions Deprecated. 

  --datamixer-pu-ds input dataset for the data mixer module.

  --dataset is the input dataset to be processed

  --dbs-status is the status flag the output datasets will have in DBS. If
    VALID, the datasets will be accesible by the physicists, If PRODUCTION, 
    the datasets will be hidden.

  --dbs-url The URL of the DBS Service containing the input dataset

  --group is the Physics group

  --only-blocks allows you to specify a comma seperated list of blocks that
    will be imported if you dont want the entire dataset.
    Eg: --only-blocks=blockname1,blockname2,blockname3 will process only files
    belonging to the named blocks.

  --only-closed-blocks  Switch that will mean that open blocks are ignored
    by the dataset injector. Defaults: False

  --only-sites allows you to restrict which fileblocks are used based on
    the site name, which will be the SE Name for that site
    Eg: --only-sites=site1,site2 will process only files that are available
    at the specified list of sites.

  --override-channel allows you to specify a different primary dataset
    for the output. Default/Normal use is to use the same channel/primary
    as the input dataset.

  --pileup-dataset is the input pileup dataset

  --pileup-files-per-job is the pileup files per job

  --processing_string sets the processing string

  --processing_version sets the processing version

  --py-cfg is path to the python cfg file to be used for the skimming cmsRun
    task

  --selection-efficiency sets the efficiency

  --split-into-primary  Take datasets flagged in cfg file as primary datasets

  --split-size is an integer that defines the size of jobs. If --split-type is
    set to files, then it is the number of files per job. If --split-type is
    set to events, then it is the number of events per job

  --split-type should be either file or event. file means split the jobs based
    on file boundaries. This means jobs with have N files per job, where N is
    the value of --split-size. If this is events, then each job will contain N
    events (regardless of file boundaries) where N is the value of --split-size

  --stageout-intermediates=<true|false>, Stageout intermediate files in
    chained processing

  --version is the version of the CMSSW to be used, you should also have done
    a scram runtime setup for this version

  --workflow_tag sets the workflow tag to distinguish e.g. RAW and RECO workflows
    for a given channel

"""

#  //
# // Bonus track options (Please do not use them if you're don't know waht you're doing)
# || These options don't work for chain processing!
#//
# --tar-up-lib Switch turns up tarring up and bringing along the lib
#   dir.  Not for any particular reason, just to uh... be safe.
#  --tar-up-src Similar to --tar-up-lib, but for src.  It makes sense
#   to have this be seperate


usage += options


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

cfgFiles = []
versions = []
stageoutOutputs = []
chainedInputs = []
requestId = "%s-%s" % (os.environ['USER'], int(time.time()))
physicsGroup = "Individual"
label = "Test"
category = "mc"
channel = None
cfgTypes = []
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

dataMixDS = None

activity = "PrivateReprocessing"

tarupLib = False
tarupSrc = False

acquisitionEra="Test"
conditions="Bad"
processingVersion=666
workflow_tag=None
processingString = None
dbsStatus = 'PRODUCTION'


for opt, arg in opts:
    if opt == "--cfg":
        cfgFiles.append(arg)
        cfgTypes.append("cfg")
    if opt == "--py-cfg":
        cfgFiles.append(arg)
        cfgTypes.append("python")
    if opt == "--version":
        versions.append(arg)
    if opt == "--stageout-intermediates":
        if arg.lower() in ("true", "yes"):
            stageoutOutputs.append(True)
        else:
            stageoutOutputs.append(False)
    if opt == '--chained-input':
        chainedInputs.append([x.strip() for x in arg.split(',') if x!=''])
    if opt == "--category":
        category = arg

    if opt == "--override-channel":
        channel = arg
#    if opt == "--label":
#        label = arg
    if opt == "--group":
        physicsGroup = arg
#    if opt == "--request-id":
#        requestId = arg

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
    if opt == '--datamixer-pu-ds':
        dataMixDS = arg
    if opt == '--pileup-dataset':
        pileupDataset = arg
    if opt == '--pileup-files-per-job':
        pileupFilesPerJob = arg
    if opt == '--activity':
        activity = arg
    if opt == "--acquisition_era":
        acquisitionEra = arg
    if opt == "--conditions":
        conditions = arg
    if opt == "--processing_version":
        processingVersion = arg
    if opt == "--processing_string":
        processingString = arg
    if opt == "--workflow_tag":
        workflow_tag = arg
    if opt == '--split-into-primary':
        splitIntoPrimary = True
    if opt == '--tar-up-lib':
        tarupLib = True
    if opt == '--tar-up-src':
        tarupSrc = True
    if opt == '--dbs-status':
        if arg in ("VALID", "PRODUCTION"):
            dbsStatus = arg
 

requestId = processingVersion
if workflow_tag:
    requestId = "%s_%s" % (workflow_tag, requestId)
if processingString:
    requestId = "%s_%s" % (processingString, requestId)

label=acquisitionEra


        
if not len(cfgFiles):
    msg = "--cfg or --py-cfg option not provided: This is required"
    raise RuntimeError, msg
elif len(cfgFiles) > 1:
    print "%s cfgs listed - chaining them" % len(cfgFiles)

if not len(versions):
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg
for item in versions:
    if item  in ("", None):
        msg = "Version option appears to be empty."
        raise RuntimeError, msg
if len(versions) != len(cfgFiles):
    msg = "Need same number of --cfg and --version arguments"
    raise RuntimeError, msg
if len(stageoutOutputs) != len(cfgFiles) - 1:
    msg = "Need one less --stageout-intermediates than --cfg arguments"
    raise RuntimeError, msg
if len(chainedInputs) and len(chainedInputs) != len(cfgFiles) - 1:
    msg = "Need one less chained-input than --cfg arguments"
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

#channel0 = DatasetConventions.parseDatasetPath(dataset)['Primary']

if channel == None:
    #  //
    # // Assume same as input
    #//
    channel = DatasetConventions.parseDatasetPath(dataset)['Primary']
    

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
maker.setPhysicsGroup(physicsGroup)
#maker.changeCategory(category)
#maker.setAcquisitionEra(acquisitionEra)



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


# loop over cfg's provided and add to workflow
# first cmsRun node created implicitly by WorkflowMaker
nodeNumber = 0
for cfgFile in cfgFiles:

    if cfgTypes[nodeNumber] == "cfg":
        from FWCore.ParameterSet.Config import include
        cmsCfg = include(cfgFile)
    else:
        import imp
        modRef = imp.load_source( os.path.basename(cfgFile).replace(".py", ""),  cfgFile)
        cmsCfg = modRef.process

    cfgWrapper = CMSSWConfig()
    #cfgWrapper.originalCfg = file(cfgFile).read()
    cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
    cfgInt.validateForProduction()

    if nodeNumber:
        try:
            inputModules = chainedInputs[nodeNumber-1]
        except IndexError:
            inputModules = []
        maker.chainCmsRunNode(stageoutOutputs[nodeNumber-1], *inputModules)

    maker.setCMSSWVersion(versions[nodeNumber])
    maker.setConfiguration(cfgWrapper, Type = "instance")
    maker.setOriginalCfg(file(cfgFile).read())
    maker.setPSetHash(WorkflowTools.createPSetHash(cfgFile))
    
    nodeNumber = nodeNumber + 1

maker.changeCategory(category)
maker.setNamingConventionParameters(acquisitionEra, processingString, processingVersion)
 
#  //
# // Pileup sample?
#//
if pileupDataset != None:
    maker.addPileupDataset(pileupDataset, pileupFilesPerJob)

#  //
# // DataMix pileup sample?
#//
if dataMixDS:
     maker.addPileupDataset(dataMixDS, 1, 'DataMixingModule')

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

maker.setOutputDatasetDbsStatus(dbsStatus)
spec = maker.makeWorkflow()
spec.setActivity(activity)
#appendedname="%s-%s" % (maker.workflowName,channel0)
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


