#!/usr/bin/env python
"""
_releaseValidation_

Tool to retrieve config files from the Web and generate Workflows
for them, and then inject them into the ProdAgent as Jobs.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.22 $"
__revision__ = "$Id: releaseValidation.py,v 1.22 2007/04/19 13:53:29 afanfani Exp $"


import os
import sys
import getopt
import popen2
import time
import re

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.MCPayloads.RelValSpec import getRelValSpecForVersion, listAllVersions
from MessageService.MessageService import MessageService
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions


valid = ['url=', 'version=', 'relvalversion=', 'events=', 'run=',
         'subpackage=', 'alltests', "site-pref=", "sites=", "no-recreate",
         'testretrieval', 'testpython', "cvs-tag=", "fake-hash",
         'pileup-dataset=', 'pileup-files-per-job=',
         'data-dir=', 'create-workflows-only',
         'category=',
         'local-checkout=',
         'use-input-dataset'

         ]

usage = "Usage: releaseValidation.py --url=<Spec XML URL>\n"
usage += "                           --version=<CMSSW version to be used>\n"
usage += "                           --relvalversion=<comma separated list  of versions to be used in spec file>\n"
usage += "                           --events=<events per job>\n"
usage += "                           --run=<first run number>\n"
usage += "                           --cvs-tag=<CVS Tag of cfg files if not same as version>\n"
usage += "                           --fake-hash\n"
usage += "                           --no-recreate\n"
usage += "                           --site-pref=<Site Name>\n"
usage += "                           --sites=<comma seperated list of sites to run relval at>\n"
usage += "                           --alltests\n"
usage += "                           --cvs-tag=<CVS Tag of cfg files if not same as version>\n"
usage += "                           --local-checkout=<location of the cfg file: yourPath/CMSSW_1_X_X/src>\n"
usage += "                           --use-input-dataset= <datasetPath of the input dataset>\n"


usage += "    Test mode Options:\n"
usage += "                            --testretrieval\n"
usage += "                            --testpython\n"
usage += "                            "
usage += "You must have a scram runtime environment setup to use this tool\n"
usage += "since it will invoke EdmConfig tools\n\n"
usage += "Events per job defaults to 100 for faster finishing jobs\n"
usage += "First run number defaults to 5000\n"
usage += " --fake-hash will generate a fake PSet Hash for each dataset\n"
usage += " --site-pref sets the site name to run jobs on\n"
usage += " --sites allows you to provide a list of sites to run all the samples on\n"
usage += " --no-recreate assumes that the workflows are already in the ProdAgent and creates more jobs from them\n"
usage += " --relvalversion is a list of Validate tag versions to run in the spec file\n"
usage += " --alltests means use all tests found in the spec file\n"
usage += "If --testretrieval is provided only parsing the XML spec and \n"
usage += "retrieving the cfg files is performed\n"
usage += "If --testpython is provided the cfg files will be retrieved \n"
usage += "and converted to python, but no jobs will be submitted\n" 


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

xmlFile = None
version = None
cvsTag = None
relvalVersions = []
allTests = False
category = "RelVal"
timestamp = int(time.time())
requestId = "%s"%timestamp
label="RelVal"
physicsGroup = label
channel = None
eventsPerJob = 100
run = 5000
subpackage = "ReleaseValidation"
testRetrievalMode = False
testPythonMode = False
workflowsOnly = False
fakeHash = False
siteList = []
sitePref = None
noRecreate = False
dataDir = "data"
localCheckout = None
useInputDataset = False

inputDataset = None
splitType = None
splitSize = None

pileupDS = None
pileupFilesPerJob = 1

#
# hardcode DBS URL
#
inputDBSDLS = {
    "DBSURL": "https://cmsdbsprod.cern.ch:8443/cms_dbs_int_global_writer/servlet/DBSServlet",
    }
dbsUrl = inputDBSDLS['DBSURL']

# for RelVal assume the PU is at the sites to run on:  
pileupSkipLocation = True 

def reduceVersion(versString):
    """
    _reduceVersion_

    take a string like CMSSW_X_Y_Z_preA and reduce it to
    XYZpreA

    We add this into the job name for each release validation run to enable
    easily identifying which jobs are from which release
    
    """
    result = versString.replace("CMSSW_", "")
    result = result.replace("_", "")
    return result

for opt, arg in opts:
    if opt == "--url":
        xmlFile = arg
    if opt == "--version":
        version = arg
    if opt == "--cvs-tag":
        cvsTag = arg
    if opt == "--relvalversion":
        relvalVersions = [ i for i in arg.split(',') if i != "" ]
    if opt == "--sites":
        siteList = [ i for i in arg.split(',') if i != "" ]
        
    if opt == "--events":
        eventsPerJob = int(arg)
    if opt == "--run":
        run = int(arg)
    if opt == "--testretrieval":
        testRetrievalMode = True
    if opt == "--testpython":
        testPythonMode = True
    if opt == "--fake-hash":
        fakeHash = True
    if opt == "--alltests":
        allTests = True
    if opt == "--no-recreate":
        noRecreate = True
    if opt == "--subpackage":
        subpackage = arg
    if opt == "--site-pref":
        sitePref = arg
    if opt == '--pileup-dataset':
        pileupDS = arg
    if opt == '--pileup-files-per-job':
        pileupFilesPerJob = arg
    if opt == '--data-dir':
        dataDir = arg
    if opt == '--create-workflows-only':
        workflowsOnly = True
    if opt == '--category':
        category = arg
    if opt == '--local-checkout':
        localCheckout = arg
    if opt == '--use-input-dataset':
        useInputDataset = True

if xmlFile == None:
    msg = "--url option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg

if localCheckout != None:
    if not os.path.exists(localCheckout):
        msg = "Local checkout directory not found:\n"
        msg += "%s\n" % localCheckout
        raise RuntimeError, msg

if not allTests:
    if relvalVersions == []:
        msg = "--relvalVersion not provided, You need to provide a list of"
        msg += "comma seperated test names or set the --alltests flag"
        raise RuntimeError, msg
else:
    relvalVersions = listAllVersions(xmlFile)

if cvsTag == None:
    cvsTag = version

try:
    relValSpec = getRelValSpecForVersion(xmlFile, *relvalVersions)
except StandardError, ex:
    msg = "Error retrieving Release Validation Spec File:\n"
    msg += "%s\n" % xmlFile
    msg += str(ex)
    print msg
    sys.exit(1)

if relValSpec == None:
    msg =  "Unable to extract release validation spec from file:\n"
    msg += "%s\n" % xmlFile
    msg += "Release Validation Versions requested: %s\n " % relvalVersions
    msg += "You may need to provide --relvalversion as a command line option"
    print msg
    sys.exit(1)


summaryJobs = 0
summaryEvents = 0

selectionEfficiency = None

for relTest in relValSpec:
    prodName = relTest['Name']
    prodName = prodName.replace("RelVal", "RelVal%s" % reduceVersion(version))
    prodName = prodName.replace("PhysVal", "PhysVal%s" % reduceVersion(version))
    prodName = prodName.replace("HLTVal", "HLTVal%s" % reduceVersion(version))
    prodName = prodName.replace("SVSuite", "SVSuite%s" % reduceVersion(version))
    cfgFile = os.path.join(os.getcwd(), "%s.cfg" % prodName)
    ## set channel   
    channel = prodName
    ## set label
    if prodName.count("RelVal"): label="RelVal"
    if prodName.count("PhysVal"): label="PhysVal"
    if prodName.count("HLTVal"): label="HLTVal"
    if prodName.count("SVSuite"): label="SVSuite"
    ## FIXME: set physicgroup to label as well when those group will be defined in DBS2

    numberOfJobs = int( int(relTest['Events']) / eventsPerJob) + 1
    eventCount = eventsPerJob

    if relTest['FractionSelected'] != None:
        efficiency = float(relTest['FractionSelected'])
        eventCount = int(eventCount / efficiency) + 1
        print " ==>Selection Efficiency Found: %s " % efficiency
        ## set selectionEfficiency in workflow : recomputing events done in PA interanlly
        selectionEfficiency = efficiency        
        #print " ==>Events Per Job Adjusted To: %s" % eventCount

    # Input dataset related variables taken from XML
    if useInputDataset:
        if relTest.get("InputDataset", None) == None:
            msg = "You must have InputDataset"
            print msg
            sys.exit(1)
        else:                                                                                    
            inputDataset = relTest['InputDataset']
            ## set the channel to the primary dataset of inputDataset
            channel = DatasetConventions.parseDatasetPath(inputDataset)['Primary']
            splitType="event"
            splitSize=eventCount


    if localCheckout == None:
        urlBase = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/*checkout*/CMSSW/Configuration/%s/%s/" % (subpackage, dataDir)
    else:
        urlBase = "%s/Configuration/%s/%s/" % (localCheckout, subpackage, dataDir)
  
    cfgUrl = "%s%s" % (urlBase, relTest['CfgUrl'])

## replace restriction only_with_tag with rev :
    if localCheckout == None:
        cfgUrl += "?rev=%s" % cvsTag   
        print " cfgUrl %s"%cfgUrl

    if not noRecreate:
        if localCheckout == None:
            wgetCommand = "wget %s -O %s" % (cfgUrl, cfgFile)
            pop = popen2.Popen4(wgetCommand)
            while pop.poll() == -1:
                exitStatus = pop.poll()
            exitStatus = pop.poll()
            if exitStatus:
                msg = "Error creating retrieving cfg file: %s\n" % cfgUrl
                msg += pop.fromchild.read()
                raise RuntimeError, msg
        else:
            print "Local checkout used:"
            print " .cfg file source: %s" % cfgUrl
            os.system("/bin/cp %s %s" % (cfgUrl, cfgFile))
    
    if testRetrievalMode:
        print "Test Retrieval Mode:"
        print "Retrieval Completed for %s" % prodName
        print "Cfg File is %s " % cfgFile
        continue
    pycfgFile = "%s.pycfg" % prodName
    

    if not os.path.exists(cfgFile):
        msg = "Cfg File Not Found: %s" % cfgFile
        raise RuntimeError, msg

    #  //
    # // Make sure PSet ends up with unique hash
    #//
    RealPSetHash = None
    if not noRecreate:
        cfgFileContent = file(cfgFile).read()
        replacer=re.compile("\}[\s]*$")
        psetHackString = "\n  PSet psetHack = { string relval = \"%s\"  }\n}\n" % (
            prodName,
            )
        cfgFileContent = replacer.sub(psetHackString, cfgFileContent)
        handle = open(cfgFile, 'w')
        handle.write(cfgFileContent)
        handle.close()    


        #  //
        # // Cleanup existing files
        #//
    
        if os.path.exists(pycfgFile):
            os.remove(pycfgFile)
    
        #  //
        # // Generate python cfg file
        #//
        WorkflowTools.createPythonConfig(cfgFile)
    
        #  //
        # // Generate PSet Hash
        #//
        RealPSetHash = WorkflowTools.createPSetHash(cfgFile)

    #  //
    # // Existence checks for created files
    #//
    for item in (cfgFile, pycfgFile):
        if not os.path.exists(item):
            msg = "File Not Found: %s" % item
            raise RuntimeError, msg

        

    if testPythonMode:
        print "Test Python Mode:"
        print "EdmConfigToPython and EdmConfigHash successful for %s" % prodName
        print "Python Config File: %s" % pycfgFile
        print "Hash: %s" % RealPSetHash
        continue
    
    #  // 
    # // Create a new WorkflowSpec and set its name
    #//

    #  //
    # // Instantiate a WorkflowMaker
    #//
    maker = WorkflowMaker(requestId, channel, label )

    maker.setCMSSWVersion(version)
    maker.setPhysicsGroup(physicsGroup)
    maker.setConfiguration(cfgFile, Format = "cfg", Type = "file")
    maker.setPSetHash(RealPSetHash)
    maker.changeCategory(category)

    if selectionEfficiency != None:
      maker.addSelectionEfficiency(selectionEfficiency)

    #  //
    # // Pileup??
    #//
    if pileupDS != None:
      maker.addPileupDataset( pileupDS, pileupFilesPerJob)

    if dbsUrl != None:
      maker.workflow.parameters['DBSURL'] = dbsUrl

    #  //
    # // Input Dataset
    #//
    if useInputDataset:
       maker.addInputDataset(inputDataset)
       maker.inputDataset['SplitType'] = splitType
       maker.inputDataset['SplitSize'] = splitSize

    spec = maker.makeWorkflow()
    workflowBase = "%s-Workflow.xml" % maker.workflowName
    workflow = os.path.join(os.getcwd(), workflowBase)

    # use MessageService
    ms = MessageService()
    # register message service instance as "Test"
    ms.registerAs("Test")
    

    if not noRecreate:
        spec.save("%s-Workflow.xml" % maker.workflowName)

        print "Created: %s-Workflow.xml" % maker.workflowName
        print "Created: %s " % pycfgFile
        print "From Tag: %s Of %s " % (cvsTag, cfgFile )
        if useInputDataset:
          print "Input Dataset: %s " % inputDataset
          print "  ==> Will be split by %s in increments of %s" % (splitType, splitSize)
        print "Output Datasets:"
        [ sys.stdout.write(
          "/%s/%s/%s\n" % (
          x['PrimaryDataset'],
          x['ProcessedDataset'],
          x['DataTier'])) for x in spec.outputDatasets()]

        if not workflowsOnly:
            # Set Workflow and NewDataset
            if useInputDataset:
                ms.publish("DatasetInjector:SetWorkflow", workflow)
                ms.publish("DatasetInjector:SelectWorkflow", workflowBase)
                ms.commit()
                time.sleep(1)
                ms.publish("NewDataset", workflow)
                ms.commit()
            else:
                ms.publish("RequestInjector:SetWorkflow", workflow)
                ms.publish("RequestInjector:SelectWorkflow", workflowBase)
                ms.publish("RequestInjector:SetInitialRun", str(run))
                ms.commit()
                time.sleep(1)
                ms.publish("RequestInjector:NewDataset",'')
                ms.commit()
    

    else:
        print "Using: %s-Workflow.xml" % maker.workflowName

        
    if workflowsOnly:
        continue 
    if not useInputDataset:
     if sitePref != None:
        ms.publish("RequestInjector:SetSitePref", sitePref)
        ms.commit()
     # Set first run and number of events per job
     ms.publish("RequestInjector:SelectWorkflow", workflowBase)
     ms.publish("RequestInjector:SetEventsPerJob", str(eventCount))
     ms.commit()
     time.sleep(1)


    # Loop over sites
    sitesToLoop = [None]
    if len(siteList) > 0:
        sitesToLoop = siteList
    for siteName in sitesToLoop:
        # Loop over jobs
        if not useInputDataset:
         if siteName != None:
            ms.publish("RequestInjector:SetSitePref", siteName)
            ms.commit()
         for i in range(0, numberOfJobs):
            summaryJobs += 1
            summaryEvents += eventsPerJob
            
            time.sleep(1)
            ms.publish("RequestInjector:ResourcesAvailable","none")
            ms.commit()
        else:
            #AF: ms.publish("DatasetInjector:ReleaseJobs","1000")
            ms.publish("DatasetInjector:ReleaseJobs","%s"%numberOfJobs)
            ms.commit()

        print "Created Jobs..."
        print "%s jobs being created for workflow: %s" % (numberOfJobs, workflowBase)
        print "Target Site: %s" % siteName

if not useInputDataset:
 print "Total Jobs Created: %s" % summaryJobs
 print "Total Events for all jobs: %s" % summaryEvents
