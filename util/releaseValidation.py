#!/usr/bin/env python
"""
_releaseValidation_

Tool to retrieve config files from the Web and generate Workflows
for them, and then inject them into the ProdAgent as Jobs.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.10 $"
__revision__ = "$Id: releaseValidation.py,v 1.10 2006/10/27 17:04:39 evansde Exp $"


import os
import sys
import getopt
import popen2
import time
import re

from MCPayloads.WorkflowSpec import WorkflowSpec
from MCPayloads.LFNAlgorithm import unmergedLFNBase, mergedLFNBase
from MCPayloads.RelValSpec import getRelValSpecForVersion, listAllVersions
from CMSConfigTools.CfgInterface import CfgInterface
from MessageService.MessageService import MessageService
from MCPayloads.DatasetExpander import splitMultiTier
import MCPayloads.WorkflowTools as WorkflowTools
import MCPayloads.UUID as MCPayloadsUUID

valid = ['url=', 'version=', 'relvalversion=', 'events=', 'run=',
         'subpackage=', 'alltests', "site-pref=",
         'testretrieval', 'testpython', "cvs-tag=", "fake-hash"]

usage = "Usage: releaseValdidation.py --url=<Spec XML URL>\n"
usage += "                            --version=<CMSSW version to be used>\n"
usage += "                            --relvalversion=<version to be used in spec file>\n"
usage += "                            --events=<events per job>\n"
usage += "                            --run=<first run number>\n"
usage += "                            --cvs-tag=<CVS Tag of cfg files if not same as version>\n"
usage += "     Options:\n"
usage += "                            --testretrieval\n"
usage += "                            --testpython\n"
usage += "                            "
usage += "You must have a scram runtime environment setup to use this tool\n"
usage += "since it will invoke EdmConfig tools\n\n"
usage += "Events per job defaults to 100 for faster finishing jobs\n"
usage += "First run number defaults to 5000"
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
eventsPerJob = 100
run = 5000
subpackage = "ReleaseValidation"
testRetrievalMode = False
testPythonMode = False
fakeHash = False
sitePref = None

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
        relvalVersions = [ i for i in args.split(',') if i != "" ]
        
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
    if opt == "--subpackage":
        subpackage = arg
    if opt == "--site-pref":
        sitePref = arg

if xmlFile == None:
    msg = "--url option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
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


for relTest in relValSpec:
    prodName = relTest['Name']
    prodName = prodName.replace("RelVal", "RelVal%s" % reduceVersion(version) )
    prodName = prodName.replace("PhysVal", "PhysVal%s" % reduceVersion(version) )
    cfgFile = os.path.join(os.getcwd(), "%s.cfg" % prodName)

    numberOfJobs = int( int(relTest['Events']) / eventsPerJob) + 1
    eventCount = eventsPerJob

    if relTest['FractionSelected'] != None:
        efficiency = float(relTest['FractionSelected'])
        eventCount = int(eventCount / efficiency) + 1
        print " ==>Selection Efficiency Found: %s " % efficiency
        print " ==>Events Per Job Adjusted To: %s" % eventCount
        
    urlBase = "http://cmsdoc.cern.ch/swdev/viewcvs/viewcvs.cgi/*checkout*/CMSSW/Configuration/%s/data/" % subpackage

    cfgUrl = "%s%s" % (urlBase, relTest['CfgUrl'])
    cfgUrl += "?only_with_tag=%s" % cvsTag
    
    

    wgetCommand = "wget %s -O %s" % (cfgUrl, cfgFile)
    
    pop = popen2.Popen4(wgetCommand)
    while pop.poll() == -1:
        exitStatus = pop.poll()
    exitStatus = pop.poll()
    if exitStatus:
        msg = "Error creating retrieving cfg file: %s\n" % cfgUrl
        msg += pop.fromchild.read()
        raise RuntimeError, msg


   
    
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
    spec = WorkflowSpec()
    spec.setWorkflowName(prodName)
    spec.setRequestCategory(category)
    spec.setRequestTimestamp(timestamp)


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
    datasetList = []
    for outModName, val in cfgInt.outputModules.items():
        #  //
        # // Check for Multi Tiers.
        #//  If Output module contains - characters, we split based on it
        #  //And create a different tier for each basic tier
        # //
        #//

        tierList = splitMultiTier(outModName)    
        for dataTier in tierList:
            processedDS = "%s-%s-%s-unmerged" % (
                cmsRun.application['Version'], outModName, timestamp)
            outDS = cmsRun.addOutputDataset(prodName, 
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
    print "From Tag: %s Of %s " % (cvsTag, cfgFile )
    print "Output Datasets:"
    for item in datasetList:
        print " ==> %s" % item

    #  //
    # // Inject the workflow into the ProdAgent and trigger job creation
    #//
    
    workflowBase = "%s-Workflow.xml" % prodName
    workflow = os.path.join(os.getcwd(), workflowBase)

    print "Creating Jobs..."
    print "%s jobs being created for workflow: %s" % (numberOfJobs, workflowBase)
    if sitePref != None:
        print " Jobs created for site: %s" % sitePref

    # use MessageService
    ms = MessageService()
    # register message service instance as "Test"
    ms.registerAs("Test")

    # Debug level
    #ms.publish("RequestInjector:StartDebug","none")
    #ms.publish("JobCreator:StartDebug","none")
    #ms.publish("JobSubmitter:StartDebug","none")
    #ms.publish("TrackingComponent:StartDebug","none")
    
    #  //
    # // These should come from the ProdAgentConfig
    #//
    # Set Creator
    #ms.publish("JobCreator:SetCreator","LCGCreator")
    # Set Submitter
    #ms.publish("JobSubmitter:SetSubmitter","LCGSubmitter")


    # Set Workflow and NewDataset
    ms.publish("RequestInjector:SetWorkflow", workflow)
    ms.publish("RequestInjector:SelectWorkflow", workflowBase)
    ms.commit()
    time.sleep(1)
    ms.publish("RequestInjector:NewDataset",'')
    ms.commit()
    if sitePref != None:
        ms.publish("RequestInjector:SetSitePref", sitePref)
        ms.commit()
    # Set first run and number of events per job
    ms.publish("RequestInjector:SetInitialRun", str(run))
    ms.publish("RequestInjector:SetEventsPerJob", str(eventCount))
    ms.commit()
    time.sleep(1)

    # Loop over jobs
    for i in range(0, numberOfJobs):
        summaryJobs += 1
        summaryEvents += eventsPerJob
        
        time.sleep(1)
        ms.publish("RequestInjector:ResourcesAvailable","none")
        ms.commit()

print "Total Jobs Created: %s" % summaryJobs
print "Total Events for all jobs: %s" % summaryEvents
