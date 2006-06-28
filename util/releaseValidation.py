#!/usr/bin/env python
"""
_releaseValidation_

Tool to retrieve config files from the Web and generate Workflows
for them, and then inject them into the ProdAgent as Jobs.

This calls EdmConfigToPython and EdmConfigHash, so a scram
runtime environment must be setup to use this script.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: releaseValidation.py,v 1.1 2006/06/14 20:02:22 evansde Exp $"


import os
import sys
import getopt
import popen2
import time
import xmlrpclib

from MCPayloads.WorkflowSpec import WorkflowSpec
from MCPayloads.LFNAlgorithm import unmergedLFNBase, mergedLFNBase
from MCPayloads.RelValSpec import getRelValSpecForVersion
from CMSConfigTools.CfgInterface import CfgInterface


prodAgentService = xmlrpclib.Server("http://127.0.0.1:8081")


valid = ['url=', 'version=', 'relvalversion=', 'events=']
usage = "Usage: releaseValdidation.py --url=<Spec XML URL>\n"
usage += "                            --version=<CMSSW version to be used>\n"
usage += "                            --relvalversion=<version to be used in spec file>\n"
usage += "                            --events=<events per job>\n"
usage += "                            "
usage += "You must have a scram runtime environment setup to use this tool\n"
usage += "since it will invoke EdmConfig tools\n\n"
usage += "Events per job defaults to 100 for faster finishing jobs\n"


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

xmlFile = None
version = None
relvalVersion = None
category = "RelVal"
timestamp = int(time.time())
eventsPerJob = 100


for opt, arg in opts:
    if opt == "--url":
        xmlFile = arg
    if opt == "--version":
        version = arg
    if opt == "--relvalversion":
        relvalVersion = arg
    if opt == "--events":
        eventsPerJob = int(arg)

if xmlFile == None:
    msg = "--url option not provided: This is required"
    raise RuntimeError, msg

if version == None:
    msg = "--version option not provided: This is required"
    raise RuntimeError, msg
if relvalVersion == None:
    msg = "--relvalVersion not provided, falling back to: %s" % version
    print msg

try:
    relValSpec = getRelValSpecForVersion(xmlFile, relvalVersion)
except StandardError, ex:
    msg = "Error retrieving Release Validation Spec File:\n"
    msg += "%s\n" % xmlFile
    msg += str(ex)
    print msg
    sys.exit(1)

if relValSpec == None:
    msg =  "Unable to extract release validation spec from file:\n"
    msg += "%s\n" % xmlFile
    msg += "Release Validation Version requested: %s\n " % relvalVersion
    msg += "You may need to provide --relvalversion as a command line option"
    print msg
    sys.exit(1)


summaryJobs = 0
summaryEvents = 0


for relTest in relValSpec:
    cfgUrl = relTest['CfgUrl']
    prodName = relTest['Name']
    cfgFile = os.path.join(os.getcwd(), "%s.cfg" % relTest['Name'])
    numberOfJobs = int( int(relTest['Events']) / eventsPerJob) + 1
    wgetCommand = "wget %s -O %s" % (cfgUrl, cfgFile)
    os.system(wgetCommand)
    
    pycfgFile = "%s.pycfg" % relTest['Name']
    hashFile = "%s.hash" % relTest['Name']

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
    spec.setRequestCategory(category)
    spec.setRequestTimestamp(timestamp)

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
    
    mergedLFNBase(spec)
    unmergedLFNBase(spec)

    spec.save("%s-Workflow.xml" % prodName)


    print "Created: %s-Workflow.xml" % prodName
    print "Created: %s " % pycfgFile
    print "Created: %s " % hashFile
    print "From: %s " % cfgFile




    #  //
    # // Inject the workflow into the ProdAgent and trigger job creation
    #//
    
    prodAgentService
    workflowBase = "%s-Workflow.xml" % prodName
    workflow = os.path.join(os.getcwd(), workflowBase)

    print "Creating Jobs..."
    print "%s jobs being created for workflow: %s" % (numberOfJobs, workflowBase)

    prodAgentService.publishEvent("RequestInjector:SetWorkflow", workflow)
    prodAgentService.publishEvent("RequestInjector:SelectWorkflow", workflowBase)
    prodAgentService.publishEvent("RequestInjector:SetEventsPerJob", str(eventsPerJob))
    prodAgentService.publishEvent("RequestInjector:NewDataset")
    #prodAgentService.publishEvent("JobCreator:SetCreator", "OSGCreator")
    #prodAgentService.publishEvent("JobSubmitter:SetSubmitter", "NoSubmit")
    
    for i in range(0, numberOfJobs):
        summaryJobs += 1
        summaryEvents += eventsPerJob

        time.sleep(.1)
        prodAgentService.publishEvent("ResourcesAvailable")
        
    
print "Total Jobs Created: %s" % summaryJobs
print "Total Events for all jobs: %s" % summaryEvents
