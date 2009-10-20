#!/usr/bin/env python
"""

_BulkGenerator_

JobGenerator that generates a common workflow sandbox that is
parametrized on the job spec

"""
import logging
from popen2 import Popen4

from JobCreator.GeneratorInterface import GeneratorInterface
from JobCreator.Registry import registerGenerator

from ProdAgentCore.Configuration import prodAgentName

from TaskObjects.TaskObject import TaskObject
from TaskObjects.Tools.TaskDirBuilder import TreeTaskDirBuilder
from TaskObjects.Tools.BashEnvironmentMaker import BashEnvironmentMaker
from TaskObjects.Tools.WriteStructuredFiles import WriteStructuredFiles
from TaskObjects.Tools.TaskDirBuilder import FlatTaskDirBuilder
from TaskObjects.Tools.WriteIMProvDocs import WriteIMProvDocs
from TaskObjects.Tools.WriteShREEKConfig import generateShREEKConfig
from TaskObjects.Tools.WriteShREEKConfig import writeShREEKConfig
from TaskObjects.Tools.GenerateMainScript import GenerateMainScript
from JobCreator.PrestageTool import InstallPrestage
from JobCreator.PythonLibTools import PythonLibInstaller
from JobCreator.AppTools import InsertBulkAppDetails, PopulateMainScript
from JobCreator.AppTools import InsertJobReportTools
from JobCreator.RunResTools import InstallRunResComponent
from JobCreator.RunResTools import AccumulateRunResDB
from JobCreator.RunResTools import BulkCMSSWRunResDB, InsertDirInRunRes
from JobCreator.StageOutTools import NewInsertStageOut
from JobCreator.StageOutTools import NewPopulateStageOut
from JobCreator.CleanUpTools import InsertCleanUp, PopulateCleanUp
from JobCreator.LogArchTools import InstallLogArch, PopulateLogArch
from JobCreator.BulkTools import InstallUnpacker, InstallUserSandbox
from JobCreator.FastMergeTools import InstallBulkFastMerge
from JobCreator.DashboardTools import installBulkDashboardInfo, writeDashboardInfo
from JobCreator.CmsGenTools import InsertCmsGenStructure, PopulateCmsGenScript
from JobCreator.FrontierTools import InsertFrontierTools
from JobCreator.ScriptControlTools import InstallScriptControls
from JobCreator.LogCollectTools import InsertLogCollect, PopulateLogCollect 

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo, generateDashboardID

import inspect
import os

import ProdCommon
import ShREEK
import IMProv
import ProdCommon.CMSConfigTools
import ProdCommon.Core
import ProdCommon.MCPayloads
import RunRes
import ProdCommon.FwkJobRep
import ProdCommon.TrivialFileCatalog
import StageOut


_StandardPackages = ["ShREEK", "IMProv", "StageOut", "ProdCommon.MCPayloads",
                     "ProdCommon.CMSConfigTools",
                     "ProdCommon.Core",
                     "RunRes", "ProdCommon.FwkJobRep",
                     "ProdCommon.TrivialFileCatalog",
                     "WMCore.Algorithms"]


class TaskObjectMaker:
    def __init__(self, jobType = "Processing"):
        self.jobType = jobType


    def __call__(self, payloadNode):
        """
        _makeTaskObject_
        
        Operator to act on a PayloadNode instance and generate a TaskObject
        for it.
        
        """
        taskName = payloadNode.name
        taskObj = TaskObject(taskName)
        taskObj['Type'] = payloadNode.type
        taskObj['RequestName'] = payloadNode.workflow
        taskObj['JobName'] = payloadNode.name
        taskObj['JobType'] = self.jobType
        setattr(payloadNode, "taskObject", taskObj)
        
        if payloadNode.parent != None:
            parentTaskObj = getattr(payloadNode.parent, "taskObject")
            parentTaskObj.addChild(taskObj)
            
        taskObj['PayloadNode'] = payloadNode
    
        return


class BulkGenerator(GeneratorInterface):
    """
    _BulkGenerator_

    """
    def actOnWorkflowSpec(self, workflowSpec, workflowCache):
        """
        Create the workflow wide job template for jobs
        """
        logging.info(
            "BulkGenerator.actOnWorkflowSpec(%s, %s)" % (
               workflowSpec, workflowCache)
            )
        
        wftype = workflowSpec.parameters['WorkflowType']
        
        logging.info("Generating template for %s type jobs" % wftype)
        workflowSpec.payload.operate(TaskObjectMaker(wftype))
        
        jobTemplate = os.path.join(workflowCache, wftype)
        self.workflowSpec = workflowSpec
        
        directory = self.newJobArea(workflowSpec.workflowName(), jobTemplate)
        taskObject = workflowSpec.payload.taskObject
        taskObject['Activity'] = workflowSpec.parameters.get("Activity", None)
        generateShREEKConfig(taskObject)
        installBulkDashboardInfo(taskObject)
        taskObject(GenerateMainScript())
        taskObject(InsertBulkAppDetails("PayloadNode"))
        taskObject(InsertCmsGenStructure("PayloadNode"))
        taskObject(InstallRunResComponent())
        taskObject(InsertFrontierTools(self.componentConfig.get("FrontierDiagnostic", False)))
        taskObject(InsertJobReportTools())
        taskObject(InsertCleanUp())
        taskObject(InstallLogArch())
        taskObject(InstallBulkFastMerge())
        taskObject(NewInsertStageOut())
        taskObject(InstallUserSandbox())
        taskObject(InsertLogCollect())
        
        logging.debug(
            "JobGenerator: Calling Creator:")
        
        self.creator(taskObject)
        logging.debug("JobGenerator: Creator finished")

        #//Enabling Prestage on Workflow
        taskObject['PreStage'] = workflowSpec.parameters.get("PreStage",'False')
        taskObject(InstallPrestage())

        taskObject(InstallUnpacker())
        taskObject(InstallScriptControls())
        taskObject(BashEnvironmentMaker())
        taskObject(PopulateMainScript())
        taskObject(PopulateCmsGenScript("PayloadNode"))
        taskObject(PopulateCleanUp())
        taskObject(PopulateLogArch(self.componentConfig.get("LogArchStageOut", False)))
        taskObject(NewPopulateStageOut())
        taskObject(PopulateLogCollect())
        
        logging.debug("JobGenerator:Creating Physical Job")
        logging.debug("directory=%s" % directory)
        taskObject(FlatTaskDirBuilder(directory))
        taskObject(BulkCMSSWRunResDB(**self.componentConfig))
        taskObject(InsertDirInRunRes())
        writeDashboardInfo(taskObject, jobTemplate)
        taskObject(WriteStructuredFiles())
        taskObject(WriteIMProvDocs())

        accumRunRes = AccumulateRunResDB()
        taskObject(accumRunRes)
        accumRunRes.writeMainDB(os.path.join(directory, "RunResDB.xml"))

        
        writeShREEKConfig(directory, taskObject)

        tarName = "%s-%s.tar.gz" % (workflowSpec.workflowName(),
                                    wftype)
        createTarball(directory, tarName)
        
        return


    def actOnJobSpec(self, jobSpec, jobCache):
        """
        Populate the cache for the individual JobSpec

        """
        logging.info(
            "BulkGenerator.actOnJobSpec(%s, %s)" % (jobSpec, jobCache)
            )
        jobname = jobSpec.parameters['JobName']
        jobSpec.parameters['ProdAgentName'] = prodAgentName()


        jobname = jobSpec.parameters['JobName']
        jobType = jobSpec.parameters['JobType']
        workflowName = jobSpec.payload.workflow

        commonTarball = os.path.join(self.workflowCache, jobType)
        commonTarball += "/%s-%s.tar.gz" % (workflowName, jobType)
        jobSpec.parameters['BulkInputSandbox'] = commonTarball
        jobSpecFile = "%s/%s-JobSpec.xml" % (jobCache, jobname) 
        jobSpec.save(jobSpecFile)
        
        
        # Propagate dashboard info to job cache
        dashboardInfoMaster = os.path.join(self.workflowCache, jobType,
                                           "DashboardInfo.xml")
        
        if os.path.exists(dashboardInfoMaster):
            master = DashboardInfo()
            master.read(dashboardInfoMaster)
            task, job = generateDashboardID(jobSpec)
            master.task = task
            master.job = job
            jobCopy = os.path.join(jobCache, "DashboardInfo.xml")
            master.write(jobCopy)
        
            
            
        return jobSpecFile




    def newJobArea(self, jobname, jobCache):
        """
        _newJobArea_

        Create a new job area for a job in the working dir provided.
        A standard package-able job area is created containing
        a job local python and binary area with a ShREEK installation.

        A standard environment is provided and a main script is setup
        and run. Executable tasks are added to it by inserting a
        ShREEKConfiguration XML file into the toplevel directory of the
        job.

        The standard job framework also defines the variable
        PRODAGENT_JOB_DIR which is set at runtime and can be used to
        access objects relatively within the job.

        The localBin dir created is automatically added to the jobs
        PATH setup so that small binaries can be added to the job if required.

        The localPython dir contains python libraries to be transported with
        the job. Care should be taken to ensure that any python lib
        is python 2.2 compliant, since that is the standard in Scientific
        Linux (even if it is yonks old...)

        Note: TaskObjects are used to construct the skeleton for convienience,
        this TaskObject is not seen externally.
        """
        #  //
        # // build basic structure
        #//
        taskObj = TaskObject(jobname)
        binObj = TaskObject("localBin")
        taskObj.attachFile(self.workflowFile)
        taskObj.addChild(binObj)
        
        
        
        #  //
        # // Attach standard python packages and shreek binary
        #//
        shreekBin = os.path.join(
            os.path.dirname(inspect.getsourcefile(ShREEK)), "shreek")
        binObj.attachFile(shreekBin)

        #  //
        # // Include python libs specified in both StandardPackages and in the 
        #//  WorkflowSpec itself
        modules = set(_StandardPackages)
        for wfmodule in self.workflowSpec.pythonLibraries():
            modules.add(wfmodule)

        installer = PythonLibInstaller(*modules)
        installer(taskObj)


        #  //
        # // Generate standard environment settings
        #//
        taskObj.addEnvironmentVariable("PYTHONPATH",
                                       "$PYTHONPATH", "`pwd`/localPython")
        taskObj.addEnvironmentVariable("PATH", "$PATH", "`pwd`/localBin")
        taskObj.addEnvironmentVariable("PRODAGENT_JOB_DIR", "`pwd`")
        taskObj.addEnvironmentVariable(
            "PRODAGENT_WORKFLOW_SPEC",
            "`pwd`/%s" % os.path.basename(self.workflowFile))
        
        taskObj.addEnvironmentVariable("RUNRESDB_URL",
                                       "file://`pwd`/RunResDB.xml")
        envMaker = BashEnvironmentMaker("jobEnvironment.sh")
        envMaker(taskObj)

        #  //
        # // Generate main execution script for job
        #//
        mainScript = taskObj.addStructuredFile("run.sh")
        mainScript.setExecutable()
        mainScript.append("#!/bin/sh")
        mainScript.append("export PRODAGENT_JOBSPEC=$1")
        mainScript.append("echo \"Job Spec: $PRODAGENT_JOBSPEC\"")
        mainScript.append(". jobEnvironment.sh")
        mainScript.append("shreek --config=./ShREEKConfig.xml")
        
        #  //
        # // Now build the directory and file structure in the working dir
        #//  Make sure the dir doesnt exist.
        if os.path.exists(jobCache):
            os.system("/bin/rm -rf %s " % jobCache )
        dirMaker = TreeTaskDirBuilder(jobCache)
        taskObj(dirMaker)
        #  //
        # // Write the scripts required.
        #//
        scriptWriter = WriteStructuredFiles()
        taskObj(scriptWriter)

        #  //
        # // Return the new job directory so that 
        #//  it can be populated with tasks
        thisPath = os.path.join(jobCache, taskObj['Directory'].name)
        return thisPath
        

def createTarball(dirName, tarballName):
    """
    _createTarball_

    Tar up the common sandbox

    """
    tarballFile = os.path.join(os.path.dirname(dirName), tarballName)
    if os.path.exists(tarballFile):
        logging.debug(
            "createTarball:Tarball exists, cleaning: %s" % tarballFile)
        os.remove(tarballFile)

    tarComm = "tar -czf %s -C %s %s " % (
        tarballFile,
        os.path.dirname(dirName),
        os.path.basename(dirName)
        )
    logging.info("Creating Tarball for workflow: %s" % tarComm)
    pop = Popen4(tarComm)
    while pop.poll() == -1:
            exitCode = pop.poll()
    exitCode = pop.poll()
    if exitCode:
        msg = "Error creating Tarfile:\n"
        msg += tarComm
        msg += "Exited with code: %s\n" % exitCode
        msg += pop.fromchild.read()
        logging.error("createTarball: Tarball creation failed:")
        logging.error(msg)
        raise RuntimeError, msg
    return
      
registerGenerator(BulkGenerator, "Bulk")

