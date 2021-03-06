#!/usr/bin/env python
"""

_DefaultGenerator_

JobGenerator that generates an individual sandbox for every job

"""
import logging
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

from JobCreator.AppTools import InsertAppDetails, PopulateMainScript
from JobCreator.AppTools import InsertPythonPSet, InsertJobReportTools
from JobCreator.RunResTools import InstallRunResComponent
from JobCreator.RunResTools import AccumulateRunResDB
from JobCreator.RunResTools import CMSSWRunResDB, InsertDirInRunRes
from JobCreator.StageOutTools import NewInsertStageOut
from JobCreator.StageOutTools import  NewPopulateStageOut
from JobCreator.DashboardTools import installDashboardInfo, writeDashboardInfo
from JobCreator.CleanUpTools import InsertCleanUp, PopulateCleanUp
from JobCreator.CmsGenTools import InsertCmsGenStructure, PopulateCmsGenScript
from JobCreator.LogArchTools import InstallLogArch, PopulateLogArch
from JobCreator.LogCollectTools import InsertLogCollect, PopulateLogCollect 

import inspect
import os
import logging

import ProdCommon
# Silence deprecation warnings until all runtime python > python2.6
import warnings
warnings.filterwarnings("ignore", category = DeprecationWarning)
import ShREEK
warnings.filterwarnings("default", category = DeprecationWarning)
import IMProv
import ProdCommon.CMSConfigTools 
import ProdCommon.MCPayloads
import ProdCommon.Core
import RunRes
import ProdCommon.FwkJobRep
import StageOut
import ProdCommon.TrivialFileCatalog

_StandardPackages = [ShREEK, IMProv, StageOut, ProdCommon.MCPayloads,
                     ProdCommon.Core,
                     ProdCommon.CMSConfigTools, RunRes, ProdCommon.FwkJobRep,
                     ProdCommon.TrivialFileCatalog]


class TaskObjectMaker:
    def __init__(self, jobSpecInstance):
        self.jobSpecInstance = jobSpecInstance

    def __call__(self, jobSpecNode):
        """
        _makeTaskObject_
        
        Operator to act on a JobSpecNode instance and generate a TaskObject
        for it.
        
        """
        taskName = jobSpecNode.name
        taskObj = TaskObject(taskName)
        taskObj['Type'] = jobSpecNode.type
        taskObj['RequestName'] = jobSpecNode.workflow
        taskObj['JobName'] = jobSpecNode.jobName
        taskObj['JobType'] = jobSpecNode.jobType
        setattr(jobSpecNode, "taskObject", taskObj)
        
        if jobSpecNode.parent != None:
            parentTaskObj = getattr(jobSpecNode.parent, "taskObject")
            parentTaskObj.addChild(taskObj)
    
        taskObj['JobSpecNode'] = jobSpecNode
        taskObj['JobSpecInstance'] = self.jobSpecInstance
        return 


class DefaultGenerator(GeneratorInterface):
    """
    _DefaultGenerator_

    """
    def actOnWorkflow(self, workflowSpec, workflowCache):
        """
        Nothing to do in this case
        """
        workflowSpec.save(os.path.join(workflowCache, "WorkflowSpec.xml"))
        return
    

    def actOnJobSpec(self, jobSpec, jobCache):
        """
        Create an individual self contained job

        """
        logging.info(
            "DefaultGenerator.actOnJobSpec(%s, %s)" % (jobSpec, jobCache)
            )

        jobname = jobSpec.parameters['JobName']
        self._JobSpec = jobSpec
        makeTaskObject = TaskObjectMaker(jobSpec)
        self._JobSpec.payload.operate(makeTaskObject)
        self._TaskObject = self._JobSpec.payload.taskObject
        self._WorkingDir = jobCache
        jobSpecFile = "%s/%s-JobSpec.xml" % (jobCache, jobname)
        runtimeJobSpecFile = "%s/%s/%s-JobSpec.xml" % (
            jobCache, jobname, jobname)
        
        directory = self.newJobArea(jobname, jobCache)

        logging.debug("JobGenerator: Job Directory: %s" % directory)
        cacheDir = os.path.dirname(directory)

        jobSpec.parameters['ProdAgentName'] = prodAgentName()
        jobSpec.save(jobSpecFile)
        jobSpec.save(runtimeJobSpecFile)

        #  //
        # // Insert Standard Objects into TaskObject tree
        #//
        taskObject = self._JobSpec.payload.taskObject
        generateShREEKConfig(taskObject)
        installDashboardInfo(taskObject)
        taskObject(GenerateMainScript())
        taskObject(InsertAppDetails())
        taskObject(InsertCmsGenStructure("JobSpecNode"))
        taskObject(InstallRunResComponent())

        taskObject(InsertJobReportTools())
        taskObject(InsertCleanUp())
        taskObject(InstallLogArch())
        taskObject(NewInsertStageOut())
        taskObject(InsertLogCollect())

        #  //
        # // Invoke the creator plugin on the TaskObject
        #//  so that customisations can be performed
        logging.debug(
            "DefaulGenerator: Calling Creator")
        
        self.creator(self._TaskObject)
        logging.debug("DefaultGenerator: Creator finished")

        #  //
        # // Create the actual jobs from the TaskObjects
        #//
        taskObject(BashEnvironmentMaker())
        taskObject(InsertPythonPSet())
        taskObject(PopulateMainScript())
        taskObject(PopulateCmsGenScript("JobSpecNode"))
        taskObject(PopulateCleanUp())
        taskObject(PopulateLogArch(self.componentConfig.get("LogArchStageOut", False)))
        taskObject(NewPopulateStageOut())
        taskObject(PopulateLogCollect())
        
        #  //
        # // Physical Job Creation starts here
        #//
        logging.debug("JobGenerator:Creating Physical Job")
        taskObject(FlatTaskDirBuilder(directory))
        taskObject(CMSSWRunResDB(**self.componentConfig))
        taskObject(InsertDirInRunRes())
        writeDashboardInfo(taskObject, cacheDir)
        taskObject(WriteStructuredFiles())
        taskObject(WriteIMProvDocs())
        accumRunRes = AccumulateRunResDB()
        taskObject(accumRunRes)
        accumRunRes.writeMainDB(os.path.join(directory, "RunResDB.xml"))
        writeShREEKConfig(directory, taskObject)
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
        pythonObj = TaskObject("localPython")
        prodCommonObj = TaskObject("ProdCommon")
        binObj = TaskObject("localBin")
        taskObj.addChild(pythonObj)
        taskObj.addChild(binObj)
        taskObj.attachFile(self.workflowFile)
        pythonObj.addChild(prodCommonObj)
        
        prodCommonInit = inspect.getsourcefile(ProdCommon)
        prodCommonObj.attachFile(prodCommonInit)
        
        
        #  //
        # // Attach standard python packages and shreek binary
        #//
        for pkg in _StandardPackages:
            srcfile = inspect.getsourcefile(pkg)
            if pkg.__name__.startswith("ProdCommon."):
                prodCommonObj.attachFile(os.path.dirname(srcfile))
            else:
                pythonObj.attachFile(os.path.dirname(srcfile))
            
        shreekBin = os.path.join(
            os.path.dirname(inspect.getsourcefile(ShREEK)), "shreek")
        binObj.attachFile(shreekBin)


        #  //
        # // Generate standard environment settings
        #//
        taskObj.addEnvironmentVariable("PYTHONPATH",
                                       "$PYTHONPATH", "`pwd`/localPython")
        taskObj.addEnvironmentVariable("PATH", "$PATH", "`pwd`/localBin")
        taskObj.addEnvironmentVariable("PRODAGENT_JOB_DIR", "`pwd`")
        taskObj.addEnvironmentVariable("PRODAGENT_JOBSPEC",
                                       "`pwd`/%s-JobSpec.xml" % jobname)
        taskObj.addEnvironmentVariable(
            "PRODAGENT_WORKFLOW_SPEC",
            "`pwd`/%s" % os.path.basename(self.workflowFile))
        
        taskObj.addEnvironmentVariable("RUNRESDB_URL", "file://`pwd`/RunResDB.xml")
        
        envMaker = BashEnvironmentMaker("jobEnvironment.sh")
        envMaker(taskObj)

        #  //
        # // Generate main execution script for job
        #//
        mainScript = taskObj.addStructuredFile("run.sh")
        mainScript.setExecutable()
        mainScript.append("#!/bin/sh")
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
        newCache = os.path.join(jobCache, taskObj['Directory'].name)
        return newCache
        
        

        

registerGenerator(DefaultGenerator, "Default")

