#!/usr/bin/env python
"""

_BulkGenerator_

JobGenerator that generates a common workflow sandbox that is
parametrized on the job spec

"""
import logging
from JobCreator.GeneratorInterface import GeneratorInterface
from JobCreator.Registry import registerGenerator


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

import inspect
import os

import ProdCommon
import ShREEK
import ShLogger
import IMProv
import ProdCommon.CMSConfigTools 
import RunRes
import FwkJobRep
import StageOut
import SVSuite

_StandardPackages = [ShREEK, ShLogger, IMProv, StageOut,
                     ProdCommon.CMSConfigTools, RunRes, FwkJobRep, SVSuite]

def makeTaskObject(payloadNode):
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

        workflowSpec.payload.operate(makeTaskObject)
        jobTemplate = os.path.join(workflowCache, "template")
        
        directory = self.newJobArea(workflowSpec.workflowName(), jobTemplate)
        taskObject = workflowSpec.payload.taskObject
        generateShREEKConfig(taskObject)
        
        taskObject(GenerateMainScript())
        taskObject(InsertAppDetails("PayloadNode"))
        logging.debug(
            "JobGenerator: Calling Creator:")
        
        #self.creator(taskObject)
        logging.debug("JobGenerator: Creator finished")
 
        taskObject(BashEnvironmentMaker())
        taskObject(PopulateMainScript())
        
        
        logging.debug("JobGenerator:Creating Physical Job")
        taskObject(FlatTaskDirBuilder(directory))
       
        taskObject(WriteStructuredFiles())
        taskObject(WriteIMProvDocs())
        writeShREEKConfig(directory, taskObject)
        
        
        return


    def actOnJobSpec(self, jobSpec, jobCache):
        """
        Populate the cache for the individual JobSpec

        """
        logging.info(
            "BulkGenerator.actOnJobSpec(%s, %s)" % (jobSpec, jobCache)
            )
        jobname = jobSpec.parameters['JobName']
        jobSpec.save("%s/%s-JobSpec.xml" % (jobCache, jobname))
        



        return




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
        taskObj.addEnvironmentVariable("RUNRESDB_URL", "file://`pwd`/RunResDB.xml")
        envMaker = BashEnvironmentMaker("jobEnvironment.sh")
        envMaker(taskObj)

        #  //
        # // Generate main execution script for job
        #//
        mainScript = taskObj.addStructuredFile("run.sh")
        mainScript.setExecutable()
        mainScript.append("#!/bin/sh")
        mainScript.append("PRODAGENT_JOBSPEC=$1")
        mainScript.append("echo \"$PRODAGENT_JOBSPEC\"")
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
        return taskObj['Directory']['AbsName']
        
        
registerGenerator(BulkGenerator, "Bulk")

