#!/usr/bin/env python
"""
_StageOutTools_

Tools for manipulating StageOut type nodes for including
StageOut processes in a job

"""

import logging
import inspect
import os
import StageOut.RuntimeStageOut as NewRuntimeStageOut
import StageOut.RuntimeStageOutFailure as RuntimeStageOutFailure
from IMProv.IMProvDoc import IMProvDoc
from xml.sax import make_parser
from IMProv.IMProvLoader import IMProvHandler
from IMProv.IMProvQuery import IMProvQuery
from ProdAgentCore.PluginConfiguration import loadPluginConfig

from ShREEK.ControlPoints.CondImpl.CheckExitCode import CheckExitCode
from ShREEK.ControlPoints.ActionImpl.BasicActions import SetNextTask


                          
class NewInsertStageOut:
    """
    _NewInsertStageOutAttrs_

    Put in standard StageOut specific fields in the task Objects

    """
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a taskObject and if its type is StageOut, add in the standard fields for a StageOut tgask

        """
        if taskObject['Type'] != "StageOut":
            return
        #  //
        # // Lists of pre stage out and post stage out shell commands
        #//  for setting up stage out env
        #  //Should be bash shell commands as strings
        taskObject['PreStageOutCommands'] = []
        taskObject['PostStageOutCommands'] = []

        #  //
        # // Determine what is being staged out from the parent node
        #//  (This should be a CMSSW node)
        parent = taskObject.parent
        if parent == None:
            # no parent => dont know what to stage out
            return

        if parent['Type'] not in  ("CMSSW", "SVSuite"):
            # parent isnt a CMSSW node, dont know what it does...
            return
        stageOutFor = parent['Name']
        taskObject['StageOutFor'] = taskObject['PayloadNode'].configuration
        return

_StageOutFailureScript = \
"""
EXIT_STATUS=$?
echo $EXIT_STATUS > exit.status
if [ $EXIT_STATUS -ne 0 ];then
   echo "Failure to invoke RuntimeStageOut.py: Exit $EXIT_STATUS"
   echo "Invoking Failure handler"
   chmod +x ./RuntimeStageOutFailure.py
   ./RuntimeStageOutFailure.py
fi

"""


class NewPopulateStageOut:
    """
    _NewPopulateStageOut_


    Take a StageOut task object and add standard stuff to it, including
    metadata fields, RunRes info and main script.

    Site specific customisation should be done in the Creator object
    for that site

    """
    def __call__(self, taskObject):
        """
        _operator()_


        Operate on a TaskObject of type StageOut to insert a standard
        StageOut structure

        """
        if taskObject['Type'] != "StageOut":
            return

     
        #  //
        # // Pre and Post Stage out commands
        #//
        precomms = taskObject.get("PreStageOutCommands", [])
        postcomms = taskObject.get("PostStageOutCommands", [])
        

        #  //
        # // Install the main script
        #//
        srcfile = inspect.getsourcefile(NewRuntimeStageOut)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)

        #  //
        # // Failure script
        #//
        fsrcfile = inspect.getsourcefile(RuntimeStageOutFailure)
        if not os.access(fsrcfile, os.X_OK):
            os.system("chmod +x %s" % fsrcfile)
        taskObject.attachFile(fsrcfile)
        
        exeScript = taskObject[taskObject['Executable']]
        
        envScript = taskObject[taskObject["BashEnvironment"]]
        envCommand = "%s %s" % (envScript.interpreter, envScript.name)
        exeScript.append(envCommand)
        
        for precomm in precomms:
            exeScript.append(str(precomm))
        exeScript.append("chmod +x ./RuntimeStageOut.py")   
        exeScript.append("./RuntimeStageOut.py")
        exeScript.append(_StageOutFailureScript )
        for postcomm in postcomms:
            exeScript.append(str(postcomm))


            
        #  //
        # // Insert End Control Point check on exit status
        #//
        controlP = taskObject['ShREEKTask'].endControlPoint
        exitCheck = CheckExitCode()
        exitCheck.attrs['OnFail'] = "skipToLog"
        exitAction = SetNextTask("skipToLog")
        exitAction.content = "logArchive"
        controlP.addConditional(exitCheck)
        controlP.addAction(exitAction)
        
        #  //
        # // Populate the RunResDB
        #//
        runres = taskObject['RunResDB']
        toName = taskObject['Name']
        paramBase = "/%s/StageOutParameters" % toName
        runres.addPath(paramBase)

        for stageOutFor in taskObject['PayloadNode'].configuration.split():
            runres.addData("/%s/StageOutFor" % paramBase, stageOutFor)
        taskObject['PayloadNode'].configuration = ""

        #  //
        # // Configuration for retries?
        #//
        try:
            creatorCfg = loadPluginConfig("JobCreator", "Creator")
            stageOutCfg = creatorCfg.get("StageOut", {})
            numRetries = int(stageOutCfg.get("NumberOfRetries", 3))
            retryPause = int(stageOutCfg.get("RetryPauseTime", 600))
            runres.addData("%s/RetryPauseTime" % paramBase, retryPause)
            runres.addData("%s/NumberOfRetries" % paramBase, numRetries)
            msg = "Stage Out Retries = %s; Pause = %s" % (
                numRetries, retryPause)
            logging.debug(msg)
        except:
            logging.debug("No Retry/Pause Stage Out cfg found")
        
        #  //
        # // Is there an override for this in the JobSpec??
        #//
        payloadNode = taskObject.get("JobSpecNode", None)
        if payloadNode == None:
            payloadNode = taskObject["PayloadNode"]
        cfgStr = payloadNode.configuration

        if len(cfgStr) == 0:
            return
        
        handler = IMProvHandler()
        parser = make_parser()
        parser.setContentHandler(handler)
        try:
            parser.feed(cfgStr)
        except Exception, ex:
            # No xml data, no override, nothing to be done...
            return

        logging.debug("StageOut Override provided")
        override = handler._ParentDoc
        commandQ = IMProvQuery("/Override/command[text()]")
        optionQ = IMProvQuery("/Override/option[text()]")
        seNameQ = IMProvQuery("/Override/se-name[text()]")
        lfnPrefixQ = IMProvQuery("/Override/lfn-prefix[text()]")
        
        command = commandQ(override)[0]
        option = optionQ(override)[0]
        seName = seNameQ(override)[0]
        lfnPrefix = lfnPrefixQ(override)[0]
        
        logging.debug("%s %s %s %s " % (command, option, seName, lfnPrefix))
        overrideBase = "/%s/StageOutParameters/Override" % toName
        runres.addPath(overrideBase)
        runres.addData("/%s/command" % overrideBase, command)
        runres.addData("/%s/option" % overrideBase, option)
        runres.addData("/%s/se-name" % overrideBase, seName)
        runres.addData("/%s/lfn-prefix" % overrideBase, lfnPrefix)
        
        return
                       
                        
