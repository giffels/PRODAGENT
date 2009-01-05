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

        return


