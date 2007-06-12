#!/usr/bin/env python
"""
_CmsGenTools_

Tools for installing/manipulating a CmsGen type workflow node within
a workflow

"""

import inspect
import os

from JobCreator.AppTools import _StandardPreamble, _StandardAbortCheck
from JobCreator.AppTools import _StandardExitCodeCheck
import JobCreator.RuntimeTools.RuntimeCmsGen as RuntimeCmsGen


from ShREEK.ControlPoints.CondImpl.CheckExitCode import CheckExitCode
from ShREEK.ControlPoints.ActionImpl.BasicActions import KillJob

#  //
# // Hardcoded at present, until we distribute the tool properly...
#//
CmsGenScriptUrl = "http://cern.ch/ceballos/alpgen/bin/cmsGen.py"


class InsertCmsGenStructure:
    """
    _InsertCmsGenStructure_

    TaskObject operator.

    Act on a CmsGen type TaskObject and install some standard structure
    for the TaskObject so that commands can be added to it

    These fields get commands added to them by the creator plugin
    allowing it to be customised if necessary.

    Then the contents of the object gets built into an actual script
    by the PopulateCmsGenScript operator below
    
    """
    def __init__(self, nodeType = "PayloadNode"):
        self.nodeType = nodeType
        
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a TaskObject, install a standard structure for generating
        the main Executable script that calls cmsGen

        """
        spec = taskObject[self.nodeType]
        if spec.type != "CmsGen":
            return
        
        appDetails = spec.application
        taskObject['CMSProjectName'] = spec.application['Project']
        taskObject['CMSProjectVersion'] = spec.application['Version']
        taskObject['CMSExecutable'] = spec.application['Executable']
        taskObject['CmsGenConfiguration'] = spec.configuration
        
        #  //
        # // Add an empty structured file to contain the PSet after
        #//  it is converted from the Python format. 
        taskObject.addStructuredFile("CmsGen.cfg")
        
        
            
        #  //
        # // Add structures to enable manipulation of task main script
        #//  These fields are used to add commands and script calls
        #  //at intervals in the main script.
        # //
        #//
        taskObject['PreTaskCommands'] = []
        taskObject['PostTaskCommands'] = []
        taskObject['PreAppCommands'] = []
        taskObject['PostAppCommands'] = []



      
        #  //
        # // Insert End Control Point check on exit status
        #//
        controlP = taskObject['ShREEKTask'].endControlPoint
        exitCheck = CheckExitCode()
        exitCheck.attrs['OnFail'] = "killJob"
        exitAction = KillJob("killJob")
        controlP.addConditional(exitCheck)
        controlP.addAction(exitAction)
        
        return


class PopulateCmsGenScript:
    """
    _PopulateCmsGenScript_

    Act on the TaskObject to convert fields into commands and insert them
    into the main script structured file instance.

    """
    def __init__(self, nodeType = "PayloadNode"):
        self.nodeType = nodeType
        
    def __call__(self, taskObject):
        """
        _operator()_

        For a TaskObject that has the appropriate App Keys generate
        a standard task running script
        
        """
        spec = taskObject[self.nodeType]
        if spec.type != "CmsGen":
            return
        
        exeScript = taskObject[taskObject['Executable']]
        
        #  //
        # // Install standard error handling command
        #//
        exeScript.append(_StandardPreamble)
        
        envScript = taskObject[taskObject["BashEnvironment"]]
        envCommand = "%s %s" % (envScript.interpreter, envScript.name)
        exeScript.append(envCommand)

        srcfile = inspect.getsourcefile(RuntimeCmsGen)
        taskObject.attachFile(srcfile)
        taskObject['PreTaskCommands'].append("chmod +x ./RuntimeCmsGen.py")
        taskObject['PreTaskCommands'].append(
            "./RuntimeCmsGen.py"
            )
        
        for item in taskObject['PreTaskCommands']:
            exeScript.append(item)

        #  //
        # // Pull in the cmsGen tool from the web and
        #// make sure it is executable
        exeScript.append("wget %s -O cmsGen" % CmsGenScriptUrl)
        exeScript.append("chmod +x cmsGen") 
        
        exeScript.append("( # Start App Subshell")
        for item in taskObject['PreAppCommands']:
            exeScript.append(item)

        #  //
        # // Need to set command line args at runtime
        #//  and pass them to the cmsGen command
        #  //The RuntimeCmsGen.py script will generate a file
        # // called cmsGen.args which we cat to extract the content
        #//
        checkArgs =  "if [ -e %s ];then\n" % "cmsGen.args"
        checkArgs += "    echo \"cmsGen.args is present\"\n"
        checkArgs += "else\n"
        checkArgs += "    echo \"ERROR: cmsGen.args not present\"\n"
        checkArgs += "    prodAgentFailure 50113\n"
        checkArgs += "fi\n"
        exeScript.append(checkArgs)
        exeScript.append(_StandardAbortCheck)
        
        #  //
        # // Build Executable command
        #//
        exeComm = "./%s `cat cmsGen.args` &" % taskObject['CMSExecutable']
        exeScript.append(exeComm)
        exeScript.append("PROCID=$!")
        exeScript.append("echo $PROCID > process_id")
        exeScript.append("wait $PROCID")
        exeScript.append("EXIT_STATUS=$?")
        exeScript.append(_StandardExitCodeCheck)
        exeScript.append(
            "if [ ! -e exit.status ]; then echo \"$EXIT_STATUS\" > exit.status; fi")
        exeScript.append("echo \"App exit status: $EXIT_STATUS\"")
        for item in taskObject['PostAppCommands']:
            exeScript.append(item)
        exeScript.append("exit $EXIT_STATUS")
        exeScript.append(") # End of App Subshell")
        exeScript.append("EXIT_STATUS=$?")
        exeScript.append("echo `date +%s` >| end.time")
        for item in taskObject['PostTaskCommands']:
            exeScript.append(item)
        exeScript.append("echo \"Ended: `date +%s`\"")
        exeScript.append("exit $EXIT_STATUS")

        
        return
