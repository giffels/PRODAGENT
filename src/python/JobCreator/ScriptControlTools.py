#!/usr/bin/env python
"""
_ScriptControlTools_

Utils for controlling runtime scripts from a workflow spec file

The WorkflowSpec's payload nodes can contain a set of scripts to be
included in that nodes task in four lists:

PreTask - Start of the runtime task, outside the exe subshell
PreExe  - Start of runtime task, inside the exe subshell (after PreTask)
PostExe - Immediately post executable in the exe subshell
PostTask - At the end of the task, outside the exe subshell (after PostExe)

Each of these is represented as a list in the PayloadNode.scriptControls
attribute.

Entries in the list should be the name of a Runtime script module.

The module is found by import/inspect and added to the TaskObject

"""


import inspect
import os
from JobCreator.ScramSetupTools import setupRuntimeCfgGenerationScript


def findScriptSource(moduleName):
    """
    _findScriptSource_

    Find the source file for the moduleName provided using import/inspect

    """
    mod = __import__(moduleName)
    components = moduleName.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp, None)
        if mod == None:
            break

    if mod == None:
        msg = "Unable to import module named: %s" % moduleName
        raise RuntimeError, msg

    modSrc = inspect.getsourcefile(mod)
    if not os.access(modSrc, os.X_OK):
        os.system("chmod +x %s" % modSrc)
    return modSrc



class InstallScriptControls:
    """
    _InstallScriptControls_

    Operator to act on a TaskObject and install the scripts requested
    by the workflow if possible.

    """
    def __call__(self, taskObject):
        """
        _operator(taskObject)_

        Grab the JobSpecNode, extract the list of scripts to be added
        and attempt to add them

        """
        jobSpecNode = taskObject.get("JobSpecNode", None)
        if jobSpecNode == None:
            jobSpecNode = taskObject['PayloadNode']

        scriptControls = jobSpecNode.scriptControls

        if taskObject['Type'] == "StageOut":
            preScripts = []
            preScripts.extend(scriptControls['PreTask'])
            preScripts.extend(scriptControls['PreExe'])

            postScripts = []
            postScripts.extend(scriptControls['PostExe'])
            postScripts.extend(scriptControls['PostTask'])

            for item in preScripts:
                sourceFile = findScriptSource(item)
                taskObject.attachFile(sourceFile)
                taskObject['PreStageOutCommands'].append(
                    "./%s" % os.path.basename(sourceFile))
            for item in postScripts:
                sourceFile = findScriptSource(item)
                taskObject.attachFile(sourceFile)
                taskObject['PostStageOutCommands'].append(
                    "./%s" % os.path.basename(sourceFile))

            return


        if taskObject['Type'] == "CleanUp":
            preScripts = []
            preScripts.extend(scriptControls['PreTask'])
            preScripts.extend(
                scriptControls['PreExe'])
            postScripts = []
            postScripts.extend(scriptControls['PostExe'])
            postScripts.extend(scriptControls['PostTask'])


            for item in preScripts:
                sourceFile = findScriptSource(item)
                taskObject.attachFile(sourceFile)
                taskObject['PreCleanUpCommands'].append(
                    "./%s" % os.path.basename(sourceFile))
            for item in postScripts:
                sourceFile = findScriptSource(item)
                taskObject.attachFile(sourceFile)
                taskObject['PostCleanUpCommands'].append(
                    "./%s" % os.path.basename(sourceFile))

            return

        if taskObject['Type'] == "CMSSW":
            for item in scriptControls['PreTask']:
                sourceFile = findScriptSource(item)
                taskObject.attachFile(sourceFile)
                taskObject['PreTaskCommands'].append(
                    "./%s" % os.path.basename(sourceFile))
            for item in scriptControls['PreExe']:
                sourceFile = findScriptSource(item)
                taskObject.attachFile(sourceFile)
                taskObject['PreAppCommands'].append(
                        setupRuntimeCfgGenerationScript(
                            "./%s" % os.path.basename(sourceFile)))
            for item in scriptControls['PostExe']:
                sourceFile = findScriptSource(item)
                taskObject.attachFile(sourceFile)
                taskObject['PostAppCommands'].append(
                    "./%s" % os.path.basename(sourceFile))
            for item in scriptControls['PostTask']:
                sourceFile = findScriptSource(item)
                taskObject.attachFile(sourceFile)
                taskObject['PostTaskCommands'].append(
                    "./%s" % os.path.basename(sourceFile))
            return












