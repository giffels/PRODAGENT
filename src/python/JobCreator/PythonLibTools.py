#!/usr/bin/env python
"""
_PythonLibTools_

Utils for installing python libs into task objects

"""

import inspect
import os

from TaskObjects.TaskObject import Directory, TaskObject
from TaskObjects.Tools.TaskDirBuilder import TreeTaskDirBuilder

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
    return modSrc


def moduleToPaths(modName):
    """
    _moduleToPaths_

    break down a Dir1.Dir2.Module style name into a list
    of dirnames

    """
    result = str(modName).split(".")
    return result


def moduleToTaskObjects(moduleName, topObject):
    """
    _moduleToTaskObjects_

    Replicate a python module structure with TaskObjects

    """
    dirList = moduleToPaths(moduleName)

    currentDir = topObject['Directory']
    thisModule = ""
    theModule = dirList[-1]
    for dirName in dirList[:-1]:
        if thisModule != "":
            thisModule += "."
        thisModule += dirName
        if dirName not in currentDir.children.keys():
            currentDir = currentDir.addDirectory(dirName)
        else:
            currentDir = currentDir.children[dirName]
            
        if not currentDir.files.has_key("__init__.py"):
            moduleInit = findScriptSource(thisModule)
            currentDir.addFile(moduleInit)

    modSrc = findScriptSource(moduleName)
    if not currentDir.files.has_key(theModule):
        currentDir.addFile(os.path.dirname(modSrc))
    return


class PythonLibInstaller:
    """
    _PythonLibInstaller_
    
    Tool for installing python libraries into the job when called on
    the workflow spec or to make a new job area
    
    """
    def __init__(self, *modules):
        self.modules = modules


    def __call__(self, taskObject):
        """
        _operator(taskObject)_

        Create the localPython TaskObject and populate it with the python libs provided
        in the ctor
        
        """
        pythonObj = TaskObject("localPython")
        taskObject.addChild(pythonObj)

        for modName in self.modules:
            moduleToTaskObjects(modName, pythonObj)

        return


if __name__ == '__main__':

    moduleNames = ["T0.DataStructs", "ProdCommon.MCPayloads",
                   "ProdCommon.DataMgmt.JobSplit"]

    StandardPackages = ["ShREEK", "IMProv", "StageOut", "ProdCommon.MCPayloads",
                         "ProdCommon.CMSConfigTools.ConfigAPI" ,
                         "ProdCommon.Core",
                         "RunRes", "ProdCommon.FwkJobRep"]
    StandardPackages.extend(moduleNames)
    taskObj = TaskObject("top")
    
    installer = PythonLibInstaller(*StandardPackages)
    installer(taskObj)

    jobCache = "/Users/evansde/Work/devel/PRODAGENT/src/python/JobCreator/detritus/"
    dirMaker = TreeTaskDirBuilder(jobCache)
    taskObj(dirMaker)


    
    
    
    

    


