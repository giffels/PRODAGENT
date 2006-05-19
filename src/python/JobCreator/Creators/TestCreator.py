#!/usr/bin/env python
"""
_TestCreator_

Testing Job Creator plugin that generates a generic job that
can be run interactively to test the job creation

"""


from JobCreator.Registry import registerCreator
from JobCreator.Creators.CreatorInterface import CreatorInterface


    
class TestCreator(CreatorInterface):
    """
    _TestCreator_

    Simple Creator implementation for testing/development

    """
    def __init__(self):
        CreatorInterface.__init__(self)

    def processTaskObject(self, taskObject):
        typeVal = taskObject['Type']
        if typeVal == "CMSSW":
            handleCMSSWTaskObject(taskObject)
            return
        elif typeVal == "Script":
            handleScriptTaskObject(taskObject)
            return
        else:
            return
    
        

    
def handleCMSSWTaskObject(taskObject):
    """
    _handleCMSSWTaskObject_

    Method to customise CMSSW type (Eg cmsRun application) TaskObjects

    """
    test = taskObject.has_key("CMSProjectVersion") and \
           taskObject.has_key("CMSProjectName")
    if not test:
        return

    taskObject['Environment'].addVariable("SCRAM_ARCH", "slc3_ia32_gcc323")
    
    scramSetup = taskObject.addStructuredFile("scramSetup.sh")
    scramSetup.interpreter = "."
    taskObject['PreAppCommands'].append(". /uscms/prod/sw/cms/setup/bashrc")
    taskObject['PreAppCommands'].append(". scramSetup.sh")
    
    scramSetup.append("#!/bin/bash")
    scramSetup.append("scramv1 project %s %s" % (
        taskObject['CMSProjectName'], taskObject['CMSProjectVersion'])
                      )
    scramSetup.append("cd %s" % taskObject['CMSProjectVersion'])
    scramSetup.append("eval `scramv1 runtime -sh`")
    scramSetup.append("cd ..")
    return

def handleScriptTaskObject(taskObject):
    """
    _handleScriptTaskObject_

    Handle a Script type TaskObject, assumes the the Executable specifies
    a shell command, the command is extracted from the JobSpecNode and
    inserted into the main script

    """
    exeScript = taskObject[taskObject['Executable']]
    jobSpec = taskObject['JobSpecNode']
    exeCommand = jobSpec.application['Executable']
    exeScript.append(exeCommand)
    return

    




#  //
# // Register an instance of TestCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(TestCreator, "TestCreator")


