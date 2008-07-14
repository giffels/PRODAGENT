#!/usr/bin/env python
"""
_LCGCreator_

Testing Job Creator plugin that generates a job for the prodAgent Dev Node

"""


from JobCreator.Registry import registerCreator
                                                             
from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand
from JobCreator.ScramSetupTools import scramRuntimeCommand

def printTaskObjectDetails(taskObject):
    """
    recursive printout of task objects attributes, along with
    descriptions of some of the fields

    """
    desc = {
        "Name" : "Unique name of TaskObject in TaskObject Tree",
        "Executable" : "Name of StructuredFile instance containing main script",
        "ShREEKTask" : "Instance of ShREEKTask to run the executable script",
        "Directory" : "DMB instance that contains the task dir structure info",
        "Environment" : "Map of variable names to variables for task env",
        "CMSExecutable" : "CMS Exe to be run in main script",
        "CMSProjectVersion" : "Version of CMS Software project containing exe",
        "CMSPythonPSet" : "Python formatted string containing the PSet Python",
        
            }

    print "==>TaskObject Keys:"
    for key in taskObject.keys():
        print "===>", key, desc.get(key, "")
        if key in taskObject['StructuredFiles']:
            print "====> ** This object is a StructuredFile instance"
        

    

    
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
    taskObject['PreAppCommands'].append(
        setupScramEnvironment(". $VO_CMS_SW_DIR/cmsset_default.sh"))
    taskObject['PreAppCommands'].append(". scramSetup.sh")

    scramSetup.append("#!/bin/bash")
    scramSetup.append(
        scramProjectCommand(taskObject['CMSProjectName'],
                            taskObject['CMSProjectVersion'])
        )
    scramSetup.append(
        scramRuntimeCommand(taskObject['CMSProjectVersion'],"scramv1",True)
        )    

    #scramSetup.append("#!/bin/bash")
    #scramSetup.append("scramv1 project %s %s" % (
    #    taskObject['CMSProjectName'], taskObject['CMSProjectVersion'])
    #                  )
    #scramSetup.append("cd %s" % taskObject['CMSProjectVersion'])
    #scramSetup.append("eval `scramv1 runtime -sh | grep -v SCRAMRT_LSB_JOBNAME\n`")
    #scramSetup.append("cd ..")
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

def handleStageOut(taskObject):
    """
    _handleStageOut_

    Handle a StageOut type task object.

    """
    template = taskObject['StageOutTemplates'][0]
    template['TargetHostName'] = "castorgrid.cern.ch"
    template['TargetPathName'] = "/castor/cern.ch/cms/grid/PATest"
    template['TransportMethod'] = "lcg"

    return
    
def distributor(taskObject):
    """
    _distributor_

    Function that distributes the taskObject to the appropriate handler
    based on the taskObjects Type provided from the WorkflowSpec

    """
    typeVal = taskObject['Type']
    if typeVal == "CMSSW":
        handleCMSSWTaskObject(taskObject)
        return
    elif typeVal == "Script":
        handleScriptTaskObject(taskObject)
        return
    elif typeVal == "StageOut":
        handleStageOut(taskObject)
    else:
        return
 
def installMonitor(taskObject):
    """
    _installMonitor_
                                                                                                                   
    Installs shreek monitoring plugins
                                                                                                                   
    """
    shreekConfig = taskObject['ShREEKConfig']
#    shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobMonMonitor")
#    shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobTimeout")
    shreekConfig.addPluginModule("ShREEK.CMSPlugins.BOSSMonitor")
    boss = shreekConfig.newMonitorCfg()  # new monitor config object
    boss.setMonitorName("boss-1") # name of this instance (make it up)
    boss.setMonitorType("boss")   # type of this instance (as registered in BOSSMonitor.py)
    shreekConfig.addMonitorCfg(boss) # add to the shreek config
    #shreekConfig.addPluginModule("ShREEK.CMSPlugins.CMSMetrics")  # module containing example updator plugin
    #shreekConfig.addUpdator("ProcessToBinary")
    #shreekConfig.addUpdator("Example")

    return


class LCGCreator:
    """
    _LCGCreator_

    Test job creator implementation for prodAgent dev node jobs which are
    run on lxplus at cern

    Static class containing tools to create a generic test job from
    a TaskObject tree.
    Since this class is static, care must be taken to avoid leaving
    any state in the class since it may affect the next job created.
    
    
    """



    def __call__(self, taskObject):

        if taskObject.parent == None:
            installMonitor(taskObject) # top object has the ShREEKConfig object in it.

        #taskObject(printTaskObjectDetails)
        taskObject(distributor)



#  //
# // Register an instance of LXB1125Creator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(LCGCreator(), "lcg")


