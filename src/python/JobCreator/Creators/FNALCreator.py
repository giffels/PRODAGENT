#!/usr/bin/env python
"""
_FNALCreator_

Testing Job Creator plugin that generates a generic job that
can be run interactively to test the job creation

"""

import os
from JobCreator.Registry import registerCreator

from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand


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
        setupScramEnvironment(". /uscms/prod/sw/cms/setup/bashrc"))
    taskObject['PreAppCommands'].append(". scramSetup.sh")
    
    scramSetup.append("#!/bin/bash")
    scramSetup.append(
        scramProjectCommand(taskObject['CMSProjectName'],
                            taskObject['CMSProjectVersion'])
        )
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

    Handle a StageOut type task object. For FNAL, manipulate the stage out
    settings to do a dCache dccp stage out

    """
    template = taskObject['StageOutTemplates'][0]
    template['TargetHostName'] = None
    template['TargetPathName'] = "/pnfs/cms/WAX/12/preprod-round1"
    template['TransportMethod'] = "dccp"

    
    
    
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

    #  //
    # // Insert list of plugin modules to be used
    #//
    shreekConfig.addPluginModule("ShREEK.CMSPlugins.DashboardMonitor")
    shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobMonMonitor")
    shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobTimeout")
    shreekConfig.addPluginModule("ShREEK.CMSPlugins.BOSSMonitor")
    shreekConfig.addPluginModule("ShREEK.CMSPlugins.CMSMetrics")

    #  //
    # // Insert list of metrics to be generated
    #//
    shreekConfig.addUpdator("ChildProcesses")
    shreekConfig.addUpdator("ProcessToBinary")
    shreekConfig.addUpdator("Example")

    
    #dashboard = shreekConfig.newMonitorCfg()
    #dashboard.setMonitorName("cmsdashboard-1")
    #dashboard.setMonitorType("dashboard")
    #dashboard.addKeywordArg(RequestName = taskObject['RequestName'],
    #                      JobName = taskObject['JobName'])
    #shreekConfig.addMonitorCfg(dashboard)

    #jobmon = shreekConfig.newMonitorCfg()
    #jobmon.setMonitorName("cmsjobmon-1")
    #jobmon.setMonitorType("jobmon")

    #  //
    # // Include the proxy file in the job so that it can be registered to 
    #//  jobMon
    #proxyFile = "/tmp/x509up_u%s" % os.getuid()
    #taskObject.attachFile(proxyFile)
    #injobProxy = "$PRODAGENT_JOB_DIR/%s/x509_u%s" % (
    #    taskObject['Name'], os.getuid(),
    #    )
    #jobmon.addKeywordArg(
    #    RequestName = taskObject['RequestName'],
    #    JobName = taskObject['JobName'],
    #    CertFile = injobProxy,
    #    KeyFile = injobProxy,
    #    ServerURL = "https://b0ucsd03.fnal.gov:8443/clarens"
    #    )
    #shreekConfig.addMonitorCfg(jobmon)


    boss = shreekConfig.newMonitorCfg()
    boss.setMonitorName("boss-1") # name of this instance (make it up)
    boss.setMonitorType("boss")   # type of this instance (as registered)
    shreekConfig.addMonitorCfg(boss)
    

    #timeout = shreekConfig.newMonitorCfg()
    #timeout.setMonitorName("timeout-1")
    #timeout.setMonitorType("timeout")
    # Timeout is number of seconds before job gets whacked
    #timeout.addKeywordArg(Timeout = 5) 
    #shreekConfig.addMonitorCfg(timeout)

    
    return
    
    


class FNALCreator:
    """
    _FNALCreator_

    FNAL job creator implementation.
    
    """



    def __call__(self, taskObject):
        if taskObject.parent == None:
            installMonitor(taskObject)
        
        taskObject(distributor)



#  //
# // Register an instance of FNALCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(FNALCreator(), "fnal")


