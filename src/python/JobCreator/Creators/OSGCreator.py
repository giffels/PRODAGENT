#!/usr/bin/env python
"""
_OSGCreator_

Creator plugin for generating generic OSG jobs

"""

import os
import logging

from JobCreator.Registry import registerCreator
from JobCreator.Creators.CreatorInterface import CreatorInterface
from JobCreator.JCException import JCException

from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand
from JobCreator.ScramSetupTools import scramRuntimeCommand


class OSGCreator(CreatorInterface):
    """
    _OSGCreator_

    Creator plugin for creating dedicated OSG jobs

    """
    def __init__(self):
        CreatorInterface.__init__(self)


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Validaate/default config for this plugin

        """
        if self.pluginConfig == None:
            #  //
            # // No config => create one and populate with defaults
            #//
            msg = "Creator Plugin Config could not be loaded for:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JCException(msg, ClassInstance = self)

        if not self.pluginConfig.has_key("StageOut"):
            msg = "Creator Plugin Config contains no StageOut Config:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JCException(msg, ClassInstance = self)
            
        if not self.pluginConfig.has_key("SoftwareSetup"):
            swsetup = self.pluginConfig.newBlock("SoftwareSetup")
            swsetup['ScramCommand'] = "scramv1"
            swsetup['ScramArch'] = "slc3_ia32_gcc323"
            swsetup['SetupCommand'] = ". /uscms/prod/sw/cms/setup/bashrc"
            
            
        return


    def processTaskObject(self, taskObject):
        """
        _processTaskObject_

        Process each TaskObject based on type
        """
        typeVal = taskObject['Type']
        if typeVal == "CMSSW":
            self.handleCMSSWTaskObject(taskObject)
            return
        elif typeVal == "Script":
            self.handleScriptTaskObject(taskObject)
            return
        elif typeVal == "StageOut":
            self.handleStageOut(taskObject)
        else:
            return



    def preprocessTree(self, taskObjectTree):
        """
        _preprocessTree_

        Install monitors into top TaskObject

        """
        installMonitor(taskObjectTree)
        return
        

        



    def handleCMSSWTaskObject(self, taskObject):
        """
        _handleCMSSWTaskObject_
        
        Method to customise CMSSW type (Eg cmsRun application) TaskObjects
        
        """
        test = taskObject.has_key("CMSProjectVersion") and \
               taskObject.has_key("CMSProjectName")
        if not test:
            return
        
        taskObject['Environment'].addVariable(
            "SCRAM_ARCH",
            self.pluginConfig['SoftwareSetup']['ScramArch'])
        
        scramSetup = taskObject.addStructuredFile("scramSetup.sh")
        scramSetup.interpreter = "."
        taskObject['PreAppCommands'].append(
            setupScramEnvironment(
            self.pluginConfig['SoftwareSetup']['SetupCommand'])
            )
        taskObject['PreAppCommands'].append(". scramSetup.sh")
        
        scramSetup.append("#!/bin/bash")
        scramSetup.append(
            scramProjectCommand(
            taskObject['CMSProjectName'],
            taskObject['CMSProjectVersion'],
            self.pluginConfig['SoftwareSetup']['ScramCommand']
            )
            )
        scramSetup.append(
        scramRuntimeCommand(
            taskObject['CMSProjectVersion'],
            self.pluginConfig['SoftwareSetup']['ScramCommand']
            )
        )
        
        return

    def handleScriptTaskObject(self, taskObject):
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

    def handleStageOut(self, taskObject):
        """
        _handleStageOut_
        
        Handle a StageOut type task object. For OSG, manipulate the stage out
        settings to do a dCache dccp stage out
        
        """
        template = taskObject['StageOutTemplates'][0]
        template['TargetHostName'] = None
        template['TargetPathName'] = \
                    self.pluginConfig['StageOut']['TargetPathName']
        template['TransportMethod'] = \
                    self.pluginConfig['StageOut']['TransportMethod']
        
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


    

    #timeout = shreekConfig.newMonitorCfg()
    #timeout.setMonitorName("timeout-1")
    #timeout.setMonitorType("timeout")
    # Timeout is number of seconds before job gets whacked
    #timeout.addKeywordArg(Timeout = 5) 
    #shreekConfig.addMonitorCfg(timeout)

    
    return
    
    





#  //
# // Register an instance of OSGCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(OSGCreator, OSGCreator.__name__)


