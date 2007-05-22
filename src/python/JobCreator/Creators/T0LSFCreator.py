#!/usr/bin/env python
"""
_T0LSFCreator_

Tier 0 LSF Creator

"""

from JobCreator.Registry import registerCreator
from JobCreator.Creators.CreatorInterface import CreatorInterface
from JobCreator.JCException import JCException

from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand
from JobCreator.ScramSetupTools import scramRuntimeCommand

class T0LSFCreator(CreatorInterface):
    """
    _T0LSFCreator

    Process a TaskObject Tree and add in setup details appropriate to
    the Tier 0 environment

    """
    def __init__(self):
        CreatorInterface.__init__(self)


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Validate/default config for this plugin

        Probably dont need to read details from the
        cfg file since this is a single system, probably
        just easier to code them in this module

        """

        if self.pluginConfig == None:
            msg = "Creator Plugin Config could not be loaded for:\n"
            msg += self.__class__.__name__
            raise JCException(msg, ClassInstance = self)
            
	if not self.pluginConfig.has_key("SoftwareSetup"):
            swsetup = self.pluginConfig.newBlock("SoftwareSetup")
            swsetup['ScramCommand'] = "scramv1"
            swsetup['ScramArch'] = "slc3_ia32_gcc323"

        return

    
    def processTaskObject(self, taskObject):
        """
        _processTaskObject_

        Process each TaskObject based on type

        CMSSW means add a CMSSW setup to it, so get CMS_PATH defined,
              make scram available etc.

        
        StageOut means it is a stage out node, needs to setup castor
                 SITECONF, TFC etc

        CleanUp is for cleaning unmerged files, so will probably mean same
                setup as StageOut is needed
        
        """
        typeVal = taskObject['Type']
        if typeVal == "CMSSW":
            self.handleCMSSWTaskObject(taskObject)
            return
        elif typeVal == "StageOut":
            self.handleStageOut(taskObject)
            return
        elif typeVal == "CleanUp":
            self.handleCleanUp(taskObject)
            return
        else:
            return


    def preprocessTree(self, taskObjectTree):
        """
        _preprocessTree_

        Get the entire tree of task objects, useful for
        installing job wide monitoring etc.

        Skip this for now.

        """
        self.installMonitor(taskObjectTree)
        return


    def installMonitor(self, taskObject):
        """
        _installMonitor_
        
        Installs shreek monitoring plugins
        
        """
        shreekConfig = taskObject['ShREEKConfig']
        
        #  //
        # // Insert list of plugin modules to be used
        #//
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobTimeout")

        #  //
        # // JobTimeout Setup
        #//  Looks in CreatorPluginConfig.xml
        #  //
        # // 	<ConfigBlock Name="JobTimeout">
 	#  //		<Parameter Name="UseJobMon" Value="False"/>
	# //		<Parameter Name="Timeout" Value="360"/>   <<<<< Soft time out (SIGUSR2) in minutes
        #//             <Parameter Name="HardKillDelay" Value="60"/>    <<<<< Hard time out (SIGKILL) in minutes
	#  //	</ConfigBlock>
        # //  After Timeout minutes, the process is sent a SIGUSR2 to try a gentle exit
        #//   After Timeout + HardKillDelay minute, the process is terminated & the job fails with traceback info generated

        timeoutCfg = self.pluginConfig.get('JobTimeout', {})
        usingJobTimeout = timeoutCfg.get("UseJobTimeout", "False")
        if usingJobTimeout.lower() == "true":
            jobtimeout= shreekConfig.newMonitorCfg()
            jobtimeout.setMonitorName("bulktimeout-1")
            jobtimeout.setMonitorType("timeout")
            jobtimeout.addKeywordArg(
                 Timeout = timeoutCfg['Timeout'],
                 HardKillDelay = timeoutCfg['HardKillDelay'])
            shreekConfig.addMonitorCfg(jobtimeout)




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

        taskObject['PreTaskCommands'].append(
           setupScramEnvironment(". $VO_CMS_SW_DIR/cmsset_default.sh ; "))

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
          scramRuntimeCommand(taskObject['CMSProjectVersion'],self.pluginConfig['SoftwareSetup']['ScramCommand'],True)
        )

        return


    def handleStageOut(self, taskObject):
        """
        _handleStageOut_
        
        Handle a StageOut task object.
        
        """
        taskObject['PreStageOutCommands'].append(
            ". $VO_CMS_SW_DIR/cmsset_default.sh"
            )
        
        return
    
    
    def handleCleanUp(self, taskObject):
        """
        _handleCleanUp_

        Handle a CleanUp task object

        """
        taskObject['PreCleanUpCommands'].append(
            ". $VO_CMS_SW_DIR/cmsset_default.sh"
            )
        
        return


#  //
# // Register an instance of OSGCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(T0LSFCreator, T0LSFCreator.__name__)
