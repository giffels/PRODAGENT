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

        Probably dont need to read details from the
        cfg file since this is a single system, probably
        just easier to code them in this module

        """
        pass


    
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

        #  //
        # // Command to define CMS_PATH, make scram available etc
        #//
        swSetupCommand = "source $VO_CMS_SW_DIR/cmsset_default.sh "

        #  //
        # // If you want to set SCRAM_ARCH, or any other env var,
        #//  you can do it like this:
        #taskObject['Environment'].addVariable(
        #    "SCRAM_ARCH",          # variable name
        #    "slc3_ia32_gcc323")    # value

        #  //
        # // We add the setup command to the list of commands to run
        #//  inside the task
        taskObject['PreTaskCommands'].append(
            swSetupCommand
            )
        
        #  //
        # // We build a script to setup scram and abort if it fails
        #//  Adding it to the list of commands called just prior to the
        #  //executable
        # //
        #//
        scramSetup = taskObject.addStructuredFile("scramSetup.sh")
        scramSetup.interpreter = "."
        taskObject['PreAppCommands'].append(
            setupScramEnvironment(
            swSetupCommand
            )
            )
        taskObject['PreAppCommands'].append(". scramSetup.sh")

        #  //
        # // We build the scram setup script itself, which is basically 
        #//  adding shell lines to a list.
        #  //
        # // Since everyone has to do this, we have some standard tools
        #//  to build the various bits.
        scramSetup.append("#!/bin/bash")

        # scram project command and check
        scramSetup.append(
            scramProjectCommand(
            taskObject['CMSProjectName'],
            taskObject['CMSProjectVersion'],
            )
            )
        # scram runtime command and check
        scramSetup.append(
        scramRuntimeCommand(
            taskObject['CMSProjectVersion']
            )
        )

        # all done!
        return

    
    def handleStageOut(self, taskObject):
        """
        _handleStageOut_
        
        Handle a StageOut type task object.
        
        """
        # command that makes CMS_PATH, SITECONF, TFC available
        stageOutSetup = " export SCRAM_ARCH=slc3_ia32_gcc323 ; source $VO_CMS_SW_DIR/cmsset_default.sh "

        # use that command before stage out
        taskObject['PreStageOutCommands'].append(
            stageOutSetup
            )
        
        
        return
    
    
    def handleCleanUp(self, taskObject):
        """
        _handleCleanUp_

        Handle a CleanUp task object

        """
        # same setup as stage out for cleanup I think
        stageOutSetup = " export SCRAM_ARCH=slc3_ia32_gcc323 ; source $VO_CMS_SW_DIR/cmsset_default.sh "

        # call it before cleanup is invoked
        taskObject['PreCleanUpCommands'].append(
            stageOutSetup
            )
        return
    
#  //
# // Register an instance of OSGCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(T0LSFCreator, T0LSFCreator.__name__)
