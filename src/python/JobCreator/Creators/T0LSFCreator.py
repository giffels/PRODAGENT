#!/usr/bin/env python
"""
_T0LSFCreator_

Tier 0 LSF Creator

"""

import os

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
           
        if not self.pluginConfig.has_key('StageOut'):
            self.pluginConfig.newBlock('StageOut')

        if not self.pluginConfig['StageOut'].has_key('Command'):
            self.pluginConfig['StageOut']['Command'] = "rfcp"

        if not self.pluginConfig['StageOut'].has_key('LFNPrefix'):
            self.pluginConfig['StageOut']['LFNPrefix'] = "None"

        if not self.pluginConfig['StageOut'].has_key('SEName'):
            self.pluginConfig['StageOut']['SEName'] = "srm.cern.ch"

        if not self.pluginConfig['StageOut'].has_key('Option'):
            self.pluginConfig['StageOut']['Option'] = "None"


	if not self.pluginConfig.has_key('SoftwareSetup'):
            self.pluginConfig.newBlock('SoftwareSetup')

        if not self.pluginConfig['SoftwareSetup'].has_key('ScramCommand'):
            self.pluginConfig['SoftwareSetup']['ScramCommand'] = "scramv1"

        if not self.pluginConfig['SoftwareSetup'].has_key('ScramArch'):
            self.pluginConfig['SoftwareSetup']['ScramArch'] = "slc4_ia32_gcc345"


        if not self.pluginConfig.has_key('OverrideUserSandbox'):
            self.pluginConfig.newBlock('OverrideUserSandbox')


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
        elif typeVal == "CmsGen":
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
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.PerfMonitor")

        #  //
        # // Perf Monitor
        #//
        perfConfig = self.pluginConfig.get("PerformanceMonitor", {})
        usingPerfMon = perfCfg.get("UsePerformanceMonitor", "False")
        if usingPerfMon.lower() == "true":
            perfMonitor =  shreekConfig.newMonitorCfg()
            perfMonitor.setMonitorName("perfmonitor-1")
            perfMonitor.setMonitorType("perf-monitor")
            shreekConfig.addMonitorCfg(perfMonitor)
            

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

        taskObject['Environment'].addVariable(
            "BUILD_ARCH",
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

        if self.pluginConfig['OverrideUserSandbox'].has_key(taskObject['CMSProjectVersion']):

            taskObject.attachFile(self.pluginConfig['OverrideUserSandbox'][taskObject['CMSProjectVersion']])

            runres = taskObject['RunResDB']

            runres.addData("/%s/UserSandbox" % taskObject['Name'],
                           os.path.basename(self.pluginConfig['OverrideUserSandbox'][taskObject['CMSProjectVersion']]))


        return


    def handleStageOut(self, taskObject):
        """
        _handleStageOut_
        
        Handle a StageOut task object.
        
        """
        
        taskObject['PreStageOutCommands'].append(
            ". $VO_CMS_SW_DIR/cmsset_default.sh"
            )

        if ( self.pluginConfig['StageOut']['Command'] != "None" and \
             self.pluginConfig['StageOut']['LFNPrefix'] != "None" and \
             self.pluginConfig['StageOut']['SEName'] != "None" ):

            option = self.pluginConfig['StageOut']['Option']
            if ( option == "None" ):
                option = ""

            runres = taskObject['RunResDB']
          
            overrideBase = "/%s/StageOutParameters/Override" % 'stageOut1'

            runres.addPath(overrideBase)
            runres.addData("/%s/command" % overrideBase, self.pluginConfig['StageOut']['Command'])
            runres.addData("/%s/option" % overrideBase, option)
            runres.addData("/%s/se-name" % overrideBase, self.pluginConfig['StageOut']['SEName'])
            runres.addData("/%s/lfn-prefix" % overrideBase, self.pluginConfig['StageOut']['LFNPrefix'])
        
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
