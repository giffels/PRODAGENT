#!/usr/bin/env python
"""
_LCGCreator_

Testing Job Creator plugin that generates a job for the prodAgent Dev Node

"""

import os, logging
from JobCreator.Registry import registerCreator

from JobCreator.Creators.CreatorInterface import CreatorInterface
from ProdAgentCore.PluginConfiguration import PluginConfiguration

from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand
from JobCreator.ScramSetupTools import scramRuntimeCommand

from IMProv.IMProvNode import IMProvNode

class LCGCreator(CreatorInterface):
    """
    _LCGCreator_

    Test job creator implementation 

    """

    def __init__(self):
        CreatorInterface.__init__(self)


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Validate/default config for this plugin

        """
        if self.pluginConfig == None:
            #  //
            # // No config 
            #//
            msg = "Creator Plugin Config could not be loaded for:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JCException(msg, ClassInstance = self)
            #self.pluginConfig = PluginConfiguration()
 

        if not self.pluginConfig.has_key("StageOut"):
            ## add a new stageout block:
            # stageOut = self.pluginConfig.newBlock("StageOut")
            ## or complain:
            msg = "Creator Plugin Config contains no StageOut Config:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JCException(msg, ClassInstance = self)
        else:
            #  //
            # // StageOut defaults
            #//
            if self.pluginConfig['StageOut']['TargetHostName']=='None':
               self.pluginConfig['StageOut']['TargetHostName']="castorgrid.cern.ch"
            if self.pluginConfig['StageOut']['TargetPathName']=='None':
               self.pluginConfig['StageOut']['TargetPathName'] = "/castor/cern.ch/cms/grid/PATest"
            if self.pluginConfig['StageOut']['TransportMethod']=='None':
               self.pluginConfig['StageOut']['TransportMethod']="lcg"

	if not self.pluginConfig.has_key("SoftwareSetup"):
            swsetup = self.pluginConfig.newBlock("SoftwareSetup")
            swsetup['ScramCommand'] = "scramv1"
            swsetup['ScramArch'] = "slc3_ia32_gcc323"
        #    swsetup['SetupCommand'] = ". /uscms/prod/sw/cms/setup/bashrc"

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
        elif typeVal == "CleanUp":
            self.handleCleanUp(taskObject)
        else:
            return



    def preprocessTree(self, taskObjectTree):
        """
        _preprocssTree_

        Install monitors into top TaskObject

        """
        self.installMonitor(taskObjectTree)
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

        taskObject['PreTaskCommands'].append(
           setupScramEnvironment(". $VO_CMS_SW_DIR/cmsset_default.sh"))

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
        
        Handle a StageOut type task object. For FNAL, manipulate the stage out
        settings to do a dCache dccp stage out
        
        """
        taskObject['PreStageOutCommands'].append(
            ". $VO_CMS_SW_DIR/cmsset_default.sh"
            )
    
        return
    
    def handleCleanUp(self, taskObject):
        """
        _handleCleanup_

        Handle a Cleanup type task object. For FNAL, manipulate the stage out
        settings to do a dCache dccp stage out

        """
        taskObject['PreCleanUpCommands'].append(
            ". $VO_CMS_SW_DIR/cmsset_default.sh"
            )

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
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.DashboardMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.BOSSMonitor")

        #  //
        # // (Optional) JobTimeout
        #//
        timeoutCfg = self.pluginConfig.get('JobTimeout', {})
  	usingJobTimeout = timeoutCfg.get("UseJobTimeout", "False")
  	if usingJobTimeout.lower() == "true":
           shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobTimeout")
  	   jobtimeout= shreekConfig.newMonitorCfg()
  	   jobtimeout.setMonitorName("bulktimeout-1")
  	   jobtimeout.setMonitorType("timeout")
  	   jobtimeout.addKeywordArg(
  	      Timeout = timeoutCfg['Timeout'],
  	      HardKillDelay = timeoutCfg['HardKillDelay'])
  	   shreekConfig.addMonitorCfg(jobtimeout)

        #  //
        # // BOSS monitoring
        #//
        boss = shreekConfig.newMonitorCfg()
        boss.setMonitorName("boss-1") # name of this instance (make it up)
        boss.setMonitorType("boss")   # type of this instance (as registered)
        shreekConfig.addMonitorCfg(boss)

        #  //
        # // Dashboard Monitoring
        #//
        dashboardCfg = self.pluginConfig.get('Dashboard', {})
        usingDashboard = dashboardCfg.get("UseDashboard", "False")
        if usingDashboard.lower() == "true":
            dashboard = shreekConfig.newMonitorCfg()
            dashboard.setMonitorName("cmsdashboard-1")
            dashboard.setMonitorType("dashboard")
            dashboard.addKeywordArg(
                ServerHost = dashboardCfg['DestinationHost'],
                ServerPort = dashboardCfg['DestinationPort'],
                ProdAgentJobID = taskObject['JobName'],
                DashboardInfo = taskObject['DashboardInfoLocation'])

            
            #  //
            # // Use realtime event monitoring?
            #//
            evHost = dashboardCfg.get("EventDestinationHost", None)
            evPort = dashboardCfg.get("EventDestinationPort", None)
            if evPort and evHost:
                dashboard.addNode(IMProvNode("EventDestination", None,
                                             Host = evHost, Port = evPort))


            shreekConfig.addMonitorCfg(dashboard)
            


        return
    


#  //
# // Register an instance of LCGCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
#registerCreator(LCGCreator, "lcg")
registerCreator(LCGCreator, LCGCreator.__name__)
