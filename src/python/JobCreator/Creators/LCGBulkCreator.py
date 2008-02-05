#!/usr/bin/env python
"""
_LCGBulkCreator_

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

class LCGBulkCreator(CreatorInterface):
    """
    _LCGBulkCreator_

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
 

        if not self.pluginConfig.has_key("SoftwareSetup"):
            msg = "Creator Plugin Config contains no SoftwareSetup Config:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JCException(msg, ClassInstance = self)
                                                                                             
        if self.pluginConfig['SoftwareSetup']['SetupCommand'] != "None" :
            self.swSetupCommand = self.pluginConfig['SoftwareSetup']['SetupCommand']
        else:
            self.swSetupCommand = "if (set -u; : $OSG_GRID) 2> /dev/null; then . $OSG_GRID/setup.sh; . $OSG_APP/cmssoft/cms/cmsset_default.sh;  else . $VO_CMS_SW_DIR/cmsset_default.sh; fi"

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
        if typeVal == "CmsGen":
            self.handleCMSSWTaskObject(taskObject)
            return
        elif typeVal == "Script":
            self.handleScriptTaskObject(taskObject)
            return
        elif typeVal == "StageOut":
            self.handleStageOut(taskObject)
        elif typeVal == "CleanUp":
            self.handleCleanUp(taskObject)
        elif typeVal == "LogArchive":
            self.handleLogArchive(taskObject)
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

        taskObject['Environment'].addVariable(
  	    "BUILD_ARCH",
  	    self.pluginConfig['SoftwareSetup']['ScramArch'])


        taskObject['PreTaskCommands'].append(
           setupScramEnvironment(self.swSetupCommand))

        scramSetup = taskObject.addStructuredFile("scramSetup.sh")
        scramSetup.interpreter = "."
        taskObject['PreAppCommands'].append(
          setupScramEnvironment(self.swSetupCommand))
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
        taskObject['Environment'].addVariable(
            "SCRAM_ARCH",
            self.pluginConfig['SoftwareSetup']['ScramArch'])

        taskObject['PreStageOutCommands'].append(
            self.swSetupCommand
            )
    
        return
    
    def handleCleanUp(self, taskObject):
        """
        _handleCleanup_
                                                                                                                          
        Handle a Cleanup type task object. For FNAL, manipulate the stage out
        settings to do a dCache dccp stage out
                                                                                                                          
        """
        taskObject['Environment'].addVariable(
            "SCRAM_ARCH",
            self.pluginConfig['SoftwareSetup']['ScramArch'])

        taskObject['PreCleanUpCommands'].append(
            self.swSetupCommand
            )
                                                                                                                          
        return
                                                                                                                          
    def handleLogArchive(self, taskObject):
        """
        _handleCleanup_

        Handle a Logrch type task object.
                                                                                                                          
        """
        taskObject['Environment'].addVariable(
            "SCRAM_ARCH",
            self.pluginConfig['SoftwareSetup']['ScramArch'])

        taskObject['PreLogArchCommands'].append(
            self.swSetupCommand
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
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.BOSSMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.BulkDashboardMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.BulkEventMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.PerfMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobTimeout")

        
        
        boss = shreekConfig.newMonitorCfg()
        boss.setMonitorName("boss-1") # name of this instance (make it up)
        boss.setMonitorType("boss")   # type of this instance (as registered)
        shreekConfig.addMonitorCfg(boss)
        
        #  //
        # // Perf Monitor
        #//
        perfConfig = self.pluginConfig.get("PerformanceMonitor", {})
        usingPerfMon = perfConfig.get("UsePerformanceMonitor", "False")
        if usingPerfMon.lower() == "true":
            perfMonitor =  shreekConfig.newMonitorCfg()
            perfMonitor.setMonitorName("perfmonitor-1")
            perfMonitor.setMonitorType("perf-monitor")
            shreekConfig.addMonitorCfg(perfMonitor)



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
        # // Dashboard Monitoring
        #//
        dashboardCfg = self.pluginConfig.get('Dashboard', {})
        usingDashboard = dashboardCfg.get("UseDashboard", "False")
        if usingDashboard.lower() == "true":
            dashboard = shreekConfig.newMonitorCfg()
            dashboard.setMonitorName("cmsdashboard-1")
            dashboard.setMonitorType("bulk-dashboard")
            dashboard.addKeywordArg(
                ServerHost = dashboardCfg['DestinationHost'],
                ServerPort = dashboardCfg['DestinationPort'],
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
            
            
        #  //
        # // Run & Event monitoring via MonALISA
        #//
        evLog = self.pluginConfig.get("EventLogger", {})
        evLogDest = self.pluginConfig.get("EventLoggerDestinations", {})
        usingEvLog = evLog.get("UseEventLogger", "False")
        if usingEvLog.lower() == "true":
            evlogger = shreekConfig.newMonitorCfg()
            evlogger.setMonitorName("cmseventlogger-1")
            evlogger.setMonitorType("bulk-event")
            
        
            
            evlogger.addKeywordArg(
                EventFile = "EventLogger.log"
                )
            for dest, port in evLogDest.items():
                evlogger.addNode(IMProvNode("Destination", None,
                                            Host = dest, Port = port))
            shreekConfig.addMonitorCfg(evlogger)


        return
    


#  //
# // Register an instance of LCGBulkCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(LCGBulkCreator, LCGBulkCreator.__name__)
