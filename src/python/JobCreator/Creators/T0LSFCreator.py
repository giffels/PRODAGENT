#!/usr/bin/env python
"""
_T0LSFCreator_

Tier 0 LSF Creator

"""

import os
import logging

from JobCreator.Registry import registerCreator
from JobCreator.Creators.CreatorInterface import CreatorInterface
from JobCreator.JCException import JCException

from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand
from JobCreator.ScramSetupTools import scramRuntimeCommand

from IMProv.IMProvNode import IMProvNode


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
        elif typeVal == "LogCollect":
            self.handleLogCollect(taskObject)
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
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.BulkDashboardMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.BulkEventMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobTimeout")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.PerfMonitor")

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
        # // JobTimeout Setup
        #//
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

        stageHost = os.getenv("STAGE_HOST")
        if stageHost:
            scramSetup.append("export STAGE_HOST=%s\n" % stageHost )

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

        stageHost = os.getenv("STAGE_HOST")
        if stageHost:
            taskObject['PreStageOutCommands'].append(
                "export STAGE_HOST=%s\n" % stageHost
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

    
    def handleLogCollect(self, taskObject):
        """
        _handleCleanUp_

        Handle a LogCollect task object

        """
        taskObject['PreLogCollectCommands'].append(
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
