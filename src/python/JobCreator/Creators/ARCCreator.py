#!/usr/bin/env python
"""
_ARCCreator_

ARC Middleware/Nordugrid Creator.

This plugin is used by the job creator component to install standard
environment setup for jobs sent to sites via the ARC Middleware

"""

import socket
import logging

from JobCreator.Registry import registerCreator
from JobCreator.Creators.CreatorInterface import CreatorInterface
from JobCreator.JCException import JCException

from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand
from JobCreator.ScramSetupTools import scramRuntimeCommand

from IMProv.IMProvNode import IMProvNode


class ARCCreator(CreatorInterface):
    """
    _ARCCreator

    Process a TaskObject Tree and add in setup details appropriate to
    the ARC Middleware environment.

    """
    def __init__(self):
        CreatorInterface.__init__(self)


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Validate/default config for this plugin

        This plugin has a Config file associated with it:
        CreatorPluginConfig.xml

        You can use this method to check what is in there

        """
        if self.pluginConfig == None:
            msg = "Creator Plugin Config could not be loaded for:\n"
            msg += self.__class__.__name__
            raise JCException(msg, ClassInstance = self)
            
        if not self.pluginConfig.has_key("StageOut"):
            msg = "Creator Plugin Config contains no StageOut Config:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JCException(msg, ClassInstance = self)

        if not self.pluginConfig.has_key("SoftwareSetup"):
            msg = "Creator Plugin Config contains no SoftwareSetup Config:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JCException(msg, ClassInstance = self)

        for key, value in self.pluginConfig['SoftwareSetup'].items():
            if value in ("", None, "None", "none"):
                msg = "Bad Value for SoftwareSetup parameter: %s\n" % key
                msg += "This must be set to a proper value"
                raise JCException(msg, ClassInstance = self)
            
        return


    def handleSVSuite(self, taskObject):
        """
        _handleSVSuite_

        Install setup commands for SVSuite type task objects

        """
        swSetupCommand = self.pluginConfig['SoftwareSetup']['SetupCommand']
        swSetupCommand = swSetupCommand.replace(
            "$CMSSWVERSION",
            taskObject['CMSProjectVersion'])
        logging.debug("CMSSW Software Setup Command: %s" % swSetupCommand)

        taskObject['Environment'].addVariable(
            "SCRAM_ARCH",
            self.pluginConfig['SoftwareSetup']['ScramArch'])

        taskObject['PreTaskCommands'].append(
            swSetupCommand
            )

        scramSetup = taskObject['scramSetup.sh']
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
        if typeVal == "CmsGen":
            self.handleCMSSWTaskObject(taskObject)
        elif typeVal == "Script":
            self.handleScriptTaskObject(taskObject)
        elif typeVal == "StageOut":
            self.handleStageOut(taskObject)
        elif typeVal == "CleanUp":
            self.handleCleanUp(taskObject)
        elif typeVal == "SVSuite":
            self.handleSVSuite(taskObject)
        elif typeVal == "LogCollect":
            self.handleLogCollect(taskObject)
        return


    def preprocessTree(self, taskObjectTree):
        """
        _preprocessTree_

        Get the entire tree of task objects, useful for
        installing job wide monitoring etc.

        """
        self.installMonitor(taskObjectTree)


    def installMonitor(self, taskObject):
        """
        _installMonitor_

        Installs shreek monitoring plugins

        """
        shreekConfig = taskObject['ShREEKConfig']

        # Insert list of plugin modules to be used
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.BulkDashboardMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.PerfMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobTimeout")

        # Perf Monitor
        perfConfig = self.pluginConfig.get("PerformanceMonitor", {})
        usingPerfMon = perfConfig.get("UsePerformanceMonitor", "False")
        if usingPerfMon.lower() == "true":
            perfMonitor =  shreekConfig.newMonitorCfg()
            perfMonitor.setMonitorName("perfmonitor-1")
            perfMonitor.setMonitorType("perf-monitor")
            shreekConfig.addMonitorCfg(perfMonitor)

        # (Optional) JobTimeout
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

        # Dashboard Monitoring
        dashboardCfg = self.pluginConfig.get('Dashboard', {})
        usingDashboard = dashboardCfg.get("UseDashboard", "False")
        logging.debug("usingDashboard = '%s'" % (usingDashboard))
        if usingDashboard.lower() == "true":
            dashboard = shreekConfig.newMonitorCfg()
            dashboard.setMonitorName("cmsdashboard-1")
            dashboard.setMonitorType("bulk-dashboard")
            dashboard.addKeywordArg(
                     ServerHost = dashboardCfg['DestinationHost'],
                     ServerPort = dashboardCfg['DestinationPort'],
                     DashboardInfo = taskObject['DashboardInfoLocation'])

            # Use realtime event monitoring?
            evHost = dashboardCfg.get("EventDestinationHost", None)
            evPort = dashboardCfg.get("EventDestinationPort", None)
            if evPort and evHost:
                dashboard.addNode(IMProvNode("EventDestination", None,
                                             Host = evHost, Port = evPort))

            shreekConfig.addMonitorCfg(dashboard)



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

        
        # Command that sets up CMS environment, makes scram available etc
        taskObject['PreTaskCommands'].append(
           setupScramEnvironment(self.swSetupCommand))

        # Build a scram setup for the job environment
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
          scramRuntimeCommand(
            taskObject['CMSProjectVersion'],
            self.pluginConfig['SoftwareSetup']['ScramCommand'],
            True)
          )


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
        
        Handle a StageOut task object.
        
        """
        # Assuming stage out tools have same setup as rest of CMS
        # environment
        taskObject['PreStageOutCommands'].append(self.swSetupCommand)
    
    
    def handleCleanUp(self, taskObject):
        """
        _handleCleanUp_

        Handle a CleanUp task object

        """
        # Assuming stage out tools have same setup as rest of CMS
        # environment, this is usually the same as the StageOut
        taskObject['PreCleanUpCommands'].append(
            self.swSetupCommand
            )


    def handleLogCollect(self, taskObject):
        stageOutSetup = self.pluginConfig['StageOut']['SetupCommand']
        parentVersion = None
        if taskObject.parent != None:
            if taskObject.parent.has_key("CMSProjectVersion"):
                parentVersion = taskObject.parent["CMSProjectVersion"]
        if parentVersion != None:
            stageOutSetup = stageOutSetup.replace("$CMSSWVERSION",
                                                  parentVersion)

        logging.debug("LogCollect Software Setup Command: %s" % stageOutSetup)
        taskObject['PreLogCollectCommands'].append(
            stageOutSetup
            )
        return


# Register an instance of ARCCreator with the Creator Registry
# (Add import in Creators/__init__.py of this module to enable auto
# registration based on import of entire module)
registerCreator(ARCCreator, ARCCreator.__name__)
