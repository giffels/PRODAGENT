#!/usr/bin/env python
"""
_OSGCreator_

Creator plugin for generating generic OSG jobs

"""

import os
import logging
import socket

from JobCreator.Registry import registerCreator
from JobCreator.Creators.CreatorInterface import CreatorInterface
from JobCreator.JCException import JCException

from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand
from JobCreator.ScramSetupTools import scramRuntimeCommand

from IMProv.IMProvNode import IMProvNode



validator = lambda x: x in ("", None, "None", "none")

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
            msg = "Creator Plugin Config contains no SoftwareSetup Config:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JCException(msg, ClassInstance = self)

        for key, value in self.pluginConfig['SoftwareSetup'].items():
            if validator(value):
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
        elif typeVal == "SVSuite":
            self.handleSVSuite(taskObject)
        else:
            return



    def preprocessTree(self, taskObjectTree):
        """
        _preprocessTree_

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
        
        
        scramSetup = taskObject.addStructuredFile("scramSetup.sh")
        scramSetup.interpreter = "."
        taskObject['PreAppCommands'].append(
            setupScramEnvironment(
            swSetupCommand
            )
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
        stageOutSetup = self.pluginConfig['StageOut']['SetupCommand']
        parentVersion = None
        if taskObject.parent != None:
            if taskObject.parent.has_key("CMSProjectVersion"):
                parentVersion = taskObject.parent["CMSProjectVersion"]
        if parentVersion != None:
            stageOutSetup = stageOutSetup.replace("$CMSSWVERSION",
                                                  parentVersion)

        logging.debug("StageOut Software Setup Command: %s" % stageOutSetup)
        taskObject['PreStageOutCommands'].append(
            stageOutSetup
            )
        
        
        return
    
    
    def handleCleanUp(self, taskObject):
        """
        _handleCleanUp_

        Handle a CleanUp task object

        """
        
        stageOutSetup = self.pluginConfig['StageOut']['SetupCommand']
        parentVersion = None
        if taskObject.parent != None:
            if taskObject.parent.has_key("CMSProjectVersion"):
                parentVersion = taskObject.parent["CMSProjectVersion"]
        if parentVersion != None:
            stageOutSetup = stageOutSetup.replace("$CMSSWVERSION",
                                                  parentVersion)
            
        logging.debug("CleanUp Software Setup Command: %s" % stageOutSetup)
        taskObject['PreCleanUpCommands'].append(
            stageOutSetup
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
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.EventMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobMonMonitor")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.JobTimeout")
        shreekConfig.addPluginModule("ShREEK.CMSPlugins.CMSMetrics")
        
        #  //
        # // Insert list of metrics to be generated
        #//
        shreekConfig.addUpdator("ChildProcesses")
        shreekConfig.addUpdator("ProcessToBinary")

    
        
        #  //
        # // If the Config file says to use JobMon, then we add it
        #//
        jobMonCfg = self.pluginConfig.get("JobMon", {})
        usingJobMon = jobMonCfg.get("UseJobMon", "False")
        if usingJobMon.lower() == "true":
            jobmon = shreekConfig.newMonitorCfg()
            jobmon.setMonitorName("cmsjobmon-1")
            jobmon.setMonitorType("jobmon")
            #  //
            # // Include the proxy file in the job so that it can be registered to 
            #//  jobMon
            proxyFile = "/tmp/x509up_u%s" % os.getuid()
            taskObject.attachFile(proxyFile)
            injobProxy = "$PRODAGENT_JOB_DIR/%s/x509_u%s" % (
                taskObject['Name'], os.getuid(),
                )
            jobmon.addKeywordArg(
                RequestName = taskObject['RequestName'],
                JobName = taskObject['JobName'],
                CertFile = injobProxy,
                KeyFile = injobProxy,
                ServerURL = self.pluginConfig['JobMon']['ServerURL']
                )
            shreekConfig.addMonitorCfg(jobmon)
        
        
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
                DashboardInfo = taskObject['DashboardInfoLocation'])
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
            evlogger.setMonitorType("event")

            prodAgentName = self.prodAgentConfig['ProdAgent']['ProdAgentName']
            hostname = socket.gethostname()
            if hostname not in prodAgentName:
                prodAgentName = "%s@%s" % (prodAgentName, socket.gethostname())

            
            evlogger.addKeywordArg(
                ProdAgentID = prodAgentName,
                ProdAgentJobID = taskObject['JobName'],
                EventFile = "EventLogger.log"
                )
            for dest, port in evLogDest.items():
                evlogger.addNode(IMProvNode("Destination", None,
                                            Host = dest, Port = port))
            shreekConfig.addMonitorCfg(evlogger)
        return
    
    





#  //
# // Register an instance of OSGCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(OSGCreator, OSGCreator.__name__)


