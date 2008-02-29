#!/usr/bin/env python
"""
_JobEmulatorComponent_

Core of the Job Emulator.  Will accept new jobs from
the message passing system, as well as processing jobs
and generating reports as they finish.

"""

__revision__ = "$Id: $"
__version__ = "$Revision: $"
__author__ = "sfoulkes, sryu"

import os
import logging

from ProdCommon.Database import Session
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
import ProdAgentCore.LoggingUtils as LoggingUtils

from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdAgent.WorkflowEntities import Job as WEJob

from JobEmulator.Registry import retrievePlugin
from JobEmulator.JobEmulatorAPI import addJob
from JobEmulator.JobEmulatorAPI import queryJobsByStatus
import JobEmulator.JobCompletionPlugins
import JobEmulator.JobReportPlugins
import JobEmulator.JobAllocationPlugins

class JobEmulatorComponent:
    """
    _JobEmulatorComponent_

    Core of the Job Emulator.  Will accept new jobs from
    the message passing system, as well as processing jobs
    and generating reports as they finish.

    """
    def __init__(self, **args):
        JobEmulator.JobEmulatorAPI.initializeJobEM_DB()
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.allocationPlugin = None
        self.plugin = None
        self.fwkReportPlugin = None
        self.args.update(args)
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(
                self.args['ComponentDir'],
                "ComponentLog")
        LoggingUtils.installLogHandler(self)
        self.ms = None
        logging.info("JobEmulator Component Started")

        if self.args.get("avgCompletionTime", None) != None:
            self.avgCompletionTime = self.args["avgCompletionTime"]
        else:
            self.avgCompletionTime = "00:01:00"

        if self.args.get("avgCompletionPercentage", None) != None:
            self.avgCompletionPercentage = self.args["avgCompletionPercentage"]
        else:
            self.avgCompletionPercentage = "0.90"        
            
        if self.args.get("JobAllocationPlugin", None) != None:
            self.allocationPlugin = self.args["JobAllocationPlugin"] 
        else:
            logging.info("No job allocation plugin registered.")


        if self.args.get("JobCompletionPlugin", None) != None:
            self.plugin = self.args["JobCompletionPlugin"]
        else:
            logging.info("No job completion plugin registered.")

        if self.args.get("JobReportPlugin", None) != None:
            self.fwkReportPlugin = self.args["JobReportPlugin"]
        else:
            logging.info("No framwork report plugin registered.")
                        
    def __call__(self, event, payload):
        """
        _operator(message, payload)_

        Method called in response to this component recieving a message

        """
        # always have a debug on/off switch
        if event == "JobEmulatorComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
        elif event == "JobEmulatorComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
        elif event == "JobEmulatorComponent:SetJobAllocationPlugin":
            self.allocationPlugin = payload 
        elif event == "JobEmulatorComponent:SetJobCompletionPlugin":
            self.plugin = payload           
        elif event == "JobEmulatorComponent:SetJobReportPlugin":
            self.fwkReportPlugin = payload          
        elif event == "EmulateJob":
            self.emulateJob(payload)
        elif event == "JobEmulatorComponent:Update":
            self.ms.publish("JobEmulatorComponent:Update", "", "00:00:20")
            logging.debug("JobEmulatorComponent:Update")
            self.update()
        
        return
    
    def loadPlugin(self, pluginName):
        """
        _loadPlugin_
        
        Create an instance of the Job Completion plugin that is
        specified in the self.jobCompletionPlugin variable. That
        variable is set at startup when the ProdAgentConfig.xml
        file is parsed, and can also be changed by the
        JobEmulatorComponent:SetJobCompletionPlugin message.  If the
        plugin can't be loaded or the plugin name has not been set
        this returns None.

        """
        if pluginName == None:
            msg = "plugin name is not set!"
            logging.warning(msg)
            return None
                                            
        try:
            pluginInstance = retrievePlugin(pluginName)
            pluginInstance.avgCompletionTime = self.avgCompletionTime
            pluginInstance.avgCompletionPercentage = self.avgCompletionPercentage
            logging.info("Using %s plugin." % pluginName)
        except RuntimeError, ex:
            msg = "Unable to plugin named %s\n" % pluginName
            msg += str(ex)
            logging.error(msg)
            pluginInstance = None

        return pluginInstance
        

    def update(self):
        """
        _update_

        Query the database for all jobs with the status of new and
        run them through the job completion plugin to see if their
        status should change.  If the status changes to finished or
        failed generate the appropriate job report.

        """
        completionPlugin = self.loadPlugin(self.plugin)
        if completionPlugin == None:
            logging.error("Error: no job completion plugin")
            return
        completionPlugin.avgCompletionTime = self.avgCompletionTime
        completionPlugin.avgCompletionPercentage = self.avgCompletionPercentage
        
        reportPlugin = self.loadPlugin(self.fwkReportPlugin)
        
        if reportPlugin == None:
            logging.error("Error: no report plugin")
            return
        
        newJobs = queryJobsByStatus("new")
        
        for newJob in newJobs:
            jobRunningLocation = \
                    JobEmulator.JobEmulatorAPI.getWorkerNodeInfo(newJob[4])
            newJobStatus = completionPlugin.processJob(newJob, jobRunningLocation)

            if newJobStatus == "new":
                continue

            jobSpec = JobSpec()

            jobState = WEJob.get(newJob[0])
            jobSpecPath = "%s/%s-JobSpec.xml" % \
                          (jobState["cache_dir"], newJob[0])
                          
            logging.debug("------ Job Spec Path ----\n%s\n" % jobSpecPath)
            jobSpec.load(jobSpecPath)

            if newJobStatus == "finished":
                logging.debug("---------jobFinished")
                if reportPlugin != None:
                    reportPlugin.createSuccessReport(jobSpec, jobRunningLocation)
            elif newJobStatus == "failed":
                logging.debug("--------jobFailed")
                if self.fwkReportPlugin != None:
                    reportPlugin.createFailureReport(jobSpec, jobRunningLocation)
            
            logging.debug("--------- updating job status %s - %s" % (newJobStatus, newJob[0]))
            JobEmulator.JobEmulatorAPI.updateJobStatus(newJob[0], newJobStatus)
            logging.debug(" *** Job Upadated ***")
            JobEmulator.JobEmulatorAPI.decreaseJobCountAtNode(newJob[0])
    
    def emulateJob(self, payload):
        """
        _emulateJob_

        Load/parse a jobSpecFile and add the job to the database.

        """
        logging.debug("EmulatingJob : %s" % payload)

        jobSpec = JobSpec()

        try:
            jobSpec.load(payload)
        except StandardError, ex:
            logging.error("Error loading JobSpec file: %s" % payload)
            logging.error(str(ex))

        allocationPlugin = self.loadPlugin(self.allocationPlugin)
        if allocationPlugin == None:
            logging.error("Error: no allocation plugin")
            return
        
        addJob(jobSpec.parameters['JobName'], jobSpec.parameters['JobType'])
        jobRunningLocation = allocationPlugin.allocateJob()
            
        # this will increase job count for give node
        JobEmulator.JobEmulatorAPI.assignJobToNode(jobSpec.parameters['JobName'],
                                                   jobRunningLocation['HostID'])
        logging.debug("------host id: %s add job" % jobRunningLocation['HostID'])
        

        return

    def startComponent(self):
        """
        _startComponent_

        Fire up the message service and define the list of
        messages that this component subscribes to

        """
        
        # create message service
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("JobEmulatorComponent")
        # subscribe to messages
        self.ms.subscribeTo("JobEmulatorComponent:Update")
        self.ms.subscribeTo("EmulateJob")
        self.ms.subscribeTo("JobEmualtorComponent:StartDebug")
        self.ms.subscribeTo("JobEmualtorComponent:EndDebug")

        # subscribe to a plugin changes
        self.ms.subscribeTo("JobEmulatorComponent:SetJobAllocationPlugin")
        self.ms.subscribeTo("JobEmulatorComponent:SetJobCompletionPlugin")
        self.ms.subscribeTo("JobEmulatorComponent:SetJobReportPlugin")
        
        self.ms.publish("JobEmulatorComponent:Update", "")
        self.ms.commit()
        
        # wait for messages
        while True:
            try:
                Session.set_database(dbConfig)
                Session.connect()
                Session.start_transaction()
                msgtype, payload = self.ms.get()
                self.ms.commit()
                self.__call__(msgtype, payload)
                Session.commit_all()
                Session.close_all()
            except Exception, ex:
                msg = str(ex)
                logging.error(msg)
                
