#!/usr/bin/env python
"""
_RequestFeeder_

Plugin to generate a fixed amount of production jobs from a workflow.

The input to this plugin is a workflow that contains the following
parameters:

- TotalEvents
- EventsPerJob

"""
import os
import logging

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from JobQueue.JobQueueAPI import bulkQueueJobs

from ProdCommon.JobFactory.RequestJobFactory import RequestJobFactory
from ProdAgentCore.Configuration import loadProdAgentConfiguration

def getGlobalDBSURL():
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg
                                                                                                                                 
    try:
        dbsConfig = config.getConfig("GlobalDBSDLS")
    except StandardError, ex:
        msg = "Error reading configuration for GlobalDBSDLS:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg
                                                                                                                                 
    return dbsConfig.get("DBSURL", None)

class RequestFeeder(PluginInterface):
    """
    _RequestFeeder_

    Generate a pile of production style jobs based on the workflow
    provided

    """
    def handleInput(self, payload):
        logging.info("RequestFeeder: Handling %s" % payload)

        self.workflow = None
        self.totalEvents = None
        self.eventsPerJob = None
        self.initialRun = None
        self.initialEvent = None
        self.overrideInitialEvent = None
        self.sites = []
        self.loadPayload(payload)
        self.publishWorkflow(payload, self.workflow.workflowName())
        self.publishNewDataset(payload) 

        factory = RequestJobFactory(
            self.workflow,
            self.workingDir,
            self.totalEvents,
            InitialRun = self.initialRun,
            InitialEvent = self.initialEvent,
            OverrideFirstEvent = self.overrideInitialEvent,
            EventsPerJob = self.eventsPerJob,
            Sites = self.sites)
        jobsList = factory()

        bulkQueueJobs(self.sites, *jobsList)
        return        
        
                                    

    def loadPayload(self, payload):
        """
        _loadPayload_

        Load the workflow spec and ensure it has the TotalEvents and EventsPerJob settings
        in the parameters

        """

        if not os.path.exists(payload):
            raise RuntimeError, "Payload not found: %s" % payload
        

        logging.info("RequestFeeder: Loading Workflow: %s\n" % payload)
        self.workflow = self.loadWorkflow(payload)
        self.totalEvents = self.workflow.parameters.get("TotalEvents", None)
        self.eventsPerJob = self.workflow.parameters.get('EventsPerJob', None)
        self.initialRun = self.workflow.parameters.get("InitialRun", 1)
        self.initialEvent = self.workflow.parameters.get("InitialEvent", 1)
        self.overrideInitialEvent = \
                 self.workflow.parameters.get("OverrideInitialEvent", None)

        #  //
        # // OnlySites takes precedence over Sites parameter!
        #//

        siteList = self.workflow.parameters.get("Sites", None)
        onlySitesList = self.workflow.parameters.get("OnlySites", None)
        if (siteList is not None) and (onlySitesList is not None):
            logging.info("Both restrictions, OnlySites and Sites were \
                provided --- OnlySites overrides Sites ---")
        if onlySitesList is not None:
            self.sites.extend([x for x in onlySitesList.split(",") if x != ""])
        elif siteList is not None:
            self.sites.extend([x for x in siteList.split(",") if x != ""])
            
        msg = "Total Events: %s  \n" %  self.totalEvents
        msg += "EventsPerJob: %s  InitialRun: %s  \n" % (
             self.eventsPerJob, self.initialRun)
        msg += " InitialEvent %s OverrideInitial Event %s\n" % (
             self.initialEvent, self.overrideInitialEvent)
        
        logging.info(msg)
        
        if self.totalEvents == None:
            msg = "TotalEvents Parameter not provided in workflow:\n%s" % payload
            raise RuntimeError, msg
        if self.eventsPerJob == None:
            msg = "EventsPerJob Parameter not provided in workflow:\n%s" % payload
            raise RuntimeError, msg
        
        self.totalEvents = int(self.totalEvents)
        self.eventsPerJob = int(self.eventsPerJob)
        self.initialRun = int(self.initialRun)
        self.initialEvent = int(self.initialEvent)
        

        #  //
        # // in case of PU
        #//
        dbsUrl = self.workflow.parameters.get("DBSURL", None)
        if dbsUrl == None:
            dbsUrl = getGlobalDBSURL()
            self.workflow.parameters['DBSURL'] = dbsUrl


registerPlugin(RequestFeeder, RequestFeeder.__name__)



