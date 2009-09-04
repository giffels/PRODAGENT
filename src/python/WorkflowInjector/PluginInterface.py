#!/usr/bin/env python
"""
_PluginInterface_

Base class for plugins that defines the basic interface and
provides several helper utils

"""

import os
import logging

from JobQueue.JobQueueAPI import bulkQueueJobs
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import ProdAgent.WorkflowEntities.Workflow as WEWorkflow
import ProdAgent.WorkflowEntities.Job as WEJob


def hashList(listOfSites):
    """
    _hashList_

    Sort and string hash a list of sites so that lists of the
    same sites can be grouped in  dictionary
    
    """
    listOfSites.sort()
    hashVal = "-"
    hashVal = hashVal.join(listOfSites)
    return hashVal

    
    
    
class PluginInterface:
    """
    _PluginInterface_

    Base Plugin for WorkflowInjector

    """
    def __init__(self):
        self.args = {}
        self.workingDir = None
        self.jobsToPublish = {}
        self.siteLists = {}
        self.msRef = None
        
    def queueJob(self, jobSpecId, jobSpecFile, jobType,
                 workflowSpecId, workflowPriority, *listOfSites):
        """
        _queueJob_

        Add job to the job queue.

        Note that this object buffers jobs in memory and publishes
        them to the queue in bulk operations

        """
        if len(listOfSites) == 0:
            siteHash = "NOSITE"
        else:
            siteHash = hashList(list(listOfSites))

        if not self.jobsToPublish.has_key(siteHash):
            self.jobsToPublish[siteHash] = {}
            self.siteLists[siteHash] = list(listOfSites)
            

        jobDef = {
            "JobSpecId" : jobSpecId,
            "JobSpecFile": jobSpecFile,
            "JobType" : jobType,
            "WorkflowSpecId": workflowSpecId,
            "WorkflowPriority" : workflowPriority,
            }
        self.jobsToPublish[siteHash][jobSpecId] = jobDef
        return
    
    

    def loadWorkflow(self, specFile):
        """
        _loadWorkflow_

        Helper method, since every plugin will have to do
        something with a workflow

        """
        spec = WorkflowSpec()
        try:
            spec.load(specFile)
        except Exception, ex:
            msg = "Unable to read workflow spec file:\n%s\n" % specFile
            msg += str(ex)
            raise RuntimeError, msg

        return spec
        
        
    def __call__(self, payload):
        """
        _operator()_

        Called by Component to call overridden methods

        """
        self.workingDir = os.path.join(self.args['ComponentDir'],
                                       self.__class__.__name__)

        #  //
        # // Verifiying the workflow wasn't handled by another plugin.
        #//
        workflowName = self.loadWorkflow(payload).workflowName()
        # Parsing list of used Plugins. Current plugin is taken out.
        pluginCaches = [x for x in os.listdir(self.args['ComponentDir']) if \
                os.path.isdir(os.path.join(self.args['ComponentDir'], x)) and \
                x != self.__class__.__name__]
        for pluginCache in pluginCaches:
            testDir = os.path.join(self.args['ComponentDir'], pluginCache,
                '%s-Cache' % workflowName)
            if os.path.exists(testDir):
                msg = "Workflow %s was already handled by %s plugin." % (
                    payload, pluginCache)
                msg += " Current plugin is %s." % self.__class__.__name__
                msg += " Please change workflow's name"
                msg += " or use the original plugin."
                raise RuntimeError, msg

        self.handleInput(payload)

        for siteHash in self.jobsToPublish.keys():
            siteList = self.siteLists[siteHash]
            if siteHash == "NOSITE":
                siteList = []
            bulkQueueJobs(siteList, *self.jobsToPublish[siteHash].values())
            
            #  //
            # // TODO: Register each workflow/job with WE
            #//  tables
            for job in self.jobsToPublish[siteHash].values():
                WEJob.register(job['WorkflowSpecId'], None, {
                    'id' : job['JobSpecId'], 'owner' : 'WorkflowInjector',
                    'job_type' : "Processing", "max_retries" : 3,
                    "max_racers" : 1,
                    })
            
            
        return

    def publishWorkflow(self, workflowPath, workflowId = None):
        """
        _publishWorkflow_

        Register Workflow Entity & Publish NewWorkflow events for the
        workflow provided

        """
        if workflowId != None:
            msg = "Registering Workflow Entity: %s" % workflowId
            logging.debug(msg)
            WEWorkflow.register(
                workflowId,
                {"owner" : "WorkflowInjector",
                 "workflow_spec_file" : workflowPath,
                 
                 })
            
        msg="Publishing NewWorkflow for: %s" % workflowPath
        logging.debug(msg)

        self.msRef.publish("NewWorkflow", workflowPath)
        # this actually needs to happen later -- after any parents are imported
        # self.msRef.publish("NewDataset", workflowPath)
        self.msRef.commit()
        return


    def publishNewDataset(self, workflowPath):
        """
        _publishNewDataset_
        
        Publish the NewDataset event for the workflow provided -- has to 
        be done after parents are imported, so this is now split off
        from publishWorkflow

        """

        if workflowPath != None:
            msg = "Publishing NewDataset Event for: %s" % workflowPath
            logging.debug(msg)
            self.msRef.publish("NewDataset", workflowPath)
            self.msRef.commit()
        return


    def handleInput(self, inputPayload):
        """
        _handleInput_

        Handle the Input Payload.

        The payload can potentially be anything, but should probably
        be restricted to a workflow with some extra arguments added
        to it

        A set of job specs should be generated and saved and then
        passed to self.queueJob to process the job

        """
        fname = "WorkflowInjector.PluginInterface.handleInput"
        raise NotImplementedError, fname


        

        
        

