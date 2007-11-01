#!/usr/bin/env python
"""
_PluginInterface_

Base class for plugins that defines the basic interface and
provides several helper utils

"""

import os

from JobQueue.JobQueueAPI import bulkQueueJobs
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

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

        self.handleInput(payload)

        for siteHash in self.jobsToPublish.keys():
            siteList = self.siteLists[siteHash]
            if siteHash == "NOSITE":
                siteList = []
            bulkQueueJobs(siteList, *self.jobsToPublish[siteHash].values())
            
            #  //
            # // TODO: Register each workflow/job with WE
            #//  tables
            
        return

    def publishWorkflow(self, workflowPath):
        """
        _publishWorkflow_

        Publish NewWorkflow and NewDataset events for the
        workflow provided

        """
        self.msRef.publish("NewWorkflow", workflowPath)
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


        

        
        

