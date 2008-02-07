#!/usr/bin/env python
"""
_JobQueueAPI_

Public API for interacting with the JobQueue

"""
import logging
from  JobQueue.JobQueueDB import JobQueueDB

from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session


Session.set_database(dbConfig)

def queueJob(jobSpecFile, priorityMap):
    """
    _queueJob_

    Add JobSpec to the JobQueue with a priority looked up from the
    priority map by job Type.
    This queues a single job and is potentially slow for large groups of
    jobs.
    
    """
    spec = JobSpec()
    try:
        spec.load(jobSpecFile)
    except Exception, ex:
        msg = "Unable to read JobSpec File:\n"
        msg += "%s\n" % jobSpecFile
        msg += "Error: %s\n" % str(ex)
        logging.error(msg)
        return
    
    
    workflow = spec.payload.workflow
    jobSpecId = spec.parameters['JobName']
    jobType = spec.parameters['JobType']
    sitesList =  spec.siteWhitelist
    
    priority = priorityMap.get(jobType, 1)
    
    try:
        Session.connect()
        Session.start_transaction()
        jobQ = JobQueueDB()
        #jobQ.loadSiteMatchData()
        jobQ.insertJobSpec(jobSpecId, jobSpecFile, jobType, workflow,
                           priority, sitesList)
        logging.info("Job %s Queued with priority %s" % (jobSpecId, priority))
        Session.commit_all()
        Session.close_all()
    except Exception, ex:
        msg = "Failed to queue JobSpec:\n%s\n" % jobSpecFile
        msg += str(ex)
        logging.error(msg)
        Session.rollback()
        Session.close_all()
    return


def bulkQueueJobs(listOfSites, *jobSpecDicts):
    """
    _bulkQueueJobs_

    For a list of jobs all going to the same site(s) add them
    to the job queue.

    For each job spec a dictionary should be provided containing:

    "JobSpecId"
    "JobSpecFile"
    "JobType"
    "WorkflowSpecId"
    "WorkflowPriority"

    A list of site names or se names should be provided.
    All jobs will be queued for that list of sites

    """
    
    try:
        Session.connect()
        Session.start_transaction()
        jobQ = JobQueueDB()
        #jobQ.loadSiteMatchData()
        jobQ.insertJobSpecsForSites(listOfSites, *jobSpecDicts)
        logging.info("Job List Queued for sites: %s" % listOfSites)
        Session.commit_all()
        Session.close_all()
    except Exception, ex:
        msg = "Failed to queue JobSpecs:\n"
        msg += str(ex)
        logging.error(msg)
        Session.rollback()
        Session.close_all()
    return


def releaseJobs( *jobDefs):
    """
    _releaseJobs_

    Flag jobs as released so that they can be removed from the queue

    """
    logging.debug("releasing jobDefs: %s" % str(jobDefs))
    indices = [ x['JobIndex'] for x in jobDefs ]
    logging.debug("releasing indices: %s" % indices)
    Session.connect()
    Session.start_transaction()
    
    jobQ = JobQueueDB()
    jobQ.flagAsReleased(*indices)
    
    
    Session.commit_all()
    Session.close_all()
    return

def queueLength(jobType = None):
    """
    _queueLength_

    Get the number of jobs in the queue, optionally distinguishing by
    type

    """
    Session.connect()
    Session.start_transaction()
    
    jobQ = JobQueueDB()
    length = jobQ.queueLength(jobType)
    
    Session.commit_all()
    Session.close_all()
    return length
