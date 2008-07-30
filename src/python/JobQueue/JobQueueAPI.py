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

def queueJob(jobSpecFile, priorityMap, jobSpec = None, status = "new"):
    """
    _queueJob_

    Add a JobSpec to the JobQueue with a priority looked up from the
    priority map by job type. This queues a single job and is potentially slow
    for large groups of jobs.
   
    jobSpec is a JobSpec instance. If the jobSpec is available pass the jobSpec
    as well as the jobSpecFile location for the performace.
    jobSpecFile won't be loaded, but needed to update the database

    The status parameter can either be "new" or "held".  Jobs marked as "new"
    will be released as resources become available, while jobs marked as "held"
    will sit in the JobQueue until they are explicitly released.
    """
    if jobSpec != None and jobSpec.__class__ is JobSpec:
        spec = jobSpec
    else:
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
        jobQ.insertJobSpec(jobSpecId, jobSpecFile, jobType, workflow,
                           priority, sitesList, status)
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


def releaseJobs(siteIndex = None, *jobDefs):
    """
    _releaseJobs_

    Flag jobs as released so that they can be removed from the queue

    """
    logging.debug("releasing jobDefs: %s for site %s" % (str(jobDefs),
                                                                  siteIndex))
    indices = [ x['JobIndex'] for x in jobDefs ]
    logging.debug("releasing indices: %s" % indices)
    Session.connect()
    Session.start_transaction()
    
    jobQ = JobQueueDB()
    jobQ.flagAsReleased(siteIndex, *indices)
    
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

def getSiteForReleasedJob(job_spec_id):
    """
    get site index for given job
    """
    Session.connect()
    Session.start_transaction()
    
    jobQ = JobQueueDB()
    result = jobQ.getSiteForReleasedJob(job_spec_id)
    
    Session.commit_all()
    Session.close_all()
    
    return result

def removeHoldByWorkflow(workflowID):
    """
    _removeHoldByWorkflow_

    Change the status of all jobs in the JobQueue with a particular workflow ID
    from "held" to "new" so that they will eventually be released.
    """
    try:
        Session.connect()
        Session.start_transaction()
        jobQ = JobQueueDB()
        jobQ.removeHoldForWorkflow(workflowID)
        Session.commit_all()
        Session.close_all()
    except Exception, ex:
        msg = "Failed to remove jobs for workflow %s." % workflowID
        msg += str(ex)
        logging.error(msg)
        Session.rollback()
        Session.close_all()
    return    
    
def reQueueJob(jobs_spec_id):
    """
    Put job back in queue - generally used after a failure
    """
    Session.connect()
    Session.start_transaction()
    
    jobQ = JobQueueDB()
    result = jobQ.reQueueJob(jobs_spec_id)
    
    Session.commit_all()
    Session.close_all()
    
    return result
