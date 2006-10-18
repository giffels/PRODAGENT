#!/usr/bin/env python
"""
_JobQueueAPI_

Public API for interacting with the JobQueue

"""
import logging
import JobQueue.JobQueueDB as JobQueueDB

from MCPayloads.JobSpec import JobSpec


def queueJob(jobSpecFile, priorityMap):
    """
    _queueJob_

    Add JobSpec to the JobQueue with a priority looked up from the
    priority map by job Type.
    
    
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
    sites =  spec.siteWhitelist

    priority = priorityMap.get(jobType, 0)
    
    try:
        
        JobQueueDB.insertJobSpec(jobSpecId, jobSpecFile, jobType,
                                 workflow, priority, *sites)
        logging.info("Job %s Queued with priority %s" % (jobSpecId, priority))
    except Exception, ex:
        msg = "Failed to queue JobSpec:\n%s\n" % jobSpecFile
        msg += str(ex)
        logging.error(msg)
    return

    
