#!/usr/bin/env python
"""
_StatTrackerAPI_

Basic API for querying status of jobs in the StatTracker Database tables

"""

from StatTracker.StatTrackerDB import selectSuccessCount, selectFailureCount
from StatTracker.StatTrackerDB import selectFailureDetails
from StatTracker.StatTrackerDB import selectSuccessDetails
from StatTracker.StatTrackerDB import selectEventsWritten
from StatTracker.StatTrackerDB import listWorkflowSpecs
from StatTracker.StatTrackerDB import getJobAttrs

def successfulJobCount(workflowSpecId = None):
    """
    _successfulJobCount_

    Get total number of sucessful jobs.

    If workflowSpecId is provided, get number for that workflow spec only
    
    """
    try:
        return selectSuccessCount(workflowSpecId)
    except StandardError, ex:
        msg = "Error querying StatTracker DB Tables:\n"
        msg += str(ex)
        raise RuntimeError, msg

def failedJobCount(workflowSpecId = None):
    """
    _failedJobCount_

    Get total number of failed jobs, if a workflow spec id is specified,
    for that spec.

    """
    try:
        return selectFailureCount(workflowSpecId)
    except StandardError, ex:
        msg = "Error querying StatTracker DB Tables:\n"
        msg += str(ex)
        raise RuntimeError, msg
    

def jobSuccessDetails(workflowSpecId, timeInterval = "24:00:00"):
    """
    _jobSuccessDetails_

    Retrieve job success details for all successful jobs in the
    workflowSpecId provided in some time period.  Default period
    is 24 h.

    Time format should be a string of the form HH:MM:SS

    """
    return selectSuccessDetails(workflowSpecId, timeInterval)

def jobFailureDetails(workflowSpecId, timeInterval = "24:00:00"):
    """
    _jobFailureDetails_

    Retrieve job failure details for all successful jobs in the
    workflowSpecId provided in some time period. Default period
    is 24 h.

    Time format should be a string of the form HH:MM:SS
    
    """
    
    return selectFailureDetails(workflowSpecId, timeInterval)


def totalEventsWritten(workflowSpecId):
    """
    _totalEventsWritten_

    Get the total number of events written for this request:
    Note that this includes merge jobs as well, so it wont be the
    same as total events generated

    """
    return selectEventsWritten(workflowSpecId)


def workflowSpecs():
    """
    _workflowSpecs_

    List of all workflow specs currently known about in the StatTracker

    """
    return listWorkflowSpecs()

def successfulJobProperties(jobIndex):
    """
    _successfulJobProperties_

    Get the properties of the job Index provided.
    This gets all the information from the st_job_attr table for that
    job index.
    The job index is returned in the jobSuccessDetails dictionaries as the
    job_index key

    """
    result = {}
    for item in getJobAttrs(jobIndex):
        attrClass = item['attr_class']
        attrName = item['attr_name']
        attrVal = item['attr_value']
        if attrName == None:
            if not result.has_key(attrClass):
                result[attrClass] = []
            result[attrClass].append(attrVal)
        else:
            if not result.has_key(attrClass):
                result[attrClass] = {}
            result[attrClass][attrName] = attrVal

    return result
        
    
    
