#!/usr/bin/env python
"""
_StatTrackerAPI_

Basic API for querying status of jobs in the StatTracker Database tables

"""

from StatTracker.StatTrackerDB import selectSuccessCount, selectFailureCount
from StatTracker.StatTrackerDB import selectFailureDetails
from StatTracker.StatTrackerDB import selectSuccessDetails

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
    

def jobSuccessDetails(workflowSpecId):
    """
    _jobSuccessDetails_

    Retrieve job success details for all successful jobs in the
    workflowSpecId provided

    """


def jobFailureDetails(workflowSpecId):
    """
    _jobFailureDetails_

    Retrieve job failure details for all successful jobs in the
    workflowSpecId provided

    """
    
    return selectFailureDetails(workflowSpecId)
