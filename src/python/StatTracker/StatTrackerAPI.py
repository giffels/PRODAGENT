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
from StatTracker.StatTrackerDB import jobTypeSuccess
from StatTracker.StatTrackerDB import jobTypeFailures


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


def processingFailureDetails(workflowSpecId, timeInterval = "24:00:00"):
    """
    _processingFailureDetails_

    Get details of failed processing jobs in last time period.
    Default period is 24 h.
    Time format should be a string of the form HH:MM:SS

    """
    return selectFailureDetails(workflowSpecId, timeInterval, "Processing")

def mergeFailureDetails(workflowSpecId, timeInterval = "24:00:00"):
    """
    _mergeFailureDetails_

    Get details of failed merge jobs in last time period

    Default period is 24 h.
    Time format should be a string of the form HH:MM:SS

    """
    return selectFailureDetails(workflowSpecId, timeInterval, "Merge")


def processingSuccessDetails(workflowSpecId, timeInterval = "24:00:00"):
    """
    _processingSuccessDetails_

    Get details of successful processing jobs in last time period.
    Default period is 24 h.
    Time format should be a string of the form HH:MM:SS

    """
    return selectSuccessDetails(workflowSpecId, timeInterval, "Processing")

def mergeSuccessDetails(workflowSpecId, timeInterval = "24:00:00"):
    """
    _mergeSuccessDetails_

    Get details of successful merge jobs in last time period.
    Default period is 24 h.
    Time format should be a string of the form HH:MM:SS

    """
    return selectSuccessDetails(workflowSpecId, timeInterval, "Merge")



    


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
        
    
    
def jobTypeCounts(workflowSpec):
    """
    _jobTypeCounts_

    For the workflow spec provided, get a count of each job type
    that failed and succeeded.

    Returns a dictionary containing <type>_success : count and
    <type>_failure : count for each type encountered

    """
    result = {}
    failures = jobTypeFailures(workflowSpec)
    success = jobTypeSuccess(workflowSpec)

    allTypes = failures.keys()
    for typeVal in success.keys():
        if typeVal not in allTypes:
            allTypes.append(typeVal)
    
    for key in allTypes:
        result["%s_failure" % key] = failures.get(key, 0)
        result["%s_success" % key] = success.get(key, 0)

    return result
        

def workflowSummary(workflowSpec, interval = "72:00:00"):
    """
    _workflowSummary_

    Generate a general summary of all jobs for the workflow provided
    including type counts, success and failure counts by type etc
    
    """
    totalSuccess =  successfulJobCount(workflowSpec)
    totalFailed = failedJobCount(workflowSpec)
    typeCounts =  jobTypeCounts(workflowSpec)

    totalAttempts = totalSuccess + totalFailed
    totalMerges = typeCounts.get('Merge_success', 0) + \
                  typeCounts.get('Merge_failure', 0)
    totalProc = typeCounts.get('Processing_success', 0) + \
                typeCounts.get('Processing_failure', 0)
    result = {
        "TotalJobs" : int(totalAttempts),
        "TotalSuccess" : int(totalSuccess),
        "TotalFailure" : int(totalFailed),
        "TotalMerge" : totalMerges,
        "TotalProcessing" : totalProc,
        "MergeFailed" : typeCounts.get('Merge_failure', 0),
        "MergeSuccess" : typeCounts.get('Merge_success', 0),
        "ProcessingFailed" : typeCounts.get('Processing_failure', 0),
        "ProcessingSuccess" : typeCounts.get('Processing_success', 0),
        }

    if totalSuccess == 0:
        result['PercentSuccess'] = 0
    else:
        result['PercentSuccess'] = int(totalAttempts/totalSuccess) * 100

    if totalFailed == 0:
        result['PercentFailed'] = 0
    else:
        result['PercentFailed'] = int(totalAttempts/totalFailed) * 100

    percentMerges = int(float(totalMerges)/float(totalAttempts) * 100)
    percentProc = 100 - percentMerges

    result['PercentMerge'] = percentMerges
    result['PrecentProcessing'] = percentProc
    
    #  //
    # // Sites 
    #//
    failed = jobFailureDetails(workflowSpec, interval)
    succeeded = jobSuccessDetails(workflowSpec, interval)
    sites = [ i['site_name'] for i in succeeded ]
    sites.extend( [ i['site_name'] for i in failed ])
    uniqueSites = {}
    for site in sites:
        uniqueSites[site] = 1
    result['Sites'] = uniqueSites.keys()

    #  //
    # // Events
    #//
    proc = processingSuccessDetails(workflowSpec, interval)
    procTotalRead = 0
    procTotalWrite = 0
    for pjob in proc:
        procTotalWrite += pjob['events_written']
        procTotalRead += pjob['events_read']

    merged = mergeSuccessDetails(workflowSpec, interval)

    mergeTotalRead = 0
    mergeTotalWrite = 0
    for mjob in merged:
        mergeTotalRead += mjob['events_read']
        mergeTotalWrite +=  mjob['events_written']

    result['MergeEventsRead'] = int(mergeTotalRead)
    result['MergeEventsWritten'] = int(mergeTotalWrite)
    result['ProcessingEventsRead'] = int(procTotalRead)
    result['ProcessingEventsWritten'] = int(procTotalWrite)

    if procTotalWrite == 0:
        percentMergedEvents = 0
    else:
        percentMergedEvents = int(float(mergeTotalRead)/float(procTotalWrite)*100)
    
    result['PercentMergedEvents'] = percentMergedEvents
    
    return result
    
    


def shortTextWorkflowSummary(workflow, interval = "72:00:00"):
    """
    _shortTextWorkflowSummary_

    Create a summary string with formatting providing a concise short
    formatted string summarising the state of the workflow

    """
    summary = workflowSummary(workflow, interval)

    result = "Success:\t  %s/%s  (%s" % ( 
        summary['TotalSuccess'], summary['TotalJobs'],
        summary['PercentSuccess']
        )
    result += "%)"
    result += "  (Processing: %s/%s, Merges %s/%s)\n"%(
        summary['ProcessingSuccess'], summary['TotalProcessing'],
        summary['MergeSuccess'], summary['TotalMerge']
        
        )

    result += "Failures:\t  %s     (Processing %s, Merges %s)\n" % (
        summary['TotalFailure'],
        summary['ProcessingFailed'],
        summary['MergeFailed'],
        )

    result += "Events:  \t  %s/%s (%s" % (
        summary['MergeEventsRead'],
        summary['ProcessingEventsWritten'],
        summary['PercentMergedEvents'])
    result += "%)  (Merged/Unmerged)\n"
    
    
    return result
