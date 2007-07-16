#!/usr/bin/env python
"""
_Utilities_

Utilities for querying the WorkflowEntities tables
for job management purposes

"""

from ProdCommon.Database import Session


import ProdAgent.WorkflowEntities.Workflow as WEWorkflow
import ProdAgent.WorkflowEntities.Job as WEJob



def listWorkflowsByOwner(ownerName):
    """
    _listWorkflowsByOwner_

    return a list of WorkflowSpec IDs based on the component/system that
    owns them

    """
    
    sqlStr = "SELECT id FROM we_Workflow WHERE owner=\"%s\";" % ownerName
    Session.execute(sqlStr)
    rows = Session.fetchall()
    result = [ x[0] for x in rows ]
    return result


def jobsForWorkflow(workflow, status = None):
    """
    _jobsForWorkflow_

    Get a set of jobs for a workflow and
    optional status filter

    """
    jobIDs = WEWorkflow.getJobIDs([workflow])

    if status == None:
        return jobIDs

    jobData = WEJob.get(jobIDs)

    result = [ x['id'] for x in jobData if x['status'] == status ]
    
    return result
    