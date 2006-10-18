#!/usr/bin/env python
"""
_JobQueueDB_

Database API for JobQueue DB Tables


"""

import logging
import MySQLdb
from ProdAgentDB.Connect import connect


def insertJobSpec(jobSpecId, jobSpecFile, jobType, workflowId,
                  priority, *sites):
    """
    _insertJobSpec_

    Insert the JobSpec provided into the JobQueue

    """
    
    
    sqlStr = """
        INSERT INTO jq_queue( job_spec_id,
                              job_spec_file,
                              job_type,
                              workflow_id, priority, sites)
        VALUES (  "%s", "%s", "%s", "%s", %s, """ % (
        jobSpecId, jobSpecFile, jobType, workflowId, priority
        )

    if len(sites) > 0:
        sitesList = "%s" % sites[0]
        for item in sites[1:]:
            sitesList += ",%s" % item
        sqlStr += " \"%s\" " % sitesList

    else:
        sqlStr += " NULL "

    sqlStr += ");"


    connection = connect()
    dbCur = connection.cursor()
    try:
        dbCur.execute("BEGIN")
        dbCur.execute(sqlStr)
        dbCur.execute("COMMIT")
        dbCur.close()
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Failed to insert into JobQueue %s\n" % ex
        raise RuntimeError, msg

    return


if __name__ == '__main__':
    insertJobSpec("JobSpec-1", "/path/to/JobSpec-1.xml", "Processing",
                  "WorkflowX", 10, "cmssrm.fnal.gov")
    
    insertJobSpec("JobSpec-2", "/path/to/JobSpec-2.xml", "Processing",
                  "WorkflowX", 10, "cmssrm.fnal.gov", "red.unl.edu")
    insertJobSpec("JobSpec-3", "/path/to/JobSpec-3.xml", "Processing",
                  "WorkflowX", 10)
    
    
    
