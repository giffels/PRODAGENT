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

def processSites(rowData):
    """
    _processSites_

    Convert Sites list from BLOB into site list

    """
    sites = rowData.get('sites', None)
    if sites == None:
        return []
    sitesstr = sites.tostring()
    sitelist = sitesstr.split(",")
    return sitelist


def retrieveJobs(count = 1, type = None, workflow = None, *sites):
    """
    _retrieveJobs_

    Get a list of matching jobs from the DB tables. Each job
    is returned as a dictionary.

    """
    sqlStr = """SELECT * FROM jq_queue
    """

    if workflow != None:
        sqlStr +=" WHERE workflow_id=\"%s\" " % workflow

    if type != None:
        if workflow != None:
            sqlStr +=  " AND job_type=\"%s\" " % type
        else:
            sqlStr += " WHERE job_type=\"%s\" " % type
            

    if len(sites) > 0:
        if (workflow == None) and (type == None):
            sqlStr += " WHERE "
        else:
            sqlStr += " AND "

        for site in sites:
            sqlStr += "  sites LIKE \"%"
            sqlStr += str(site)
            sqlStr += "%\" "
            if site != sites[-1]:
                sqlStr += " AND "
    sqlStr += " ORDER BY priority DESC, time DESC LIMIT %s;" % count

    
    connection = connect()
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()
    result = []
    for row in rows:
        row['sites'] = processSites(row)
        row['time'] = str(row['time'])
        result.append(dict(row))
    return result

reduceList = lambda x, y : str(x) + ", " + str(y)

def eraseJobs(*jobIndices):
    """
    _eraseJobs_

    Given the job index values, erase those entries from the DB
    
    """
    if len(jobIndices) == 0:
        return
    delStr = """
    delete from jq_queue 
             where job_index IN """ 


    delStr += "( "
    delStr += str(reduce(reduceList, jobIndices))
    delStr += " );"
    
    connection = connect()
    dbCur = connection.cursor()

    try:
        dbCur.execute("BEGIN")
        dbCur.execute(delStr)
        dbCur.execute("COMMIT")
        dbCur.close()
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Failed to delet jobs:\n  %s\n from JobQueue" % str(jobIndices)
        msg += str(ex)
        raise RuntimeError, msg
    return

    

##if __name__ == '__main__':

##    for i in range(0, 50):
##        insertJobSpec("JobSpec-%s" % i,
##                      "/path/to/JobSpec-%s.xml" % i ,
##                      "Processing", "WorkflowX",
##                      i)
##    import time
##    time.sleep(2)
##    for i in range(51, 100):
##        insertJobSpec("JobSpec-%s" % i,
##                      "/path/to/JobSpec-%s.xml" % i ,
##                      "Processing", "WorkflowX",
##                      i - 50)

##    erase = []
##    for job in retrieveJobs(100):
##        print job
##        erase.append(job['job_index'])

##    eraseJobs(*erase)
