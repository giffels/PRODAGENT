#!/usr/bin/env python
"""
_StatTrackerDB_

DB API for inserting JobStatistics instances into the DB tables


"""
import logging
import MySQLdb
from ProdAgentDB.Connect import connect



def insertStats(jobStatistics):
    """
    _insertStats_

    Insert the jobStatistics instance provided based on its class type

    """
    logging.debug("StatTrackerDB.insertStats")
    if jobStatistics.__class__.__name__ == "SuccessfulJob":
        logging.debug("StatTrackerDB.insertStats:SuccessfulJob")
        insertJobSuccess(jobStatistics)
        return
    if jobStatistics.__class__.__name__ == "FailedJob":
        logging.debug("StatTrackerDB.insertStats:FailedJob")
        insertJobFailure(jobStatistics)
        return
    logging.debug("StatTrackerDB.insertStats:WEIRDNESS")
    logging.debug("StatTrackerDB.insertState: %s" % jobStatistics.__class__.__name__)
    
    return



def insertJobSuccess(jobSuccessInstance):
    """
    _insertJobSuccess_

    Insert data from jobSuccessInstance into the DB

    """
    logging.debug("StatTrackerDB.insertJobSuccess")
    sqlStr = """INSERT INTO st_job_success(job_spec_id, 
                           workflow_spec_id,
                           task_name,
                           status,
                           site_name,
                           host_name,
                           se_name,
                           exit_code,
                           events_read,
                           events_written) 
               VALUES ("%s", "%s", "%s",
                       "%s", "%s", "%s", "%s",
                       %s, %s, %s);

    """ % (
        jobSuccessInstance['job_spec_id'],
        jobSuccessInstance['workflow_spec_id'],
        jobSuccessInstance['task_name'],
        jobSuccessInstance['status'],
        jobSuccessInstance['site_name'],
        jobSuccessInstance['host_name'],
        jobSuccessInstance['se_name'],
        jobSuccessInstance['exit_code'],
        jobSuccessInstance['events_read'],
        jobSuccessInstance['events_written'],
        )

    connection = connect()
    dbCur = connection.cursor()
    try:
        dbCur.execute("BEGIN")
        dbCur.execute(sqlStr)
        dbCur.execute("COMMIT")
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Failed to insert JobSucces %s\n" % ex
        raise RuntimeError, msg


    dbCur.execute("SELECT LAST_INSERT_ID()")
    jobIndex = dbCur.fetchone()[0]
    dbCur.close()

    #  //
    # // Now insert lists
    #//
    insertAttrs = """INSERT INTO st_job_attr(job_index, attr_class, attr_value, attr_name)
         VALUES 
    """
    
    for run in jobSuccessInstance['run_numbers']:
        insertAttrs += "(%s, \"run_numbers\", \"%s\", NULL),\n" % (
            jobIndex, run)

    for ofile in jobSuccessInstance['output_files']:
        insertAttrs += "(%s, \"output_files\", \"%s\", NULL),\n" % (
            jobIndex, ofile, )

    for ifile in jobSuccessInstance['input_files']:
        insertAttrs += "(%s, \"input_files\", \"%s\", NULL),\n" % (
            jobIndex, ifile, )

    for dataset in jobSuccessInstance['output_datasets']:
        insertAttrs += "(%s, \"output_datasets\", \"%s\", NULL),\n" % (
             jobIndex, dataset, )

    for timingKey, timingValue in jobSuccessInstance['timing'].items():
        insertAttrs += "(%s, \"timing\", \"%s\", \"%s\"),\n" % (
            jobIndex, timingValue, timingKey,
            )
        
    insertAttrs = insertAttrs.strip()[:-1]
    insertAttrs += ";"
    
    dbCur = connection.cursor()
    try:
        dbCur.execute("BEGIN")
        dbCur.execute(insertAttrs)
        dbCur.execute("COMMIT")
        dbCur.close()
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Failed to insert JobSuccess Attrs %s\n" % ex
        raise RuntimeError, msg
    
    return

def insertJobFailure(jobFailureInstance):
    """
    _insertJobFailure_

    Insert data from jobFailureInstance into the DB

    """
    logging.debug("StatTrackerDB.insertJobFailure")
    sqlStr = """INSERT INTO st_job_failure(job_spec_id, 
                           workflow_spec_id,
                           task_name,
                           status,
                           site_name,
                           host_name,
                           se_name,
                           error_type, 
                           error_code,
                           exit_code,
                           error_desc)

               VALUES ("%s", "%s", "%s",
                       "%s", "%s", "%s", "%s",
                       "%s", %s, %s, "%s");

    """ % (
        jobFailureInstance['job_spec_id'],
        jobFailureInstance['workflow_spec_id'],
        jobFailureInstance['task_name'],
        jobFailureInstance['status'],
        jobFailureInstance['site_name'],
        jobFailureInstance['host_name'],
        jobFailureInstance['se_name'],
        jobFailureInstance['error_type'],
        jobFailureInstance['error_code'],
        jobFailureInstance['exit_code'],
        jobFailureInstance['error_desc'],
        )
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
        msg = "Failed to insert JobFailure %s\n" % ex
        raise RuntimeError, msg
    return



def selectSuccessCount(workflowSpec = None):
    """
    _selectSuccessCount_

    Query DB for number of successful jobs in DB, restricted by workflowSpec
    if specified

    """
    sqlStr = "SELECT COUNT(*) FROM st_job_success"
    if workflowSpec == None:
        sqlStr += ";"
    else:
        sqlStr += " WHERE workflow_spec_id=\"%s\";" % workflowSpec

    
    connection = connect()
    dbCur = connection.cursor()
    
    dbCur.execute(sqlStr)
    count = dbCur.fetchone()[0]
    dbCur.close()
    return count

def selectFailureCount(workflowSpec = None):
    """
    _selectFailureCount_

    Query DB for number of failed jobs in DB, restricted by workflowSpec
    if specified

    """
    sqlStr = "SELECT COUNT(*) FROM st_job_failure"
    if workflowSpec == None:
        sqlStr += ";"
    else:
        sqlStr += " WHERE workflow_spec_id=\"%s\";" % workflowSpec

    
    connection = connect()
    dbCur = connection.cursor()
    
    dbCur.execute(sqlStr)
    count = dbCur.fetchone()[0]
    dbCur.close()
    return count


arrToString = lambda x: x.__setitem__('error_desc', x['error_desc'].tostring())


def selectFailureDetails(workflowSpecId, sinceTime="24:00:00"):
    """
    _selectFailureDetails_

    extract the details of failed jobs for the workflowSpecId provided
    for the time interval provided.

    """
    sqlStr = """SELECT job_index,
                job_spec_id,      
                workflow_spec_id, 
                exit_code ,       
                task_name ,       
                status    ,       
                site_name ,       
                host_name ,       
                se_name   ,       
                error_type,       
                error_code,      
                error_desc FROM st_job_failure WHERE

        workflow_spec_id="%s" AND time > ADDTIME(CURRENT_TIMESTAMP,'-%s'); """ % (workflowSpecId, sinceTime)
    
    connection = connect()
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()
    map(arrToString, rows)
    return rows
    


def selectSuccessDetails(workflowSpecId, sinceTime="24:00:00"):
    """
    _selectSuccessDetails_

    Extract the details of the successful jobs for the
    workflowSpecId provided since the time interval provided

    """
    
    sqlStr = """SELECT job_index,
                job_spec_id,      
                workflow_spec_id, 
                exit_code ,       
                task_name ,       
                status    ,       
                site_name ,       
                host_name ,       
                se_name   ,       
                events_read,       
                events_written FROM st_job_success WHERE      

        workflow_spec_id="%s" AND time > ADDTIME(CURRENT_TIMESTAMP,'-%s');"""  % (workflowSpecId, sinceTime)

    connection = connect()
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()
    return rows


def selectEventsWritten(workflowSpecId):
    """
    _selectEventsWritten_

    Get total events written for request.

    """
    
    sqlStr = """ select SUM(events_read) FROM st_job_success
       WHERE workflow_spec_id="%s"
        """ % workflowSpecId

    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    count = dbCur.fetchone()[0]
    dbCur.close()
    return count

def intersection(list1, list2):
    """fast intersection of two lists"""
    intDict = {}
    list1Dict = {}
    for entry in list1:
        list1Dict[entry] = 1
    for entry in list2:
        if list1Dict.has_key(entry):
            intDict[entry] = 1
    return intDict.keys()


def removeTuple(objectInATuple):
    """
    _removeTuple_

    Given (object,)  return object

    """
    return objectInATuple[0]

def listWorkflowSpecs():
    """
    _listWorkflowSpecs_

    Return a list of all workflow spec ids (request names) in the StatTracker
    
    """
    
    sqlStr1 = """ select DISTINCT workflow_spec_id FROM st_job_success;"""
    sqlStr2 = """ select DISTINCT workflow_spec_id FROM st_job_success;"""

    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr1)
    list1 = dbCur.fetchall()
    dbCur.execute(sqlStr2)
    list2 = dbCur.fetchall()
    dbCur.close()
    return intersection(map(removeTuple, list1), map(removeTuple, list2))
    
removeBlobs = lambda x: x.__setitem__('attr_value', x['attr_value'].tostring())
def getJobAttrs(jobIndex):
    """
    _getJobAttrs_

    Get the contents of the st_job_attr table for the job index provided

    """
    sqlStr = "SELECT * FROM st_job_attr WHERE job_index=%s;" % jobIndex

    connection = connect()
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()
    map(removeBlobs, rows)
    return rows
