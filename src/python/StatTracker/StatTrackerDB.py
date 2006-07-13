#!/usr/bin/env python
"""
_StatTrackerDB_

DB API for inserting JobStatistics instances into the DB tables


"""
import logging
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
    insertAttrs = """INSERT INTO st_job_attr(job_index, attr_class, attr_value)
         VALUES 
    """
    
    for run in jobSuccessInstance['run_numbers']:
        insertAttrs += "(%s, \"run_numbers\", \"%s\"),\n" % (
            jobIndex, run)

    for ofile in jobSuccessInstance['output_files']:
        insertAttrs += "(%s, \"output_files\", \"%s\"),\n" % (
            jobIndex, ofile, )

    for ifile in jobSuccessInstance['input_files']:
        insertAttrs += "(%s, \"input_files\", \"%s\"),\n" % (
            jobIndex, ifile, )

    for dataset in jobSuccessInstance['output_datasets']:
        insertAttrs += "(%s, \"output_datasets\", \"%s\"),\n" % (
             jobIndex, dataset, )

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
