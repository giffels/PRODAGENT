#!/usr/bin/env python
"""
_CondorTrackerDB_

Database API for CondorTracker backend DB

"""
import logging
import MySQLdb
from ProdAgentDB.Connect import connect



def findJobIndex(jobSpecId, connection = None):
    """
    _findJobIndex_

    Search for a job index for the given job spec ID.
    Return it if present, or None if not

    """
    sqlStr = """SELECT job_index FROM ct_job
                 WHERE job_spec_id = "%s"; 
        
             """ % jobSpecId

    if connection == None:
        connection = connect()

    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    try:
        jobIndex = dbCur.fetchone()[0]
    except Exception:
        jobIndex = None

        
    dbCur.close()
    return jobIndex



def getJobIndex(jobSpecId, connection = None):
    """
    _getJobIndex_

    get the job_index for the jobSpecId provided.
    If not known in the table, create a new entry for it
    
    """
    
    if connection == None:
        connection = connect()
    index = findJobIndex(jobSpecId, connection)
    if index != None:
        return index
    
    sqlStr = """ INSERT INTO ct_job(job_spec_id)
                   VALUES ("%s"); """ % jobSpecId

    dbCur = connection.cursor()
    try:
        dbCur.execute("BEGIN")
        dbCur.execute(sqlStr)
        dbCur.execute("COMMIT")
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Error Creating Entry for JobSpec: %s \n" % jobSpecId
        msg += "In table ct_job\n"
        msg += str(ex)
        raise RuntimeError, msg


    dbCur.execute("SELECT LAST_INSERT_ID()")
    jobIndex = dbCur.fetchone()[0]
    dbCur.close()
    return jobIndex




def addJobAttrs(jobIndex, connection = None, **attrs):
    """
    _addJobAttrs_

    Add a dictionary of attributes to the job with the job_index value
    provided

    """

    if connection == None:
        connection = connect()

    sqlStr = """ INSERT INTO ct_job_attr(job_index, attr_name, attr_value)
        VALUES """

    keylist = attrs.keys()
    for key in keylist:
        sqlStr += " (%s, \"%s\", \"%s\" )" % (jobIndex, key, attrs[key])
        if key == keylist[-1]:
            sqlStr += ";\n"
        else:
            sqlStr += ",\n"

    
        

    dbCur = connection.cursor()
    try:
        dbCur.execute("BEGIN")
        dbCur.execute(sqlStr)
        dbCur.execute("COMMIT")
        dbCur.close()
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Error Creating Attribute for Job Index: %s \n" % jobIndex
        msg += "In table ct_job_attr\n"
        msg += str(ex)
        raise RuntimeError, msg
    return

def changeJobState(jobIndex, newstate, connection = None):
    """
    _changeJobState_

    Change Job State Value

    """
    if connection == None:
        connection = connect()
    
    sqlStr = """UPDATE ct_job SET job_state=\"%s\"
                 WHERE job_index=%s;""" % (newstate, jobIndex)
    
    dbCur = connection.cursor()
    try:
        dbCur.execute("BEGIN")
        dbCur.execute(sqlStr)
        dbCur.execute("COMMIT")
        dbCur.close()
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Error Setting Status to %s for Job: %s \n" % (
            newstate, jobSpecId)
        msg += "In table ct_job\n"
        msg += str(ex)
        raise RuntimeError, msg
    return


def addJobAttributes(jobSpecId, **attrs):
    """
    _addJobAttributes_

    Add attributes to a JobSpec ID

    """
    connection = connect()
    index = findJobIndex(jobSpecId, connection)
    addJobAttrs(index, connection, **attrs)
    return


def getJob(jobSpecId, withAttrs = False):
    """
    _getJob_

    Get the details for the jobSpecId provided 
    """
    connection = connect()
    index = findJobIndex(jobSpecId, connection)
    if index == None:
        return None
    
    sqlStr = """ SELECT * FROM ct_job WHERE job_index=%s; """ % index
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    dbCur.execute(sqlStr)
    result = dbCur.fetchone()
    dbCur.close()

    if withAttrs:
        sqlStr = """SELECT attr_name, attr_value FROM ct_job_attr WHERE job_index=%s; """ % index
        dbCur = connection.cursor()
        dbCur.execute(sqlStr)
        attrs = dbCur.fetchall()
        dbCur.close()
        attrResult = {}
        for attr in attrs:
            attrName = str(attr[0])
            if not attrResult.has_key(attrName):
                attrResult[attrName] = []
                
            attrVal = attr[1].tostring()
            attrResult[attrName].append(attrVal)
        result['job_attrs'] = attrResult
    return result


def getJobsByState(state):
    """
    _getJobsByState_

    Return a dict of job_spec_id : job_index for all jobs in the
    state specified

    """
    connection = connect()

    sqlStr = """SELECT job_spec_id, job_index FROM ct_job
                   WHERE job_state="%s"; """ % state
    
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    pairs = dbCur.fetchall()
    dbCur.close()
    result = {}
    for pair in pairs:
        result[str(pair[0])] = int(pair[1])
    return result


    
    
    
def submitJob(jobSpecId, **attrs):
    """
    _submitJob_

    Register a job as submitted, attrs can be batch job IDs etc
    
    """
    connection = connect()
    jobIndex = getJobIndex(jobSpecId, connection)
    if attrs != {}:
        addJobAttrs(jobIndex, connection, **attrs)

    changeJobState(jobIndex, "submitted", connection)
    return

    
    
    

def killJob(jobSpecId):
    """
    _killJob_

    Flag a job to be killed

    """
    connection = connect()
    jobIndex = getJobIndex(jobSpecId, connection)
    changeJobState(jobIndex, "killed", connection)
    return 

    

def jobRunning(jobSpecId, **attrs):
    """
    _jobRunning_

    flag a job as running

    """
    connection = connect()
    jobIndex = getJobIndex(jobSpecId, connection)
    if attrs != {}:
        addJobAttrs(jobIndex, connection, **attrs)
    changeJobState(jobIndex, "running", connection)
    return


def jobComplete(jobSpecId, **attrs):
    """
    _jobComplete_

    Flag a job as complete

    """
    connection = connect()
    jobIndex = getJobIndex(jobSpecId, connection)
    if attrs != {}:
        addJobAttrs(jobIndex, connection, **attrs)
    changeJobState(jobIndex, "complete", connection)
    return
    

def removeJob(jobSpecId):
    """
    _removeJob_

    Remove all entries related to the job

    """
    sqlStr = """ DELETE FROM ct_job WHERE job_spec_id="%s" """ % jobSpecId
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
        msg = "Error Removing entry for Job: %s \n" % jobSpecId
        msg += "In table ct_job\n"
        msg += str(ex)
        raise RuntimeError, msg
    return





if __name__ == '__main__':

    print getJobIndex("JobSpecX")
    print getJobIndex("JobSpecY")
    print getJobIndex("JobSpecZ")
    
    print findJobIndex("JobSpecY")
    print findJobIndex("JobSpecZ")
    print findJobIndex("JobSpecY")
    print findJobIndex("JobSpecX")
    
    
    print getJob("JobSpecX")
    print getJob("JobSpecY")
    print getJob("JobSpecZ")

    
    submitJob("JobSpecX", BatchID = 401)
    submitJob("JobSpecY", BatchID = 402)
    submitJob("JobSpecZ", BatchID = 403)

    print getJob("JobSpecX", True)
    print getJob("JobSpecY", True)
    print getJob("JobSpecZ", True)

    
    jobRunning("JobSpecX")
    jobRunning("JobSpecY")
    
    print getJobsByState("submitted")
    print getJobsByState("running")
    
    jobComplete("JobSpecX")
    jobComplete("JobSpecY")
    killJob("JobSpecZ")

    print getJobsByState("submitted")
    print getJobsByState("running")
    print getJobsByState("complete")
    print getJobsByState("killed")

    removeJob("JobSpecX")
    removeJob("JobSpecY")
    removeJob("JobSpecZ")
    
