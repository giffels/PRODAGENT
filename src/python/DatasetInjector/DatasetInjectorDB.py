#!/usr/bin/env python
"""
_DatasetInjectorDB_

DB API for Dataset Injector JobQueue

"""


import logging
import MySQLdb
from ProdAgentDB.Connect import connect
from ProdCommon.DataMgmt.JobSplit.JobSplitter import JobDefinition



def createOwner(ownerName):
    """
    _createOwner_

    Create an entry for the owner of a set of jobs
    in the job queue. The owner name should be a unique
    workflow name.

    The owner index used to insert jobs is returned

    """
    sqlStr = "INSERT INTO di_job_owner(owner_name) VALUES ( \"%s\"" % ownerName
    sqlStr += ");"

    
    connection = connect()
    dbCur = connection.cursor()
    try:
        dbCur.execute("BEGIN")
        dbCur.execute(sqlStr)
        dbCur.execute("COMMIT")
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Failed to create job owner named %s\n" % ownerName
        msg += str(ex)
        raise RuntimeError, msg

    
    dbCur.execute("SELECT LAST_INSERT_ID()")
    ownerIndex = dbCur.fetchone()[0]
    dbCur.close()

    return ownerIndex


def dropOwner(ownerName):
    """
    _dropOwner_

    delete an owner. this will also wipe out any remaining jobs belonging
    to it

    """
    sqlStr = "DELETE FROM di_job_owner WHERE owner_name=\"%s\";" % ownerName
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
        msg = "Failed to create job owner named %s\n" % ownerName
        msg += str(ex)
        raise RuntimeError, msg

    return

reduceList = lambda x, y : str(x) + ", " + str(y)

def directInsertJobs(ownerIndex, *jobDefs):
    """
    _directInsertJobs_

    Format and insert a list of job Defs for the owner index
    provided.
    inserts everything it is given in one query, in general, this
    method should only be used indirectly using the insertJobs method,
    which chops up large lists into multiple inserts to avoid stressing
    the DB Server with many K entries inserted in a single transaction

    """
    sqlStr = """

    INSERT INTO di_job_queue(owner_index,
                             fileblock,
                             se_names,
                             lfns,
                             max_events,
                             skip_events) VALUES  \n"""


    for jdef in jobDefs:
        statement = " \n (%s, \"%s\", " % (ownerIndex, MySQLdb.escape_string(jdef['Fileblock']))
        statement += " \"%s\", " % MySQLdb.escape_string(reduce(reduceList, jdef['SENames']))
        statement += " \"%s\", " % MySQLdb.escape_string(reduce(reduceList, jdef['LFNS']))
        if jdef['MaxEvents'] == None:
            statement += " NULL, "
        else:
            statement += " %s, " % jdef['MaxEvents']
        if jdef['SkipEvents'] == None:
            statement += " NULL),"
        else:
            statement += " %s)," % jdef['SkipEvents']
            
        sqlStr += statement

    sqlStr = sqlStr[0:-1]
    sqlStr += ";"
    
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
        msg = "Failed to insert jobs for owner index %s\n" % ownerIndex
        msg += str(ex)
        raise RuntimeError, msg



def insertJobs(ownerIndex, * jobDefs):
    """
    _insertJobs_
            
    Insert the list of jobs for the owner splitting the list into multiple
    inserts if it is large

    """
    _INSERTLIMIT = 2500

    while len(jobDefs) > 0:
        segment = jobDefs[0:_INSERTLIMIT]
        jobDefs = jobDefs[_INSERTLIMIT:]
        directInsertJobs(ownerIndex, *segment)
    return


def ownerIndex(ownerName):
    """
    _ownerIndex_

    Get the owner index using the owner name

    """
    sqlStr = """select owner_index from di_job_owner
                   where owner_name=\"%s\";""" % ownerName

    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    try:
        ownerIndex = dbCur.fetchone()[0]
    except Exception:
        ownerIndex = None
    dbCur.close()

    return ownerIndex

def countJobs(ownerId):
    """
    _countJobs_

    Get the number of jobs for the ownerId available

    """
    if ownerId == None:
        return 0
    sqlStr = """select COUNT(*) from di_job_queue
                  where owner_index=%s and status=\"new\";""" % ownerId
    
              
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    count = dbCur.fetchone()[0]
    dbCur.close()

    return count


def listKnownFileblocks(ownerId):
    """
    _listKnownFileblocks_

    Get a list of all known fileblocks for the ownerId provided

    """
    sqlStr = """SELECT DISTINCT(fileblock) FROM di_job_queue
                    WHERE owner_index=%s;""" % ownerId
    
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    blocks = dbCur.fetchall()
    dbCur.close()
    result = []
    for block in blocks:
        result.append(str(block[0]))
    return result

def makeJobDef(rawDict):
    """
    _makeJobDef_

    Convert the DB query output dict into a JobDefinition instance

    """
    result = JobDefinition()
    result['SkipEvents'] = rawDict['skip_events']
    result['MaxEvents'] = rawDict['max_events']
    result['Fileblock'] = rawDict['fileblock']


    try:
        
        seNames =  rawDict['se_names'].tostring()
    except Exception, ex:
        seNames = str(rawDict['se_names'])
        
    seNames = seNames.split(',')
    seNames = map(lambda x : x.strip(), seNames)
    result['SENames'] = filter(lambda x : x != "", seNames)
    
    try:
        lfns = rawDict['lfns'].tostring()
    except Exception, ex:
        lfns = str(rawDict['lfns'])
        
    lfns = lfns.split(',')
    lfns = map(lambda x : x.strip(), lfns)
    result['LFNS'] = filter(lambda x : x.strip(), lfns)
    return result

def retrieveJobDefs(ownerId, limit=100):
    """
    _retrieveJobDefs_

    Retrieve the oldest job defs (up to the limit provided) from the
    DB, convert them into JobDefinition objects, remove the entries from
    the DB, and return the list of JobDefs.

    """
    result = []
    sqlStr = """select * from di_job_queue
                  where owner_index=%s
                  and status="new" 
                  order by time limit %s;""" % (
        ownerId, limit)
    #  //
    # // retrieve job defs from DB
    #//
    connection = connect()
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()

    #  //
    # // convert DB data into JobDefinition instances and record
    #//  id values
    entryIndices = []
    for row in rows:
        entryIndices.append(str(row['job_id']))
        result.append(makeJobDef(row))

    #  //
    # // update the entries using the id values
    #//
    if len(entryIndices) > 0:
        delStr = """update di_job_queue SET status="used" 
             where owner_index=%s AND job_id IN """ % ownerId
    
        delStr += "( "
        delStr += str(reduce(reduceList, entryIndices))
        delStr += " );"
        dbCur = connection.cursor()
        
        try:
            dbCur.execute("BEGIN")
            dbCur.execute(delStr)
            dbCur.execute("COMMIT")
            dbCur.close()
        except StandardError, ex:
            dbCur.execute("ROLLBACK")
            dbCur.close()
            msg = "Failed to update jobs for owner index %s\n" % ownerId
            msg += str(ex)
            raise RuntimeError, msg


    return result

    
