#!/usr/bin/env python
"""
LogCollectorDB

DB API for the interface between LogArchiver and db

"""


#import logging
#import MySQLdb
from ProdCommon.Database import Session
#from ProdAgentDB.Config import defaultConfig as dbConfig
import logging

#reduceList = lambda x, y : str( "\'"+ x + "/'" ) + ", " + str( "\'" + y + "\'")
reduceList = lambda x, y : str(x) + ", " + str(y)

def recordLog(workflow, se, log):
    """
    record logs to db
    """
    
    sqlStr = """INSERT INTO log_input (lfn, se_name, workflow) 
    VALUES("%s", "%s", "%s")""" % (log, se, workflow)
    
    Session.execute(sqlStr)
    
    
def getLogsToArchive(age = None, update = True):
    """
    get the list of logs older than age to archive
    
    return format: id, {"workflow" : "se" : (lfns)}

    """
    
    sqlStr = """SELECT id, workflow, se_name, lfn FROM log_input 
                WHERE status = 'new' """
    if age:
        sqlStr += " AND insert_time < ADDTIME(NOW(), SEC_TO_TIME(-'%s'))" % age
        
    sqlStr += " ORDER BY workflow, se_name"
        
    Session.execute(sqlStr)
    temp = Session.fetchall()
    
    result = {}
    logs_to_archive = []
    id = temp[0][0]
    for log_id, wf, se, log in temp:
        result.setdefault(wf, {}).setdefault(se, []).append(log)
        logs_to_archive.append(log_id)
    
    if not result or not update:
        return result

    sqlStr = """UPDATE log_input SET status = 'inprogress'
                WHERE id IN (%s)""" % str(reduce(reduceList, logs_to_archive))
    Session.execute(sqlStr)
    
    return id, result
        
    
def logCollectFailed(errorLimit, logs):
    """
    reset these logs to new and update error count
        -    if over error limit set to error
    """
    
    logs = [ """ '%s' """ % x for x in logs ]
    sqlStr = """UPDATE log_input SET error_count = error_count + 1 
                WHERE lfn IN (%s)""" % (str(reduce(reduceList, logs)))
    logging.info("execute: %s" % sqlStr)
    Session.execute(sqlStr)
    sqlStr = """UPDATE log_input SET status = 'failed' 
                WHERE error_count >= %s AND status = 'inprogress' 
                AND lfn IN (%s)""" % (errorLimit, str(reduce(reduceList, logs)))
    logging.info("execute: %s" % sqlStr)
    Session.execute(sqlStr)
    sqlStr = """UPDATE log_input SET status = 'new' 
                WHERE error_count < %s AND status = 'inprogress' 
                AND lfn IN (%s)""" % (errorLimit, str(reduce(reduceList, logs)))
    logging.info("execute: %s" % sqlStr)
    Session.execute(sqlStr)
    return
    

def getCollectedLogDetails():
    """
    return details of all collected logs
    """
    
    sqlStr = """SELECT workflow, se_name, lfn FROM log_input 
                WHERE status IN ('new', 'inprogress') 
                ORDER BY workflow, se_name"""
        
    Session.execute(sqlStr)
    temp = Session.fetchall()
    
    result = {}
    for wf, se, log in temp:
        result.setdefault(wf, {}).setdefault(se, []).append(log)

    return result


#def getUnCollectedLogDetails():
#    """
#    return details of all non-archived logs
#    """
#    
#    sqlStr = """SELECT workflow, se_name, lfn FROM log_input 
#                WHERE status IN ('done') 
#                ORDER BY workflow, se_name"""
#        
#    Session.execute(sqlStr)
#    temp = Session.fetchall()
#    
#    result = {}
#    for wf, se, log in temp:
#        result.setdefault(wf, {}).setdefault(se, []).append(log)
#
#    return result

# TODO: Does not take account of multiple sites with same se
def getUnCollectedLogDetails():
    """
    return details of all non-archived logs
    """
    
    sqlStr = """SELECT workflow, se_name, lfn FROM log_input
                WHERE status IN ('new')
                GROUP BY workflow, se_name, lfn ORDER BY workflow, se_name"""
        
    Session.execute(sqlStr)
    temp = Session.fetchall()
    
    result = {}
    for wf, site, log in temp:
        result.setdefault(wf, {}).setdefault(site, []).append(log)

    return result