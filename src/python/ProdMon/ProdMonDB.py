# !/usr/bin/env python
"""
_ProdMonDB_

DB API for inserting JobStatistics instances into the DB tables
and for recalling and formating as XML

"""

#TODO: stored procedures for frequent operations?

import logging
import MySQLdb
from ProdAgentDB.Connect import connect

# function to escape and quote a value or replace None with "NULL"
addQuotes = lambda x: (x != None) and \
            ("\'" + MySQLdb.escape_string(str(x)) + "\'") or "NULL" 


def insertStats(jobStatistics):
    """
    _insertStats_

    Insert the jobStatistics instance in the database

    """
    try:
        connection = connect()
        dbCur = connection.cursor()
        
        # perform all db actions within transaction
        # technically not everything needs to be in same
        # transaction - but easier
        dbCur.execute("BEGIN")
        
        # insert/link tables
        # Note: Order does matter as these methods
        # rely on id's of foreign keys being in jobStatistics
        __setWorkflowId(dbCur, jobStatistics)
        __insertJob(dbCur, jobStatistics)
        __insertResource(dbCur, jobStatistics)
        __insertError(dbCur, jobStatistics)
        __insertInputFiles(dbCur, jobStatistics)
        __insertOutputFiles(dbCur, jobStatistics)
        __insertJobInstance(dbCur, jobStatistics)
        __linkFilesJobInstance(dbCur, jobStatistics)
        __insertRuns(dbCur, jobStatistics)
        __insertSkippedEvents(dbCur, jobStatistics)
        __insertTimings(dbCur, jobStatistics)
        
        dbCur.execute("COMMIT")
        dbCur.close()
    except StandardError, ex:
        # Close DB connection and re-throw
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Failed to insert JobStatistics:\n\t %s\n" % str(ex)
        raise RuntimeError, msg    
        
    logging.debug("Job successfully saved to database, id: %s" % 
                  jobStatistics["database_ids"]["instance_id"])
    return

    
def insertNewWorkflow(workflowName, requestId, inputDatasets, outputDatasets, appVersion):
    """
    Insert new workflow
    """
    
    try:
        connection = connect()
        dbCur = connection.cursor()
        dbCur.execute("BEGIN")
        
        input_ids = []
        for dataset in inputDatasets:
            input_ids.append(__insertIfNotExist(dbCur, "prodmon_Datasets",
                          {"dataset_name" : dataset},
                           "dataset_id"))
        output_ids = []
        for dataset in outputDatasets:
            output_ids.append(__insertIfNotExist(dbCur, "prodmon_Datasets",
                          {"dataset_name" : dataset},
                           "dataset_id"))
        workflow_id = __insertIfNotExist(dbCur, "prodmon_Workflow", 
                  {"workflow_name" : workflowName,
                   "request_id" : requestId,
                   "app_version" : appVersion},
                  "workflow_id")
        
        __linkWorkflowDatasets(dbCur, workflow_id, input_ids, output_ids)
        
        dbCur.execute("COMMIT")
        dbCur.close()
    except StandardError, ex:
        dbCur.execute("ROLLBACK")
        dbCur.close()
        msg = "Failed to insert workflow and/or datasets: %s\n" % str(ex)
        raise RuntimeError, msg 
    
    return


def __setWorkflowId(dbCur, jobStatistics):
    """
    Workflow already in database so obtain id and save
    to jobStatistics object
    """

    jobStatistics["database_ids"]["workflow_id"] = None
    
    # obtain database id for workflow
    if jobStatistics['workflow_spec_id'] != None:
        sqlStr = """SELECT workflow_id FROM prodmon_Workflow WHERE 
            workflow_name = %s
            """ % addQuotes(jobStatistics['workflow_spec_id'])
    
        dbCur.execute(sqlStr)
        jobStatistics["database_ids"]["workflow_id"] = removeTuple(dbCur.fetchone())


    # if workflow not in db must have missed a NewWorkflow event
    if jobStatistics['database_ids']["workflow_id"] == None:
        msg = "workflow %s not found in prodmon database\n" % \
                                            jobStatistics['workflow_spec_id']
        msg += "Please alert developers\n"
        raise RuntimeError, msg

    return

        
def __linkWorkflowDatasets(dbCur, workflowId, inputIds, outputIds):
    """
    Link datasets and workflow in DB
    
    """
    for dataset in inputIds:
        __insertIfNotExist(dbCur, "prodmon_input_datasets_map",
                {"workflow_id" : workflowId,
                 "dataset_id" : dataset}, 
                "workflow_id",
                returns=False)
   
    for dataset in outputIds:
        __insertIfNotExist(dbCur, "prodmon_output_datasets_map",
                {"workflow_id" : workflowId,
                 "dataset_id" : dataset},
                "workflow_id",
                returns=False)
    return


def __insertInputFiles(dbCur, jobStatistics):
    """
    Insert input files into DB
    
    Returns file_id's
    """
    jobStatistics["database_ids"]["input_file_ids"] = []
    for filename in jobStatistics["input_files"]:
        inserted_id = __insertIfNotExist(dbCur, "prodmon_LFN", 
                                       {"file_name" : filename},
                                       "file_id")
        jobStatistics["database_ids"]["input_file_ids"].append(inserted_id)


def __insertOutputFiles(dbCur, jobStatistics):
    """
    Insert output files into DB
    """
    jobStatistics["database_ids"]["output_file_ids"] = []
    for filename in jobStatistics["output_files"]:
        inserted_id = __insertIfNotExist(dbCur, "prodmon_LFN", 
                                       {"file_name" : filename}, 
                                       "file_id")
        jobStatistics["database_ids"]["output_file_ids"].append(inserted_id)


def __linkFilesJobInstance(dbCur, jobStatistics):
    """
    Fill job_instance LFN mapping tables
    """
    for file_id in jobStatistics["database_ids"]["input_file_ids"]:
        __insert(dbCur, "prodmon_input_LFN_map", 
               {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                "file_id" : file_id},
               returns = False)
    
    for file_id in jobStatistics["database_ids"]["output_file_ids"]:
        __insert(dbCur, "prodmon_output_LFN_map", 
               {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                "file_id" : file_id},
               returns = False)


def __insertJob(dbCur, jobStatistics):
    """
    Insert a job into DB
    """
    job_id = __insertIfNotExist(dbCur, "prodmon_Job", 
                {"workflow_id" : jobStatistics["database_ids"]["workflow_id"],
                 "type" : jobStatistics["job_type"],
                 "job_spec_id" : jobStatistics['job_spec_id']},
                "job_id")
    jobStatistics["database_ids"]["job_id"] = job_id


def __insertResource(dbCur, jobStatistics):
    """
    Insert a Resource
    
    Returns id of resource
    """
    resource_id = __insertIfNotExist(dbCur, "prodmon_Resource", 
                            {"site_name" : jobStatistics["site_name"],
                             "ce_hostname" : jobStatistics["ce_name"],
                             "se_hostname" : jobStatistics["se_name"]},
                            "resource_id")
    jobStatistics["database_ids"]["resource_id"] = resource_id
    
    
def __insertTimings(dbCur, jobStatistics):
    """
    Insert the timings
    
    Does not record inserted ids
    
    Returns nothing
    """
    # ignore start and end as these are in job_instance table
    jobStatistics["database_ids"]["timings"] = {}
    for key, value in jobStatistics["timing"].items():
        if key not in ("AppStartTime", "AppEndTime"):
            __insert(dbCur, "prodmon_Job_timing",
                   {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                   "timing_type" : key,
                   "value" : value},
                   returns=False)

        
def __insertError(dbCur, jobStatistics):
    """
    Insert an error class
    
    """
    if not jobStatistics["error_type"]:
        jobStatistics["database_ids"]["error_id"] = None
    else:
        error_id = __insertIfNotExist(dbCur, "prodmon_Job_errors",
                            {"error_type" : jobStatistics["error_type"]},
                            "error_id")
        jobStatistics["database_ids"]["error_id"] = error_id


def __insertRuns(dbCur, jobStatistics):
    """
    Insert runs
    """
    for run in jobStatistics["run_numbers"]:
        __insert(dbCur, "prodmon_output_runs", 
               {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                "run": run},
               returns=False)


def __insertSkippedEvents(dbCur, jobStatistics):
    """
    Insert SkippedEvents
    """
    for run, event in jobStatistics["skipped_events"]:
        __insert(dbCur, "prodmon_skipped_events", 
               {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                "run": run, "event" : event},
               returns=False)
        
            
def __insertJobInstance(dbCur, jobStatistics):
    """
    Insert a job instance
    
    """
    instance_id = __insert(dbCur, "prodmon_Job_instance",
                         {"job_id" : jobStatistics["database_ids"]["job_id"],
                          "resource_id" : jobStatistics["database_ids"]["resource_id"],
                          "dashboard_id" : jobStatistics["dashboard_id"],
                          "worker_node": jobStatistics["host_name"],
                          "exit_code" : jobStatistics["exit_code"],
                          "evts_read" : jobStatistics["events_read"],
                          "evts_written" : jobStatistics["events_written"],
                          "error_id" : jobStatistics["database_ids"]["error_id"],
                          "error_message" : jobStatistics["error_desc"],
                          "start_time" : jobStatistics["timing"]["AppStartTime"],
                          "end_time" : jobStatistics["timing"]["AppEndTime"],
                          "exported" : False},
                         "instance_id"
                          )
    jobStatistics["database_ids"]["instance_id"] = instance_id
    

def __insertIfNotExist(dbCur, table, values, identifier, returns=True):
    """
    Insert into table or return id of existing entry (if returns = True).
    Takes a DB cursor, table name, dictionary of values and 
        the desired returnValue
    Assumes cursor is active and that
        (if required) a transaction has been started
    Does not catch any DB exception
    Returns id of existing/inserted row
    """
    
    # create key-value pair for SELECT query
    keyvaluepairs = " AND ".join([key + "=" + addQuotes(value) for 
                                 key, value in values.items()])
    selectSQL = "SELECT %s FROM %s WHERE %s" % (identifier, 
                                                table, keyvaluepairs)
    dbCur.execute(selectSQL)
    result = dbCur.fetchone()
    if result != None:
        return removeTuple(result)

    return __insert(dbCur, table, values, returns)
    
    
def __insert(dbCur, table, values, returns=True):
    """
    Insert values into DB
    Does not check if value exists
    Returns id of inserted item if returns = True
    """
    variables = ", ".join([addQuotes(obj) for obj in values.values()])
    
    insertSQL = "INSERT INTO %s (%s) VALUES (%s)" % (
                                                    table,
                                                    ", ".join(values.keys()),
                                                    variables)
    dbCur.execute(insertSQL)
    if returns:
        dbCur.execute("SELECT LAST_INSERT_ID()")
        return removeTuple(dbCur.fetchone())
    

def getMergeInputFiles(jobSpecId):
    """
    _getMergeInputFiles_

    Get the set of merge_inputfile filenames from the merge db
    associated to the job spec ID provided.

    """
    sqlStr = """select merge_inputfile.name from merge_inputfile
      join merge_outputfile on merge_outputfile.id = merge_inputfile.mergedfile
        where merge_outputfile.mergejob="%s"; """ % jobSpecId
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()
    result = []
    for row in rows:
        for f in row:
            result.append(f)
    return result


def getJobInstancesToExport(maxItems):
    """
    _getJobToExport_
    
    Get a list of instance ids (max numItems) ordered by job
    that have not been exported
    
    """
    sqlStr = """SELECT instance_id FROM 
        prodmon_Job_instance WHERE
        exported = FALSE ORDER by job_id LIMIT %s;""" % str(maxItems)
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()
    rows = [removeTuple(row) for row in rows]
    return rows


def getJobInfo(job_id):
    """
    getJobInfo(job_id) return info about the job
    
    Returns dictionary of info
    """
    sqlStr = """SELECT job_id, job_spec_id, type, request_id, workflow_name, 
        prodmon_Workflow.workflow_id, app_version FROM prodmon_Job JOIN prodmon_Workflow 
        WHERE prodmon_Job.job_id = %s AND prodmon_Job.workflow_id = 
        prodmon_Workflow.workflow_id;""" % addQuotes(job_id)
    
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    job = {}
    job["job_id"], job["job_spec_id"], job["job_type"], job["request_id"], \
            job["workflow_name"], job["workflow_id"], job["app_version"] = dbCur.fetchone()
    
    inputDatasetSQL = """SELECT dataset_name from prodmon_Datasets JOIN 
                prodmon_input_datasets_map, prodmon_Workflow WHERE
                prodmon_Workflow.workflow_id = %s AND 
                prodmon_Workflow.workflow_id = prodmon_input_datasets_map.workflow_id 
                AND prodmon_input_datasets_map.dataset_id = 
                prodmon_Datasets.dataset_id;""" % addQuotes(job["workflow_id"])
    dbCur.execute(inputDatasetSQL)
    job["input_datasets"] = [removeTuple(dataset) for dataset in dbCur.fetchall()]
            
    outputDatasetSQL = """SELECT dataset_name from prodmon_Datasets JOIN 
                    prodmon_output_datasets_map, prodmon_Workflow WHERE 
                    prodmon_Workflow.workflow_id = %s AND 
                    prodmon_Workflow.workflow_id = prodmon_output_datasets_map.workflow_id 
                    AND prodmon_output_datasets_map.dataset_id = 
                    prodmon_Datasets.dataset_id;""" % addQuotes(job["workflow_id"])
    dbCur.execute(outputDatasetSQL)
    job["output_datasets"] = [removeTuple(dataset) for dataset in dbCur.fetchall()]
    dbCur.close()

    return job


def getJobStatistics(instance_ids):
    """
    load a job instances from the DB
    
    Returns a tuple of jobStatistics object
    """
    import ProdMon.JobStatistics

    instances = getJobInstancesInfo(instance_ids)
    results = [] #  list of JobStatistics to return
    jobs = {}    #  temp plaeholder for job info
    
    for instance in instances:
        
        # dont query DB if already know about job
        job_id = instance["job_id"]
        if not jobs.has_key(job_id):
            jobs[job_id] = getJobInfo(job_id)
            jobs[job_id]["workflow_spec_id"] = \
                jobs[job_id]["workflow_name"]
    
        # re-arrange instance info
        instance["host_name"] = instance["worker_node"]
        instance["ce_name"] = instance["ce_hostname"]
        instance["se_name"] = instance["se_hostname"]
        instance["run_numbers"] = instance["output_runs"]
        instance["error_desc"] = instance["error_message"]
        instance["events_read"] = instance["evts_read"]
        instance["events_written"] = instance["evts_written"]
        instance["timing"]["AppStartTime"] = instance["start_time"]
        instance["timing"]["AppEndTime"] = instance["end_time"]
                
        # add to jobStatistics
        temp = ProdMon.JobStatistics.JobStatistics()
        temp.update(jobs[job_id])
        temp.update(instance)

        #set status
        if instance["exit_code"] == 0:
            instance["status"] = "Success"
        else:
            instance["status"] = "Failed"

        # save jobStatistics to output list
        results.append(temp)
    return results
    

arrToString = lambda x: (x['error_message'] != None) and \
    (x.__setitem__('error_message', x['error_message'].tostring()))


def getJobInstancesInfo(instance_ids):
    """
    getJobInstanceInfo
    
    Get info for a set of job instances 
    does not provide infor from job table only from instance
    
    Returns a tuple of dictionary objects
    """
    if not instance_ids:
        return {}

    sqlStr = """SELECT job_id, instance_id, site_name, ce_hostname, se_hostname, 
    exit_code, evts_read, evts_written, start_time, end_time, error_message, 
    worker_node, dashboard_id FROM prodmon_Job_instance JOIN prodmon_Resource WHERE 
    prodmon_Job_instance.resource_id = prodmon_Resource.resource_id AND ("""

    first = True
    for instance in instance_ids:
        if not first:
            sqlStr += " OR "
        else:
            first = False
        sqlStr += " instance_id = " + addQuotes(instance)
    sqlStr += ");"
    connection = connect()
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)

    dbCur.execute(sqlStr)

    results = dbCur.fetchall()
    # dbCur.close() # TODO: fails with this, why?
    dbCur = connection.cursor()
    
    for instance in results:
        instance_id = instance["instance_id"]
        
        # convert error_message array ot string
        arrToString(instance)
        
        # add error type
        if instance["exit_code"] != 0:
            errorSQL = """SELECT error_type FROM prodmon_Job_errors 
                        JOIN prodmon_Job_instance WHERE 
                        prodmon_Job_instance.instance_id = %s AND
                        prodmon_Job_instance.error_id = 
                        prodmon_Job_errors.error_id;""" % addQuotes(instance_id)
            dbCur.execute(errorSQL)
            instance["error_type"] = removeTuple(dbCur.fetchone())
        else:
            instance["error_type"] = None
        
        # LFN's
        inputLFNSQL = """SELECT file_name FROM prodmon_LFN 
                    JOIN prodmon_input_LFN_map, prodmon_Job_instance 
                    WHERE prodmon_Job_instance.instance_id = %s AND
                    prodmon_Job_instance.instance_id = prodmon_input_LFN_map.instance_id
                    AND prodmon_input_LFN_map.file_id = 
                    prodmon_LFN.file_id;""" % addQuotes(instance_id)

        dbCur.execute(inputLFNSQL)
        instance["input_files"] = [removeTuple(file) for file in dbCur.fetchall()]

        outputLFNSQL = """SELECT file_name FROM prodmon_LFN 
                    JOIN prodmon_output_LFN_map, prodmon_Job_instance 
                    WHERE prodmon_Job_instance.instance_id = %s AND
                    prodmon_Job_instance.instance_id = prodmon_output_LFN_map.instance_id
                    AND prodmon_output_LFN_map.file_id = 
                    prodmon_LFN.file_id;""" % addQuotes(instance_id)
        dbCur.execute(outputLFNSQL)
        instance["output_files"] = [removeTuple(file) for file in dbCur.fetchall()]
        
        # timing
        timingSQL = """SELECT timing_type, value from prodmon_Job_timing 
                    WHERE instance_id = %s;""" % addQuotes(instance_id)
        dbCur.execute(timingSQL)
        rows = dbCur.fetchall()
        
        instance["timing"] = {}
        for key, value in rows:
            instance["timing"][key] = value
        # instance["timing"] = [(key, value) for key, value in rows]
       
        # runs
        runSQL = """SELECT run from prodmon_output_runs 
                WHERE instance_id = %s;""" % addQuotes(instance_id)
        dbCur.execute(runSQL)
        instance["output_runs"] = [removeTuple(run) for run in dbCur.fetchall()]
        
        # skipped events
        skippedSQL = """SELECT run, event FROM 
                        prodmon_skipped_events WHERE 
                        instance_id = %s;""" % addQuotes(instance_id)
        dbCur.execute(skippedSQL)
        instance["skipped_events"] = dbCur.fetchall()
      
    dbCur.close()
    return results


def markInstancesExported(instances):
    """
    Mark job instances as exported
    """
    
    sqlStr = """UPDATE prodmon_Job_instance SET exported = true WHERE """
    first = True
    for instance in instances:
        if not first:
            sqlStr += " OR "
        else:
            first = False
        sqlStr += "instance_id = " + addQuotes(instance)
    
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    dbCur.execute("COMMIT")
    dbCur.close()
    return


def selectSuccessCount(workflowSpec = None):
    """
    _selectSuccessCount_

    Query DB for number of successful jobs in DB, restricted by workflowSpec
    if specified

    """
    sqlStr = "SELECT COUNT(*) FROM prodmon_Job_instance "
    if workflowSpec != None:
        sqlStr += """JOIN prodmon_Job, prodmon_Workflow WHERE 
            prodmon_Job_instance.job_id = prodmon_Job.job_id AND
            prodmon_Workflow.workflow_id = prodmon_Job.workflow_id
            AND workflow_name = %s AND """ % addQuotes(workflowSpec)
    else:
        sqlStr += "WHERE "
    sqlStr += "prodmon_Job_instance.exit_code = \'0\';"
    
    connection = connect()
    dbCur = connection.cursor()
    
    dbCur.execute(sqlStr)
    count = removeTuple(dbCur.fetchone())
    dbCur.close()
    return count


def selectFailureCount(workflowSpec = None):
    """
    _selectFailureCount_

    Query DB for number of failed jobs in DB, restricted by workflowSpec
    if specified

    """
    sqlStr = "SELECT COUNT(*) FROM prodmon_Job_instance "
    if workflowSpec != None:
        sqlStr += """JOIN prodmon_Job, prodmon_Workflow WHERE 
            prodmon_Job_instance.job_id = prodmon_Job.job_id AND
            prodmon_Workflow.workflow_id = prodmon_Job.workflow_id
            AND workflow_name = %s AND """ % addQuotes(workflowSpec)
    else:
        sqlStr += "WHERE "
    sqlStr += "prodmon_Job_instance.exit_code != \'0\';"

    connection = connect()
    dbCur = connection.cursor()
    
    dbCur.execute(sqlStr)
    count = removeTuple(dbCur.fetchone())
    dbCur.close()
    return count


def jobTypeSuccess(workflowSpec = None):
    """
    _jobTypeSuccess_

    Get the count of job types for successes.
    

    """
    sqlStr = """SELECT type, COUNT(type) FROM prodmon_Job 
                JOIN prodmon_Job_instance, prodmon_Workflow WHERE 
                prodmon_Job_instance.job_id = prodmon_Job.job_id AND
                prodmon_Job.workflow_id = prodmon_Workflow.workflow_id
                AND exit_code = 0"""
    if workflowSpec == None:
        sqlStr += " GROUP BY type;"
    else:
        sqlStr += " AND workflow_name= %s GROUP BY type;" % addQuotes(workflowSpec)
    
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()
   
    result = {}
    for item, value in rows:
        result[item] = value
    return result
    

def jobTypeFailures(workflowSpec = None):
    """
    _jobTypeFailuresCounts_

    Get the count of job types for failures.
    

    """
    sqlStr = """SELECT type, COUNT(type) FROM prodmon_Job 
                JOIN prodmon_Job_instance, prodmon_Workflow WHERE 
                prodmon_Job_instance.job_id = prodmon_Job.job_id AND
                prodmon_Job.workflow_id = prodmon_Workflow.workflow_id
                AND exit_code != 0"""
    if workflowSpec == None:
        sqlStr += " GROUP BY type;"
    else:
        sqlStr += " AND workflow_name= %s GROUP BY type;" % addQuotes(workflowSpec)
    
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()
    
    result = {}
    for item, value in rows:
        result[item] = value
    return result


def selectDetails(workflowSpecId, sinceTime=86400, jobType = None, success = None):
    """
    _selectDetails_

    extract the details of job instances for the workflowSpecId provided
    for the time interval provided. For the job type (if provided) and 
    for success or failure (success != None)

    """
    sqlStr ="""SELECT instance_id FROM prodmon_Job_instance
            JOIN prodmon_Job, prodmon_Workflow WHERE prodmon_Job.job_id = 
            prodmon_Job_instance.job_id AND prodmon_Job.workflow_id = 
            prodmon_Workflow.workflow_id AND prodmon_Workflow.workflow_name = 
            %s AND end_time > (UNIX_TIMESTAMP(NOW()) - %s)
            """ % (addQuotes(workflowSpecId),
                                        sinceTime)

    if jobType != None:
        sqlStr += " AND type= %s" % addQuotes(jobType)
    if success == True:
        sqlStr += " AND exit_code = 0"
    if success == False:
        sqlStr += " AND exit_code != 0"
    sqlStr += ";"
    
    connection = connect()
    dbCur = connection.cursor()
    # dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    dbCur.execute(sqlStr)
    rows = dbCur.fetchall()
    dbCur.close()   
    
    rows = [removeTuple(instance) for instance in rows]
    return getJobStatistics(rows)


def selectEventsWritten(workflowSpecId):
    """
    _selectEventsWritten_

    Get total events written for request.

    """
    
    sqlStr = """SELECT SUM(evts_written) FROM 
    prodmon_Job_instance JOIN prodmon_Job, prodmon_Workflow 
    WHERE prodmon_Workflow.workflow_id = prodmon_Job.workflow_id 
    AND prodmon_Job.job_id = prodmon_Job_instance.job_id AND 
    prodmon_Job_instance.exit_code != 0 AND workflow_spec_id = %s
    """ % addQuotes(workflowSpecId)

    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    count = removeTuple(dbCur.fetchone())
    dbCur.close()
    return count


def removeTuple(objectInATuple):
    """
    _removeTuple_

    Given (object,)  return object

    """
    if objectInATuple != None:
        return objectInATuple[0]
    else:
        return objectInATuple


def listWorkflowSpecs():
    """
    _listWorkflowSpecs_

    Return a list of all workflow spec ids (request names) in the ProdMon
    
    """
    sqlStr = "SELECT workflow_name from prodmon_Workflow;"

    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    workflows = dbCur.fetchall() 
    workflows = [removeTuple(workflow) for workflow in workflows]
    return workflows
    

def activeWorkflowSpecs(interval=86400):
    """
    _activeWorkflowSpecs_

    Return a list of workflow specs that have had an entry created in the
    time interval provided until now

    """
    sqlStr = """SELECT DISTINCT workflow_name FROM prodmon_Workflow JOIN prodmon_Job, 
    prodmon_Job_instance WHERE prodmon_Workflow.workflow_id = 
    prodmon_Job.workflow_id AND prodmon_Job_instance.job_id = 
    prodmon_Job.job_id AND prodmon_Job_instance.end_time > 
    (UNIX_TIMESTAMP(NOW()) - %s);""" % interval
    
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    workflows = [removeTuple(workflow) for workflow in dbCur.fetchall()]
    dbCur.close()
    return workflows


def listSites():
    """
    _listSites_

    Return a list of all sites in the ProdMon
    
    """
    sqlStr = "SELECT DISTINCT site_name from prodmon_Resource;"
    
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    results = dbCur.fetchall() 
    sites = [removeTuple(site) for site in results]
    return sites


def selectSiteDetails(site, interval=86400, workflow=None, type=None):
    """
    Return a dictionary detailing site performances since interval
    """
    
    sqlStr = """SELECT exit_code FROM prodmon_Resource JOIN 
            prodmon_Job, prodmon_Job_instance, prodmon_Workflow WHERE 
            prodmon_Resource.resource_id = prodmon_Job_instance.resource_id 
            AND prodmon_Job.job_id = prodmon_Job_instance.job_id AND 
            prodmon_Job_instance.end_time > (UNIX_TIMESTAMP(NOW()) - %s)
            """ % interval
    if site != None:
        sqlStr += " AND prodmon_Resource.resource_name = " % addQuotes(site)
    if workflow != None:
        sqlStr += " AND prodmon_Workflow.workflow_name = " % addQuotes(workflow)
    if type != None:
        sqlStr += " AND prodmon_Job.type = " % addQuotes(type)
    
    connection = connect()
    dbCur = connection.cursor()
    dbCur.execute(sqlStr)
    results = [ removeTuple(x) for x in dbCur.fetchall() ] 
    
    performance = {}
    performance["jobs"] = 0
    performance["failed"] = 0
    performance["success"] = 0
    
    for exit_code in results:
        performance["jobs"] += 1
        if exit_code == 0:
            performance["success"] += 1
        else:
            performance["failed"] += 1
    
    return performance