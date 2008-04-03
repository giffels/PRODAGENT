# !/usr/bin/env python
"""
_ProdMonDB_

DB API for inserting JobStatistics instances into the DB tables
and for recalling and formating as XML

"""

#TODO: stored procedures for frequent operations?

import logging
import MySQLdb
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig

# do we want to export this job to external monitoring
# ignore logArchive and CleanUp jobs
exportable = lambda x: x not in  ('CleanUp', 'LogArchive') and True or False


def addQuotes(value):
    """
    function to quote mysql values properly)
    """
    if value is None:
        return 'NULL'
    elif value is True:
        return 'true'
    elif value is False:
        return 'false'
    else:
        return "\'" + MySQLdb.escape_string(str(value)) + "\'"
    

def insertStats(jobStatistics):
    """
    _insertStats_

    Insert the jobStatistics instance in the database

    """
    try:
        #Session.set_database(dbConfig)
        #Session.connect()
        #Session.start_transaction()
               
        #connection = connect()
        #Session = connection.cursor()
        
        # perform all db actions within transaction
        # technically not everything needs to be in same
        # transaction - but easier
        #Session.execute("BEGIN")
        
        # insert/link tables
        # Note: Order does matter as these methods
        # rely on id's of foreign keys being in jobStatistics
        __setWorkflowId(Session, jobStatistics)
        __insertJob(Session, jobStatistics)
        __insertResource(Session, jobStatistics)
        __insertError(Session, jobStatistics)
        __insertInputFiles(Session, jobStatistics)
        __insertOutputFiles(Session, jobStatistics)
        __insertJobInstance(Session, jobStatistics)
        __linkFilesJobInstance(Session, jobStatistics)
        __insertRuns(Session, jobStatistics)
        __insertSkippedEvents(Session, jobStatistics)
        __insertTimings(Session, jobStatistics)
        __insertPerformanceReport(Session, jobStatistics)
        
        #Session.commit()
        #Session.close()
    except StandardError, ex:
        # Close DB connection and re-throw
        #Session.rollback()
        #Session.close()
        msg = "Failed to insert JobStatistics: %s\n" % str(ex)
        raise RuntimeError, msg    
        
    logging.debug("Job successfully saved to database, id: %s" % 
                  jobStatistics["database_ids"]["instance_id"])
    return

    
def insertNewWorkflow(workflowName, requestId, inputDatasets, outputDatasets, appVersion):
    """
    Insert new workflow
    """
    
    try:
        #Session.set_database(dbConfig)
        #Session.connect()
        #Session.start_transaction()
        
        input_ids = []
        for dataset in inputDatasets:
            input_ids.append(__insertIfNotExist(Session, "prodmon_Datasets",
                          {"dataset_name" : dataset},
                           "dataset_id"))
        output_ids = []
        for dataset in outputDatasets:
            output_ids.append(__insertIfNotExist(Session, "prodmon_Datasets",
                          {"dataset_name" : dataset},
                           "dataset_id"))
        workflow_id = __insertIfNotExist(Session, "prodmon_Workflow", 
                  {"workflow_name" : workflowName,
                   "request_id" : requestId,
                   "app_version" : appVersion},
                  "workflow_id")
        
        __linkWorkflowDatasets(Session, workflow_id, input_ids, output_ids)
        
        #Session.commit()
        #Session.close()
    except StandardError, ex:
        #Session.rollback()
        #Session.close()
        msg = "Failed to insert workflow and/or datasets: %s\n" % str(ex)
        raise RuntimeError, msg 
    
    return


def __setWorkflowId(Session, jobStatistics):
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
    
        Session.execute(sqlStr)
        jobStatistics["database_ids"]["workflow_id"] = removeTuple(Session.fetchone())

    #TODO: Temp prodmgr hack
    #see https://savannah.cern.ch/task/?6367
    if jobStatistics['database_ids']["workflow_id"] == None:
        sqlStr = """SELECT workflow_id FROM prodmon_Workflow WHERE 
            workflow_name LIKE %s
            """ % addQuotes(jobStatistics['workflow_spec_id'].replace('-','%'))
    
        Session.execute(sqlStr)
        jobStatistics["database_ids"]["workflow_id"] = removeTuple(Session.fetchone())

    # if workflow not in db must have missed a NewWorkflow event
    if jobStatistics['database_ids']["workflow_id"] == None:
        msg = "workflow %s not found in prodmon database " % \
                                            jobStatistics['workflow_spec_id']
        msg += ". Did you publish a NewWorkflow event?\n"
        raise RuntimeError, msg

    return

        
def __linkWorkflowDatasets(Session, workflowId, inputIds, outputIds):
    """
    Link datasets and workflow in DB
    
    """
    for dataset in inputIds:
        __insertIfNotExist(Session, "prodmon_input_datasets_map",
                {"workflow_id" : workflowId,
                 "dataset_id" : dataset}, 
                "workflow_id",
                returns=False)
   
    for dataset in outputIds:
        __insertIfNotExist(Session, "prodmon_output_datasets_map",
                {"workflow_id" : workflowId,
                 "dataset_id" : dataset},
                "workflow_id",
                returns=False)
    return


def __insertInputFiles(Session, jobStatistics):
    """
    Insert input files into DB
    
    Returns file_id's
    """
    jobStatistics["database_ids"]["input_file_ids"] = []
    for filename in jobStatistics["input_files"]:
        inserted_id = __insertIfNotExist(Session, "prodmon_LFN", 
                                       {"file_name" : filename},
                                       "file_id")
        jobStatistics["database_ids"]["input_file_ids"].append(inserted_id)


def __insertOutputFiles(Session, jobStatistics):
    """
    Insert output files into DB
    """
    jobStatistics["database_ids"]["output_file_ids"] = []
    for filename in jobStatistics["output_files"]:
        inserted_id = __insertIfNotExist(Session, "prodmon_LFN", 
                                       {"file_name" : filename}, 
                                       "file_id")
        jobStatistics["database_ids"]["output_file_ids"].append(inserted_id)


def __linkFilesJobInstance(Session, jobStatistics):
    """
    Fill job_instance LFN mapping tables
    """
    for file_id in jobStatistics["database_ids"]["input_file_ids"]:
        __insert(Session, "prodmon_input_LFN_map", 
               {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                "file_id" : file_id},
               returns = False)
    
    for file_id in jobStatistics["database_ids"]["output_file_ids"]:
        __insert(Session, "prodmon_output_LFN_map", 
               {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                "file_id" : file_id},
               returns = False)


def __insertJob(Session, jobStatistics):
    """
    Insert a job into DB
    """
    job_id = __insertIfNotExist(Session, "prodmon_Job", 
                {"workflow_id" : jobStatistics["database_ids"]["workflow_id"],
                 "type" : jobStatistics["job_type"],
                 "job_spec_id" : jobStatistics['job_spec_id']},
                "job_id")
    jobStatistics["database_ids"]["job_id"] = job_id


def __insertResource(Session, jobStatistics):
    """
    Insert a Resource
    
    Returns id of resource
    """
    
    resource_id = __insertIfNotExist(Session, "prodmon_Resource", 
                            {"site_name" : jobStatistics["site_name"],
                             "rc_site_id" : jobStatistics["rc_site_index"],
                             "ce_hostname" : jobStatistics["ce_name"],
                             "se_hostname" : jobStatistics["se_name"]},
                            "resource_id")
    jobStatistics["database_ids"]["resource_id"] = resource_id
    
    
def __insertTimings(Session, jobStatistics):
    """
    Insert the timings
    
    Does not record inserted ids
    
    Returns nothing
    """
    # ignore start and end as these are in job_instance table
    jobStatistics["database_ids"]["timings"] = {}
    for key, value in jobStatistics["timing"].items():
        if key not in ("AppStartTime", "AppEndTime"):
            __insert(Session, "prodmon_Job_timing",
                   {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                   "timing_type" : key,
                   "value" : value},
                   returns=False)

        
def __insertError(Session, jobStatistics):
    """
    Insert an error class
    
    """
    if not jobStatistics["error_type"]:
        jobStatistics["database_ids"]["error_id"] = None
    else:
        error_id = __insertIfNotExist(Session, "prodmon_Job_errors",
                            {"error_type" : jobStatistics["error_type"]},
                            "error_id")
        jobStatistics["database_ids"]["error_id"] = error_id


def __insertRuns(Session, jobStatistics):
    """
    Insert runs
    """
    for run in jobStatistics["run_numbers"]:
        __insert(Session, "prodmon_output_runs", 
               {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                "run": run},
               returns=False)


def __insertSkippedEvents(Session, jobStatistics):
    """
    Insert SkippedEvents
    """
    for run, event in jobStatistics["skipped_events"]:
        __insert(Session, "prodmon_skipped_events", 
               {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                "run": run, "event" : event},
               returns=False)
        
        
def __insertPerformanceReport(Session, jobStatistics):
    """
    Insert performance report
    """
    
    #TODO: Add in proper core support
    
    perf_report = jobStatistics.get("performance_report", None)

    if perf_report == None:
        logging.info("performance report not found - skipping")
        return

    #node properties
    if len(perf_report.cpus) == 0 or len(perf_report.memory) == 0:
        logging.info("performance report: cpu/memory info not found")
        return

    # assume that all cpu's have same number of cores and that all have same properties
    cpu = perf_report.cpus.values()[0]
    node_id = __insertIfNotExist(Session, "prodmon_node_properties",
                {"cpu_speed" : cpu["Speed"],
                 "cpu_description" : cpu["Description"],
                 "number_cpu" : len(perf_report.cpus),
                 "number_core" : len(perf_report.cpus),
                 "memory" : perf_report.memory["MemTotal"]},
                 "node_id")
    jobStatistics["database_ids"]["node_properties_id"] = node_id

    #link node_properties and instance
    __insert(Session, "prodmon_node_map",
             {"instance_id" : jobStatistics["database_ids"]["instance_id"],
              "node_id" : jobStatistics["database_ids"]["node_properties_id"]},
              returns = False
              )
                 
    #summary info
    for metric_class, metric in perf_report.summaries.items():
        for name, value in metric.items():
            __insert(Session, "prodmon_performance_summary",
                 {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                  "metric_class" : metric_class,
                  "metric_name" : name,
                  "metric_value" : value},
                  "instance_id")

    #module info
    for module, details in perf_report.modules.items():
        for metric_class, metrics in details.items():
            for metric in metrics:
                for name, value in metric.items():
                    __insert(Session, "prodmon_performance_modules",
                             {"instance_id" : jobStatistics["database_ids"]["instance_id"],
                             "module_name" : module,
                             "metric_class" : metric_class,
                             "metric_name" : name,
                             "metric_value" : value},
                             returns = False)

            
def __insertJobInstance(Session, jobStatistics):
    """
    Insert a job instance
    
    """
    instance_id = __insert(Session, "prodmon_Job_instance",
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
                          "exported" : not exportable(jobStatistics['job_type'])},
                         "instance_id"
                          )
    jobStatistics["database_ids"]["instance_id"] = instance_id
    

def __insertIfNotExist(Session, table, values, identifier, returns=True):
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
    Session.execute(selectSQL)
    result = Session.fetchone()
    if result != None:
        return removeTuple(result)

    return __insert(Session, table, values, returns)
    
    
def __insert(Session, table, values, returns=True):
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
    Session.execute(insertSQL)
    if returns:
        Session.execute("SELECT LAST_INSERT_ID()")
        return removeTuple(Session.fetchone())
    

def getMergeInputFiles(jobSpecId):
    """
    _getMergeInputFiles_

    Get the set of merge_inputfile filenames from the merge db
    associated to the job spec ID provided.

    """
    sqlStr = """select merge_inputfile.name from merge_inputfile
      join merge_outputfile on merge_outputfile.id = merge_inputfile.mergedfile
        where merge_outputfile.mergejob="%s"; """ % jobSpecId
    #Session.set_database(dbConfig)
    #Session.connect()
    Session.execute(sqlStr)
    rows = Session.fetchall()
    #Session.close()
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
    #Session.set_database(dbConfig)
    #Session.connect()
    Session.execute(sqlStr)
    rows = Session.fetchall()
    #Session.close()
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
    
    #Session.set_database(dbConfig)
    #Session.connect()
    Session.execute(sqlStr)
    job = {}
    job["job_id"], job["job_spec_id"], job["job_type"], job["request_id"], \
            job["workflow_name"], job["workflow_id"], job["app_version"] = Session.fetchone()
    
    inputDatasetSQL = """SELECT dataset_name from prodmon_Datasets JOIN 
                prodmon_input_datasets_map, prodmon_Workflow WHERE
                prodmon_Workflow.workflow_id = %s AND 
                prodmon_Workflow.workflow_id = prodmon_input_datasets_map.workflow_id 
                AND prodmon_input_datasets_map.dataset_id = 
                prodmon_Datasets.dataset_id;""" % addQuotes(job["workflow_id"])
    Session.execute(inputDatasetSQL)
    job["input_datasets"] = [removeTuple(dataset) for dataset in Session.fetchall()]
            
    outputDatasetSQL = """SELECT dataset_name from prodmon_Datasets JOIN 
                    prodmon_output_datasets_map, prodmon_Workflow WHERE 
                    prodmon_Workflow.workflow_id = %s AND 
                    prodmon_Workflow.workflow_id = prodmon_output_datasets_map.workflow_id 
                    AND prodmon_output_datasets_map.dataset_id = 
                    prodmon_Datasets.dataset_id;""" % addQuotes(job["workflow_id"])
    Session.execute(outputDatasetSQL)
    job["output_datasets"] = [removeTuple(dataset) for dataset in Session.fetchall()]
    #Session.close()

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
    does not provide info from job table only from instance
    
    Returns a tuple of dictionary objects
    """
    if not instance_ids:
        return ()

    sqlStr = """SELECT job_id, instance_id, site_name, ce_hostname, se_hostname, 
    exit_code, evts_read, evts_written, start_time, end_time, error_message, 
    worker_node, dashboard_id, UNIX_TIMESTAMP(insert_time) FROM prodmon_Job_instance JOIN prodmon_Resource WHERE 
    prodmon_Job_instance.resource_id = prodmon_Resource.resource_id AND ("""

    first = True
    for instance in instance_ids:
        if not first:
            sqlStr += " OR "
        else:
            first = False
        sqlStr += " instance_id = " + addQuotes(instance)
    sqlStr += ");"

    #Session.set_database(dbConfig)
    #Session.connect()
    Session.execute(sqlStr)
    temp = Session.fetchall()

    results = []
    for instance in temp:
        i = {}
        i["job_id"], i["instance_id"], i["site_name"], i["ce_hostname"], i["se_hostname"], \
                i["exit_code"], i["evts_read"], i["evts_written"], i["start_time"], i["end_time"], i["error_message"], \
                i["worker_node"], i["dashboard_id"], i["insert_time"] = instance
        results.append(i)
    
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
            Session.execute(errorSQL)
            instance["error_type"] = removeTuple(Session.fetchone())
        else:
            instance["error_type"] = None
        
        # LFN's
        inputLFNSQL = """SELECT file_name FROM prodmon_LFN 
                    JOIN prodmon_input_LFN_map, prodmon_Job_instance 
                    WHERE prodmon_Job_instance.instance_id = %s AND
                    prodmon_Job_instance.instance_id = prodmon_input_LFN_map.instance_id
                    AND prodmon_input_LFN_map.file_id = 
                    prodmon_LFN.file_id;""" % addQuotes(instance_id)

        Session.execute(inputLFNSQL)
        instance["input_files"] = [removeTuple(file) for file in Session.fetchall()]

        outputLFNSQL = """SELECT file_name FROM prodmon_LFN 
                    JOIN prodmon_output_LFN_map, prodmon_Job_instance 
                    WHERE prodmon_Job_instance.instance_id = %s AND
                    prodmon_Job_instance.instance_id = prodmon_output_LFN_map.instance_id
                    AND prodmon_output_LFN_map.file_id = 
                    prodmon_LFN.file_id;""" % addQuotes(instance_id)
        Session.execute(outputLFNSQL)
        instance["output_files"] = [removeTuple(file) for file in Session.fetchall()]
        
        # timing
        timingSQL = """SELECT timing_type, value from prodmon_Job_timing 
                    WHERE instance_id = %s;""" % addQuotes(instance_id)
        Session.execute(timingSQL)
        rows = Session.fetchall()
        
        instance["timing"] = {}
        for key, value in rows:
            instance["timing"][key] = value
        # instance["timing"] = [(key, value) for key, value in rows]
       
        # runs
        runSQL = """SELECT run from prodmon_output_runs 
                WHERE instance_id = %s;""" % addQuotes(instance_id)
        Session.execute(runSQL)
        instance["output_runs"] = [removeTuple(run) for run in Session.fetchall()]
        
        # skipped events
        skippedSQL = """SELECT run, event FROM 
                        prodmon_skipped_events WHERE 
                        instance_id = %s;""" % addQuotes(instance_id)
        Session.execute(skippedSQL)
        instance["skipped_events"] = Session.fetchall()
      
    #Session.close()
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
    
    #Session.set_database(dbConfig)
    #Session.connect()
    #Session.start_transaction()
    Session.execute(sqlStr)
    #Session.commit()
    #Session.close()
    return


def deleteOldJobs(interval, deleteUnexported=False):
    """
    delete jobs older than interval
        (if !deleteUnexported only delete exported jobs)
    """
    
    logging.debug("expiring jobs older than %s" % interval)
    
    sqlStr = """DELETE FROM prodmon_Job, prodmon_Job_instance 
            USING prodmon_Job, prodmon_Job_instance
            WHERE prodmon_Job.job_id = prodmon_Job_instance.job_id
            AND insert_time < ADDTIME(NOW(), "-%s")""" % interval
    
    if not deleteUnexported:
        sqlStr += " AND exported = true"

    Session.execute(sqlStr)
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
    
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    count = removeTuple(Session.fetchone())
    Session.close()
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

    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    count = removeTuple(Session.fetchone())
    Session.close()
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
    
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    rows = Session.fetchall()
    Session.close()
   
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
    
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    rows = Session.fetchall()
    Session.close()
    
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
            %s AND insert_time > ADDTIME(NOW(), SEC_TO_TIME(-%s))
            """ % (addQuotes(workflowSpecId),
                                        sinceTime)

    if jobType != None:
        sqlStr += " AND type= %s" % addQuotes(jobType)
    if success == True:
        sqlStr += " AND exit_code = 0"
    if success == False:
        sqlStr += " AND exit_code != 0"
    sqlStr += ";"
    
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    rows = Session.fetchall()
    Session.close()   
    
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

    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    count = removeTuple(Session.fetchone())
    Session.close()
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

    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    workflows = Session.fetchall() 
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
    prodmon_Job.job_id AND prodmon_Job_instance.insert_time > 
    ADDTIME(NOW(), SEC_TO_TIME(-%s));""" % interval
    
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    workflows = [removeTuple(workflow) for workflow in Session.fetchall()]
    Session.close()
    return workflows


def listSites():
    """
    _listSites_

    Return a list of all sites in the ProdMon
    
    """
    sqlStr = "SELECT DISTINCT site_name from prodmon_Resource;"
    
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    results = Session.fetchall()
    Session.close() 
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
            prodmon_Job_instance.insert_time > ADDTIME(NOW(), SEC_TO_TIME(-%s))
            """ % interval
    if site != None:
        sqlStr += " AND prodmon_Resource.resource_name = " % addQuotes(site)
    if workflow != None:
        sqlStr += " AND prodmon_Workflow.workflow_name = " % addQuotes(workflow)
    if type != None:
        sqlStr += " AND prodmon_Job.type = " % addQuotes(type)
    
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    results = [ removeTuple(x) for x in Session.fetchall() ] 
    Session.close()
    
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
            
    performance["quality"] = (performance["jobs"] > 0) and \
                (float(performance["success"]) / performance["jobs"]) or 0.
    
    return performance


def selectRcSitePerformance(site, interval=86400, workflow=None, type=None):
    """
    Return a dictionary detailing site performances since interval
    """
    
    sqlStr = """SELECT prodmon_Job.type, exit_code FROM prodmon_Resource JOIN 
            prodmon_Job, prodmon_Job_instance, prodmon_Workflow WHERE 
            prodmon_Resource.resource_id = prodmon_Job_instance.resource_id 
            AND prodmon_Job.job_id = prodmon_Job_instance.job_id AND 
            prodmon_Job_instance.insert_time > ADDTIME(NOW(), SEC_TO_TIME(-%s))
            """ % interval
    if site != None:
        sqlStr += " AND prodmon_Resource.rc_site_id = %s" % addQuotes(site)
    if workflow != None:
        sqlStr += " AND prodmon_Workflow.workflow_name = %s" % addQuotes(workflow)
    if type != None:
        sqlStr += " AND prodmon_Job.type = %s" % addQuotes(type)
    
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    results = Session.fetchall() 
    Session.close()
    
    performance = {}
    performance["jobs"] = 0
    performance["failed"] = 0
    performance["success"] = 0
    performance['types'] = {}
    
    for type, exit_code in results:
        
        performance['types'].setdefault(type, {}).setdefault('jobs', 0)
        performance['types'][type].setdefault('failed', 0)
        performance['types'][type].setdefault('success', 0)
        
        performance["jobs"] += 1
        performance['types'][type]["jobs"] += 1
        if exit_code == 0:
            performance["success"] += 1
            performance['types'][type]['success'] += 1
        else:
            performance["failed"] += 1
            performance['types'][type]['failed'] += 1
            
    performance["quality"] = (performance["jobs"] > 0) and \
                (float(performance["success"]) / performance["jobs"]) or None
    
    for type in performance['types'].values():
        type["quality"] = (type["jobs"] > 0) and \
                (float(type["success"]) / type["jobs"]) or None
    
    return performance


def getOutputDatasets(workflow):
    """
    return the output datasets for this workflow
    """
    
    sqlStr = """SELECT prodmon_Datasets.dataset_name
                 FROM prodmon_Workflow 
                 JOIN prodmon_output_datasets_map ON prodmon_Workflow.workflow_id = prodmon_output_datasets_map.workflow_id 
                 JOIN prodmon_Datasets ON prodmon_Datasets.dataset_id = prodmon_output_datasets_map.dataset_id
                 WHERE prodmon_Workflow.workflow_name = '%s'""" % workflow
                 
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    rows = Session.fetchall() 
    Session.close()
    
    results = [removeTuple(instance) for instance in rows]
    return results


def getInputDatasets(workflow):
    """
    return the output datasets for this workflow
    """
    
    sqlStr = """SELECT prodmon_Datasets.dataset_name
                 FROM prodmon_Workflow 
                 JOIN prodmon_input_datasets_map ON prodmon_Workflow.workflow_id = prodmon_input_datasets_map.workflow_id 
                 JOIN prodmon_Datasets ON prodmon_Datasets.dataset_id = prodmon_input_datasets_map.dataset_id
                 WHERE prodmon_Workflow.workflow_name = '%s'""" % workflow
                 
    Session.set_database(dbConfig)
    Session.connect()
    Session.execute(sqlStr)
    rows = Session.fetchall() 
    Session.close()
    
    results = [removeTuple(instance) for instance in rows]
    return results