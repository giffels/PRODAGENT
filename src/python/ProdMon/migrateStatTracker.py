#!/usr/bin/env python
"""
    Script to migrate monitoring information from StatTracker
    
    Warning: Jobs are not inserted in a transaction therefore duplicates
        are possible on multiple invocation
            Each job uses a transcation and MySQL forbids nested transcations
"""

from ProdMon.JobStatistics import JobStatistics
from ProdMon.ProdMonDB import insertNewWorkflow, removeTuple, addQuotes
import MySQLdb
from ProdAgentDB.Connect import connect
import os
import sys
import time

#TODO: Chane database connection handling

CONNECTION_STRING="-uroot -pXXXXX --socket=/srv/localstage/cmsprod/mysqldata/mysql.sock PRODAGENT_0_3_0 devWork"

def migrate():
    """
    Function to migrate data from StatTracker to ProdMon
    """
    
    print "Connecting to database"
    connection = connect()
    #dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    #dbCur.execute("BEGIN")

    print "Wipe old DB"
    wipeDB(connection)

    print "Migrating workflows..."
    migrateWorkflows(connection)
    
    print "Migrating jobs..."
    migrateJobs(connection)
    
    #dbCur.execute("COMMIT")
    
    print "Migration Successful"
    
    return


def wipeDB(connection):
    """
    Wipe ProdMon tables
    """
    
    dbCur = connection.cursor()
    
    tables = """prodmon_Datasets prodmon_Job prodmon_Job_errors 
    prodmon_Job_instance prodmon_Job_timing prodmon_LFN prodmon_Resource
    prodmon_Workflow prodmon_input_LFN_map prodmon_input_datasets_map 
    prodmon_output_LFN_map prodmon_output_datasets_map prodmon_output_runs
    prodmon_skipped_events"""
    
    for table in tables.split():
        dbCur.execute("DELETE FROM %s" % table)
        dbCur.execute("COMMIT")    

    return


def loadStatTrackerDB(connection):
    """
    Load StatTRacker tables into DB
    """
    
    result = os.system("mysql %s < %s" % (CONNECTION_STRING, file))
    if (result):
        raise RuntimeError, "Error loading StatTracker data from %s" % file
    return


def migrateJobs(connection):
    
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    dbCur1 = connection.cursor()
    
    dbCur.execute("""SELECT workflow_spec_id, job_spec_id, exit_code, status, site_name,
               host_name, se_name, events_read, events_written, job_type, job_index FROM st_job_success""")
    jobs = dbCur.fetchall()
    dbCur.execute("""SELECT workflow_spec_id, job_spec_id, exit_code, status, site_name,
               host_name, se_name, job_type, job_index FROM st_job_failure""")
    jobs += dbCur.fetchall()
    
    for job in jobs:
        
        #save index for later queries
        job_index = job["job_index"]
        
        #create jobStatistics object and fill with values
        stats = JobStatistics()
        stats["workflow_spec_id"] = job["workflow_spec_id"] 
        stats["job_spec_id"] = job["job_spec_id"]
        stats["exit_code"] = job["exit_code"] 
        stats["status"] = job["status"] 
        stats["site_name"] = job["site_name"] 
        stats["host_name"] = job["host_name"]
        stats["se_name"] = job["se_name"]
        stats["ce_name"] = "Unknown"
        stats["job_type"] = job["job_type"] 
        
        #events missing for failures - set to 0
        stats["events_read"] = job.get("events_read", 0) 
        stats["events_written"] = job.get("events_written", 0) 
        
        #verify job_spec_id exists
        if stats["job_spec_id"] in (None, "", "None", "Unknown"):
            continue
        
        #if workflow_spec_id missing replace get from job_spec_id (first 3 "
        if stats["workflow_spec_id"] in (None, "", "None", "Unknown"):
            stats["workflow_spec_id"] = \
                        "-".join(stats["job_spec_id"].split("-")[0:3])
        
        #get timings (for successful jobs)
        if stats["exit_code"] == 0:
            dbCur1.execute("""SELECT attr_name, attr_value FROM st_job_attr WHERE 
            attr_class = "timing" AND job_index = %s;""" % addQuotes(job_index))
            timings = dbCur1.fetchall()
            for type, value in timings:
                stats["timing"][type] = int(value.tostring())    #is an array for some reason
        
        #get job type
        if stats["job_type"] in (None, "None"):
            if stats["job_spec_id"].find("merge") > 0:
                stats["job_type"] = "Merge"
            else:
                stats["job_type"] = "Processing"
        
        #fake dashboad_id
        stats["dashboard_id"] = str(stats["job_spec_id"] + "-" + \
                                           str(stats["timing"]["AppStartTime"]))
        
        #add errors if a failure
        if stats["exit_code"] != 0:
            dbCur1.execute("""SELECT error_type, error_desc FROM st_job_failure
                         WHERE job_index = %s""" % addQuotes(job_index))
            result = dbCur.fetchone()
            if result != None:
                stats["error_type"], stats["error_desc"] = result
        
        #self.setdefault("task_name", None)
        
        #save jobStatistics object
        print str(stats)
        #time.sleep(.05)
        stats.insertIntoDB()

    dbCur.close()
    dbCur1.close()


def migrateWorkflows(connection):
    """
    Needs at least one successful job for each workflow
    """
    
    dbCur = connection.cursor()
    
    #get workflows from job tables
    dbCur.execute("SELECT DISTINCT workflow_spec_id FROM st_job_success")
    workflows_success = [removeTuple(workflow) for workflow in dbCur.fetchall()]
#    dbCur.execute("SELECT DISTINCT workflow_spec_id FROM st_job_failure WHERE workflow_spec_id != \"None\"")
#    workflows_failure = [removeTuple(workflow) for workflow in dbCur.fetchall()]
#    
#    #get workflows for all failed jobs
#    #    first part of job_spec_id is workflow, strip job number (9 characters long)
#    dbCur.execute("select DISTINCT REPLACE(job_spec_id, RIGHT(job_spec_id, 9), \"\") from st_job_failure")
#    workflows_failure2 = [removeTuple(workflow) for workflow in dbCur.fetchall()]
    
    #add workflows with successful jobs
    for workflow in workflows_success:

        #get output dataset
        dbCur.execute("""SELECT attr_value FROM st_job_attr JOIN st_job_success
                         WHERE st_job_success.job_index = st_job_attr.job_index
                         AND st_job_attr.attr_class = "output_datasets" AND 
                         st_job_success.workflow_spec_id = %s LIMIT 1""" % addQuotes(workflow))
        output_dataset = removeTuple(dbCur.fetchone()).tostring()    #array for some reason
        
        output_datasets = []
        output_datasets.append(output_dataset)
        
        #insert workflow with correct name, fake request_id of 0, 
        #no input datasets and an unknown CMSSW version
        insertNewWorkflow(workflow, 0, (), output_datasets, "Unknown")

#    #for failed don't worry about output_datasets, as long as one job succeeded have info
#    #if not - well...
#    
#    #ignore workflow "None" - when job not linked back to workflow in db
#    for workflow in workflows_failure:
#        if workflow not in (workflows_success):
#            insertNewWorkflow(workflow, 0, (), (), "Unknown")
#        
#    for workflow in workflows_failure2:
#        if workflow not in (workflows_success, workflows_failure):
#            insertNewWorkflow(workflow, 0, (), (), "Unknown")
    
    dbCur.close()
    return


    
if __name__ == "__main__":
    
    #file = sys.argv[1]
    migrate()