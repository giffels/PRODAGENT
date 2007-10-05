#!/usr/bin/env python
"""
    Script to migrate monitoring information from StatTracker
    
    Warning: Jobs are not inserted in a transaction therefore duplicates
        are possible on multiple invocation
            Each job uses a transaction and MySQL forbids nested transcations
"""

from ProdMon.JobStatistics import JobStatistics
from ProdMon.ProdMonDB import insertNewWorkflow, removeTuple, addQuotes
import MySQLdb
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig
#for dict cursor
from ProdAgentDB.Connect import connect

import os, sys, time, getopt, re

db_id = "migrate"

def migrate():
    """
    Function to migrate data from StatTracker to ProdMon
    """
    
    print "Connecting to database"
    Session.set_database(dbConfig)
    Session.connect(sessionID=db_id)
    #Session.start_transaction()

    print "Make way for new data"
    wipeDB()

    print "Load StatTracker data..."
    loadStatTrackerDB()

    print "Migrating workflows..."
    migrateWorkflows()
    
    print "Migrating jobs..."
    print "You may ignore the database warnings... (probably)"
    migrateJobs()
    
    #Session.execute("COMMIT")
    Session.commit_all()
    Session.close_all()
    print "Migration Successful"
    
    return


def wipeDB():
    """
    Wipe ProdMon tables
    """
    
    tables = """prodmon_Datasets prodmon_Job prodmon_Job_errors 
    prodmon_Job_instance prodmon_Job_timing prodmon_LFN prodmon_Resource
    prodmon_Workflow prodmon_input_LFN_map prodmon_input_datasets_map 
    prodmon_output_LFN_map prodmon_output_datasets_map prodmon_output_runs
    prodmon_skipped_events"""
    
    for table in tables.split():
        Session.execute("DELETE FROM %s" % table, sessionID=db_id)
        #Session.commit()    
    return


def loadStatTrackerDB():
    """
    Load StatTracker tables into DB
     NB. Hardcoded to use a socket
     
     Also need to give prodAgentUser privileges (use appropriae values) with
         GRANT ALL ON devWork.* TO "ProdAgentUser"@"localhost";
         FLUSH PRIVILEGES;     
    """
    
    command = "mysql -u%s -p%s --socket=%s %s < %s" % (dbConfig["user"], \
                                            dbConfig["passwd"], \
                                            dbConfig["socketFileLocation"], \
                                            dbConfig["dbName"], \
                                            input)
    
    result = os.system(command)
    if (result):
        raise RuntimeError, "Error loading StatTracker data from %s" % str(file)
    return
    
#    Session.start_transaction()
#    
#    #get StatTracker sql and import
#    file = open(input, "r")
#    while file:
#        sql = file.readline()
#        if sql != "":
#            Session.execute(sql)
#    file.close()
#    
#    Session.commit()
    



def migrateJobs():
    
    connection = connect()
    dbCur = connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    
    #get list of known workflows
    Session.execute("SELECT DISTINCT workflow_name FROM prodmon_Workflow", sessionID=db_id)
    workflows = [removeTuple(workflow) for workflow in Session.fetchall(sessionID=db_id)]
    
    dbCur.execute("""SELECT workflow_spec_id, job_spec_id, exit_code, status, site_name,
               host_name, se_name, events_read, events_written, job_type, job_index, time FROM st_job_success""")
    jobs = dbCur.fetchall()
    dbCur.execute("""SELECT workflow_spec_id, job_spec_id, exit_code, status, site_name,
               host_name, se_name, job_type, job_index, time FROM st_job_failure""")
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
        stats["insert_time"] = job["time"]
        
        #events missing for failures - set to 0
        stats["events_read"] = job.get("events_read", 0) 
        stats["events_written"] = job.get("events_written", 0) 
        
        #verify job_spec_id exists
        if stats["job_spec_id"] in (None, "", "None", "Unknown"):
            continue
        
        #if workflow_spec_id missing replace from job_spec_id
        if stats["workflow_spec_id"] in (None, "", "None", "Unknown"):
            
            #for merges remove sename-job-id
            if stats["job_spec_id"].find("mergejob") > 0:
                stats["workflow_spec_id"] = \
                        "-".join(stats["job_spec_id"].split("-")[:-3])
                
                #handle node names that contain a - themselves
                if stats["workflow_spec_id"].split("-")[-1].isalpha():
                    stats["workflow_spec_id"] = \
                        "-".join(stats["workflow_spec_id"].split("-")[:-1])
            else:
                #for normal jobs remove last -id field
                stats["workflow_spec_id"] = \
                        "-".join(stats["job_spec_id"].split("-")[:-1])
        
        #skip if from unknown workflow
        if stats["workflow_spec_id"] not in workflows:
            print "Skipping %s, workflow %s unknown" % (stats["job_spec_id"], \
                                                        stats["workflow_spec_id"])
            continue
        
        #get timings (for successful jobs)
        if stats["exit_code"] == 0:
            Session.execute("""SELECT attr_name, attr_value FROM st_job_attr WHERE 
            attr_class = "timing" AND job_index = %s;""" % addQuotes(job_index), sessionID=db_id)
            timings = Session.fetchall()
            for type, value in timings:
                stats["timing"][type] = int(value.tostring())    #an array for some reason
        
        #get job type
        if stats["job_type"] in (None, "None"):
            if stats["job_spec_id"].find("merge") > 0:
                stats["job_type"] = "Merge"
            else:
                stats["job_type"] = "Processing"
        
        #fake dashboard_id
        #leave to be handled in exportToDashboard()
        
        #add errors if a failure
        if stats["exit_code"] != 0:
            Session.execute("""SELECT error_type, error_desc FROM st_job_failure
                         WHERE job_index = %s""" % addQuotes(job_index), sessionID=db_id)
            result = Session.fetchone(sessionID=db_id)
            if result != None and result[0] != None and result[1] != None:
                stats["error_type"] = result[0]
                stats["error_desc"] = result[1].tostring()
        
        #self.setdefault("task_name", None)
        
        #save jobStatistics object
        #print str(stats)

        stats.insertIntoDB()
        
    dbCur.close()


def migrateWorkflows():
    """
    Needs at least one successful job for each workflow
    """
    
    #get workflows from job tables
    Session.execute("SELECT DISTINCT workflow_spec_id FROM st_job_success", sessionID=db_id)
    workflows_success = [removeTuple(workflow) for workflow in Session.fetchall(sessionID=db_id)]
#    Session.execute("SELECT DISTINCT workflow_spec_id FROM st_job_failure WHERE workflow_spec_id != \"None\"")
#    workflows_failure = [removeTuple(workflow) for workflow in Session.fetchall()]
#    
#    #get workflows for all failed jobs
#    #    first part of job_spec_id is workflow, strip job number (9 characters long)
#    Session.execute("select DISTINCT REPLACE(job_spec_id, RIGHT(job_spec_id, 9), \"\") from st_job_failure")
#    workflows_failure2 = [removeTuple(workflow) for workflow in Session.fetchall()]
    
    #add workflows with successful jobs
    for workflow in workflows_success:

        #get output dataset
        Session.execute("""SELECT DISTINCT attr_value FROM st_job_attr JOIN st_job_success
                         WHERE st_job_success.job_index = st_job_attr.job_index
                         AND st_job_attr.attr_class = "output_datasets" AND 
                         st_job_success.workflow_spec_id = %s""" % addQuotes(workflow), sessionID=db_id)
        output_datasets = Session.fetchall(sessionID=db_id)
        
        #change to DBS2 dataset format
        for dataset in output_datasets:
            dataset = removeTuple(dataset).tostring()    #array for some reason
            dataset = re.sub('(.*)/(.*)/(.*CMSSW.*)', '\g<1>/\g<3>/\g<2>' , dataset)
    
        #insert workflow with fake request_id of 0, 
        #no input datasets and an unknown CMSSW version
        insertNewWorkflow(workflow, 0, (), output_datasets, "Unknown")

    #for failed don't worry about output_datasets, as long as one job succeeded have info
    #if not - well...
  
#    #ignore workflow "None" - when job not linked back to workflow in db
#    for workflow in workflows_failure:
#        if workflow not in (workflows_success):
#            insertNewWorkflow(workflow, 0, (), (), "Unknown")
#        
#    for workflow in workflows_failure2:
#        if workflow not in (workflows_success, workflows_failure):
#            insertNewWorkflow(workflow, 0, (), (), "Unknown")
    
    return

def usage():
    print "Usage: migrateStatTracker.py --input=<inpout_file>"
    return
    
if __name__ == "__main__":
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:", ["help", "input="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    
    #initial args
    input = None
    
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        if o in ("--input"):
            input = a
    
    if input == None:
        usage()
        sys.exit(2)
        
    
    #now do the migration    
    migrate()