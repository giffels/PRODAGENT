#!/usr/bin/env python
"""
_JobEmulatorDB_

Database API for JobEmulator DB Tables

Usage:

Session.set_database(dbConfig)
Session.connect()
Session.start_transaction()

jobEm = JobEmulatorDB()
jobEm.doDBStuff()
del jobEm

Session.commit_all()
Session.close_all()

"""
__revision__ = "$Id: $"
__version__ = "$Revision: $"

from ProdCommon.Database import Session
from ProdCommon.Core.ProdException import ProdException

class JobEmulatorDBError(ProdException):
    """
    _JobEmulatorDBError_

    Exception class for JobEmulatorDB Errors

    """
    def __init__(self, msg, **data):
        # need to find right error code
        ProdException.__init__(self, msg, 9000, **data)

class JobEmulatorDB:
    """
    _JobEmulatorDB_

    Object that provides DB table interface to the JobEmulator Tables
    Requires a Session to be established before construction
    
    """
    
    def __init__(self):
        pass

    @staticmethod    
    def insertJob(jobID, jobType):
        """
        _insertJob_

        Add a job the the job emulator for processing given a unique ID
        and a type (processing, merge or cleanup).  That status and
        start time will be added automatically.
                
        """
        
	    # uses the default value CURRENT_TIMESTAMP for time information
        sqlStr = \
        """INSERT INTO job_emulator (job_id, job_type, start_time, status)
        VALUES (  "%s", "%s", DEFAULT, "%s" ) """ % (
        jobID, jobType, "new")
	
        Session.execute(sqlStr)
        
        return


    @staticmethod    
    def queryJobsByStatus(status):
        """
        _queryJobsByStatus_

        Returns a list of jobs in the Job Emulator database that
        have a particular status.  Each list item consists of the
        following tuple:

        (Job ID, Job Type (processing, merge or cleanup), Job Start Time,
        Job Status (new, finished, failed))

        """
	
        sqlStr = \
        """
        SELECT * FROM job_emulator WHERE status = '%s' order by start_time

        """ % status
        
        Session.execute(sqlStr)
        result = Session.fetchall()
        result = [ (x[0], x[1], x[2], x[3], x[4]) for x in result ]
        
        return result

    @staticmethod    
    def queryNodeInfoByNodeID(hostID):
        """
        _queryNodeInfoByNodeID_

        Returns a node information. The item consists of the
        following tuple:

        (host_id, host_name, number_jobs)

        """
    
        sqlStr = \
        """
        SELECT * FROM jobEM_node_info WHERE host_id = %d

        """ % hostID
        
        Session.execute(sqlStr)
        result = Session.fetchone()
        #result = [ (x[0], x[1], x[2], x[3], x[4]) for x in result ]
        
        return result
    
    @staticmethod    
    def queryJobsByID(jobID):
        """
        _queryJobsByID_

        Returns a list of jobs in the Job Emulator database that
        have a particular job ID.  Each list item consists of the
        following tuple:

        (Job ID, Job Type (processing, merge or cleanup), Job Start Time,
        Job Status (new, finished, failed))
        
        """
	
        sqlStr = \
        """
        SELECT * FROM job_emulator WHERE job_id = '%s' order by start_time

        """ % jobID
        
        Session.execute(sqlStr)
        result = Session.fetchall()
        result = [ (x[0], x[1], x[2], x[3], x[4]) for x in result ]
        
        return result    
	
    @staticmethod    
    def removeJob(jobID):
        """
        _removeJob_

        Remove any job from the job_emulator table that has a
        particular job ID.

        """
	
        sqlStr = \
        """
        DELETE FROM job_emulator WHERE job_id = '%s' 

        """ % jobID
        
        Session.execute(sqlStr)
        
        return
	
	
    @staticmethod    
    def updateJobStatus(jobID, status):
        """
        _updateJobStatus_

        Change the status of a job with a particular job ID. Status
        can be either new, finished or failed.

        """
	
        sqlStr = \
        """
        UPDATE job_emulator SET status = '%s' WHERE job_id = '%s'

        """ % (status, jobID)
        
        Session.execute(sqlStr)
        
        return
    
    @staticmethod    
    def updateJobAlloction(jobID, nodeID):
        """
        _updateJobAlloction_

        Change the status of a job with a particular job ID. Status
        can be either new, finished or failed.

        """
    
        sqlStr = \
        """
        UPDATE job_emulator SET worker_node_id= %d WHERE job_id = '%s'

        """ % (nodeID, jobID)
        
        Session.execute(sqlStr)
        
        return

    @staticmethod
    def deleteTable(tableName):
        """
        _deleteWorkerNodeTable_

        Add a job the the job emulator for processing given a unique ID
        and a type (processing, merge or cleanup).  That status and
        start time will be added automatically.
                
        """
        sqlStr = " DELETE from %s" % tableName
        Session.execute(sqlStr)
        return
    
    @staticmethod
    def insertWorkerNode(nodeName, jobCount=0):    
        """
        _insertWorkerNode_
        
        """
        sqlStr = \
        """
        INSERT INTO jobEM_node_info (host_name, number_jobs)
        VALUES ('%s', %d)
        """ % (nodeName, jobCount)
    
        Session.execute(sqlStr)
        
        return
    
    @staticmethod
    def increaseJobCount(jobID):
        """
        _increaseJobCount_
        
        """
        sqlStr = \
        """
        UPDATE jobEM_node_info SET number_jobs = number_jobs + 1 
        WHERE host_id = 
        (SELECT worker_node_id FROM job_emulator WHERE job_id = '%s')
        """ % jobID
        
        Session.execute(sqlStr)
        return
    
    @staticmethod
    def increaseJobCountByNodeID(nodeID):
        """
        _increaseJobCountByNodeID_
        
        """
        sqlStr = \
        """
        UPDATE jobEM_node_info SET number_jobs = number_jobs + 1 
        WHERE host_id = %d
        """% nodeID
        
        Session.execute(sqlStr)
        return
    
    @staticmethod
    def decreaseJobCount(jobID):
        """
        _decreaseJobCount_
        
        """
        sqlStr = \
        """
        UPDATE jobEM_node_info SET number_jobs = number_jobs - 1
        WHERE host_id = 
        (SELECT worker_node_id FROM job_emulator WHERE job_id = '%s')
        """ % jobID
        
        Session.execute(sqlStr)
        return
    
    @staticmethod
    def decreaseJobCountByNodeID(nodeID):
        """
        _increaseJobCountByNodeID_
        
        """
        sqlStr = \
        """
        UPDATE jobEM_node_info SET number_jobs = number_jobs - 1
        WHERE host_id = %d
        """ % nodeID
        
        Session.execute(sqlStr)
        return
    
    @staticmethod    
    def selecOneNodeWithLeastJob():
        """
        _selecOneNodeWithLeastJob_

        Returns a list of jobs in the Job Emulator database that
        have a particular job ID.  Each list item consists of the
        following tuple:

        (Job ID, Job Type (processing, merge or cleanup), Job Start Time,
        Job Status (new, finished, failed))
        
        """
    
        sqlStr = \
        """
        SELECT * FROM jobEM_node_info order by number_jobs ASC, host_id

        """ 
        
        Session.execute(sqlStr)
        result = Session.fetchone()
        
        return result    
    
    
    @staticmethod    
    def selecAllNodes():
        """
        _selecOneNodeWithLeastJob_

        Returns a list of jobs in the Job Emulator database that
        have a particular job ID.  Each list item consists of the
        following tuple:

        (Job ID, Job Type (processing, merge or cleanup), Job Start Time,
        Job Status (new, finished, failed))
        
        """
    
        sqlStr = \
        """
        SELECT * FROM jobEM_node_info

        """ 
        
        Session.execute(sqlStr)
        result = Session.fetchall()
        
        return result 
    
    @staticmethod
    def dropTable(tableName):
        """
        _dropTable_

        Add a job the the job emulator for processing given a unique ID
        and a type (processing, merge or cleanup).  That status and
        start time will be added automatically.
                
        """
        sqlStr = "drop table %s" % tableName 
        Session.execute(sqlStr)
        return
    
    @staticmethod
    def createNodeTable():
        """
        _createNodeTable_
        """
        sqlStr = \
        """  
        CREATE TABLE jobEM_node_info (
            host_id INT NOT NULL AUTO_INCREMENT,
            host_name VARCHAR(255) NOT NULL,
            number_jobs SMALLINT UNSIGNED,
            UNIQUE (host_name),
            PRIMARY KEY (host_id)
        )
        """                               
        Session.execute(sqlStr)
        return
    
    @staticmethod
    def createEmulatorTable():
        """
        _createEmulatorTable_
        """
        sqlStr = \
        """  
        CREATE TABLE job_emulator (
            job_id  VARCHAR(255) NOT NULL,
            job_type ENUM('Processing', 'Merge', 'CleanUp'),
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status ENUM('new', 'assigned', 'finished', 'failed'),
            worker_node_id INT,
            FOREIGN KEY (worker_node_id) REFERENCES jobEM_node_info (host_id),
            PRIMARY KEY (job_id)
        )
        """                               
        Session.execute(sqlStr)
        return