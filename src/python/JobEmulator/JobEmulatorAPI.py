#!/usr/bin/env python
"""
_JobEmulatorAPI_

Public API for interfacing with JobEmulator. 

"""

__revision__ = "$Id: "
__version__ = "$Revision: "

from random import randrange
import logging

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdAgent.ResourceControl.ResourceControlDB import ResourceControlDB
from JobEmulator.JobEmulatorDB import JobEmulatorDB
from JobEmulator.WorkerNodeInfo import WorkerNodeInfo

Session.set_database(dbConfig)

def dbExceptionHandler(dbfunc):
    """
    _dbExceptionHandler_
    """
    def wrapperFuction(*args, **dictArgs):
        """
        _wrapperFuction_
        """
        try:
            Session.connect()
            Session.start_transaction()
            reValue = dbfunc(*args, **dictArgs)
            Session.commit_all()
            Session.close_all()
            return reValue 
            

        except Exception, ex:
            msg = "Error: %s\n" % str(ex)
            logging.error(msg)
            Session.rollback()
            Session.close_all()
    
    return wrapperFuction

@dbExceptionHandler
def addJob(jobID, jobType):
    """
    _addJob_

    Add a job the the job emulator for processing given a unique ID
    and a type (processing, merge or cleanup).  That status and
    start time will be added automatically.
        
    """
    JobEmulatorDB.insertJob(jobID, jobType)
    return

@dbExceptionHandler
def queryJobsByStatus(status):
    """
    _queryJobsByStatus_

    Returns a list of jobs in the Job Emulator database that
    have a particular status.  Each list item consists of the
    following tuple:

    (Job ID, Job Type (processing, merge or cleanup), Job Start Time,
    Job Status (new, finished, failed))

    """
    return JobEmulatorDB.queryJobsByStatus(status)

@dbExceptionHandler
def queryJobsByID(jobID):
    """
    _queryJobsByID_

    Returns a list of jobs in the Job Emulator database that
    have a particular job ID.  Each list item consists of the
    following tuple:

    (Job ID, Job Type (processing, merge or cleanup), Job Start Time,
    Job Status (new, finished, failed))

    """
    return JobEmulatorDB.queryJobsByID(jobID)


@dbExceptionHandler
def removeJob(jobID):
    """
    _removeJob_

    Remove any job from the job_emulator table that has a
    particular job ID.
    
    """
    JobEmulatorDB.removeJob(jobID)
    return

@dbExceptionHandler
def updateJobStatus(jobID, status):
    """
    _updateJobStatus_

    Change the status of a job with a particular job ID. Status
    can be either new, finished or failed.
    
    """
    JobEmulatorDB.updateJobStatus(jobID, status)
    return

@dbExceptionHandler
def assignJobToNode(jobID, nodeID):
    """
    _assignJobToNode_

    Assign the node id where a job with a particular job ID is running. 
    
    """
    JobEmulatorDB.updateJobAlloction(jobID, nodeID)
    JobEmulatorDB.increaseJobCountByNodeID(nodeID)
    return

@dbExceptionHandler
def initializeJobEM_DB():
    """
    _initializeJobEM_DB_
    Initialize database for job emulator. clean up job_emulator,
    jobEM_node_info table. and insert node names 
    """
    #JobEmulatorDB.deleteTable("job_emulator")
    #JobEmulatorDB.deleteTable("jobEM_node_info")
    try:
        JobEmulatorDB.dropTable("job_emulator")
    except Exception, ex:
        msg = "Warning: %s : new database will be created\n" % str(ex)
        logging.warning(msg)
    
    JobEmulatorDB.createEmulatorTable()
    
    try:
        JobEmulatorDB.dropTable("jobEM_node_info")
    except Exception, ex:
        msg = "Warning: %s : new database will be created\n" % str(ex)
        logging.warning(msg)
    
    JobEmulatorDB.createNodeTable()
    rsControlDB = ResourceControlDB()
    siteNames = rsControlDB.siteNames()
    for siteName in siteNames:
        for num in range(50):
            JobEmulatorDB.insertWorkerNode("fakeHost_%d.%s.FAKE" 
                                           % (num, siteName))
    
@dbExceptionHandler
def increaseJobCountAtNode(jobID):
    """
    _increaseJobCountAtNode_
    """
    JobEmulatorDB.increaseJobCount(jobID)
    return

@dbExceptionHandler
def decreaseJobCountAtNode(jobID):
    """
    _decreaseJobCountAtNode_
    """
    JobEmulatorDB.decreaseJobCount(jobID)
    return

@dbExceptionHandler
def getWorkerNodeInfo(hostID):
    """
    _ getRandomSiteAndNode_
    """
    node = JobEmulatorDB.queryNodeInfoByNodeID(hostID)
    return getSiteInfoFromNode(node)

@dbExceptionHandler
def getRandomSiteAndNode():
    """
    _ getRandomSiteAndNode_
    """
    allNodes = JobEmulatorDB.selecAllNodes()
    node = allNodes[randrange(0, len(allNodes))]
    return getSiteInfoFromNode(node)
    

@dbExceptionHandler
def getLessBusySiteAndNode():
    """
    _getLessBusySiteAndNode_
    """
    oneNode = JobEmulatorDB.selecOneNodeWithLeastJob()
    return getSiteInfoFromNode(oneNode)


@dbExceptionHandler  
def getSiteInfoFromNode(node):
    """
    _getSiteInfoFromNodeName_
    """
    nodeName = node[1]
    rsControlDB = ResourceControlDB()
    siteNames = rsControlDB.siteNames()
        
    nodeInfo = WorkerNodeInfo()
    
    for siteName in siteNames:
        if nodeName.endswith(".%s.FAKE" % siteName):
            siteData = rsControlDB.getSiteData(siteName)
            nodeInfo["SiteName"] = siteName
            nodeInfo["se-name"] = siteData['SEName']
            nodeInfo["ce-name"] = siteData['CEName']
    
            break 
    
    nodeInfo["HostID"] = node[0]
    nodeInfo["HostName"] = nodeName
    
    return nodeInfo


if __name__ == '__main__':
    initializeJobEM_DB()
    addJob('100', 'Processing')
    addJob('101', 'Merge')
    addJob('102', 'CleanUp')

    print queryJobsByStatus('new')

    updateJobStatus('100', "finished")
    
    #removeJob('100')
    #removeJob('101')
    #removeJob('102')
    
    #updateJobStatus('100', "finished")
    
    for jobId in ['100', '101', '102']:
        nodeInfo =  getRandomSiteAndNode()
        assignJobToNode(jobId, nodeInfo['HostID'])
        print nodeInfo

    for jobId in ['100', '101', '102']:
        nodeInfo =  getLessBusySiteAndNode()
        print nodeInfo
        assignJobToNode(jobId, nodeInfo['HostID'])

    decreaseJobCountAtNode('100')