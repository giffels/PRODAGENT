#!/usr/bin/env python
"""
_JobQueueDB_

Database API for JobQueue DB Tables

Usage:

Session.set_database(dbConfig)
Session.connect()
Session.start_transaction()

jobQ = JobQueueDB()
jobQ.doDBStuff()
del jobQ

Session.commit_all()
Session.close_all()

"""

import logging

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdCommon.Core.ProdException import ProdException

from ProdAgent.ResourceControl.ResourceControlDB import ResourceControlDB
from ProdAgentCore.Configuration import loadProdAgentConfiguration


class JobQueueDBError(ProdException):
    """
    _JobQueueDBError_

    Exception class for JobQueueDB Errors

    """
    def __init__(self, msg, **data):
        ProdException.__init__(self, msg, 7000, **data)



reduceList = lambda x, y : str(x) + ", " + str(y)


def verifySites():
    """
    _verifySites_

    (Safely) Extract the VerifySites parameter from the JobQueue
    configuration block. Convert this to a True/False value
    and return it, default is True (verify sites against site DB)

    """
    try:
        paConfig = loadProdAgentConfiguration()
    except Exception, ex:
        msg = "Unable to load PA Config: %s\n" % str(ex)
        msg += "VerifySites defaulting to True"
        logging.debug(msg)
        return True
    jobQueueConfig = paConfig.get("JobQueue", {})
    verify = jobQueueConfig.get("VerifySites", True)
    result = True
    if str(verify).lower() in ("false", "no"):
        result = False
    if str(verify).lower() in ("true", "yes"):
        result = True
    return result
    






class JobQueueDB:
    """
    _JobQueueDB_

    Object that provides DB table interface to the JobQueue Tables
    Requires a Session to be established before construction
    
    """
    _VerifySites = verifySites()
    
    def __init__(self):
        self.siteMatchData =  []
        self.siteIndexByName = {}
        self.siteIndexBySE = {}

    def loadSiteMatchData(self):
        """
        _loadSiteMatchData_

        Import details of all sites known by the PA to match
        site index values by site name and se

        """
        resConDB = ResourceControlDB()
        self.siteMatchData = resConDB.siteMatchData()

        [ self.siteIndexByName.__setitem__(x['SiteName'], x['SiteIndex'])
          for x in self.siteMatchData ]

        [ self.siteIndexBySE.__setitem__(x['SEName'], x['SiteIndex'])
          for x in self.siteMatchData ]
        
        return

    def getSiteIndex(self, siteId):
        """
        _getSiteIndex_

        Get the site index for the site id provided, first, site names
        are checked, then se names and the first matching index is returned.

        In the event of no match, None is returned

        """
        siteVal = self.siteIndexByName.get(siteId, None)
        if siteVal != None:
            return siteVal
        seVal = self.siteIndexBySE.get(siteId, None)
        if seVal != None:
            return seVal
        return None

    def validateJobSpecDict(self, dictInstance):
        """
        _validateJobSpecDict_
        
        Check required fields for inserting a job spec into the queue
        are all present in the dictionary provided
        
        """
        reqKeys = [
            "JobSpecId",
            "JobSpecFile",
            "JobType",
            "WorkflowSpecId",
            "WorkflowPriority",
            ]
        for key in reqKeys:
            if dictInstance.get(key, None) == None:
                msg = "Missing field %s required to insert job spec\n" % key
                msg += "Cannot queue job spec without proper information\n"
                raise JobQueueDBError(msg, MissingKey = key,
                                      RequiredKeys = reqKeys)

        if not self._VerifySites:
            return

        if not dictInstance.has_key('SiteList'):
            return
        if len(dictInstance['SiteList']) == 0:
            return
        newSites = set()
        for siteId in dictInstance['SiteList']:
            if siteId == "NULL":
                newSites.add("NULL")
            else:
                siteMatch = self.getSiteIndex(siteId)
                if siteMatch == None:
                    msg = "Unable to match site name for job spec with sites:\n"
                    msg += "%s\n" % siteId
                    raise JobQueueDBError(
                        msg,
                        UnknownSites = siteId,
                        KnownSites = self.siteIndexByName.keys() + \
                        self.siteIndexBySE.keys()
                        )
                else:
                    newSites.add(siteMatch)
        newSiteList = list(newSites)
        dictInstance['SiteList'] = newSiteList
        
        return


    
        
    
    def __directInsertJobSpecsWithSites(self, sitesList, *jobSpecDicts):
        """
        __directInsertJobSpecs_

        Insert entire list of job specs into DB
        Kept private to be called by insertJobSpecs which
        breaks list into manageable chunks

        """
        sqlStr = \
          """INSERT INTO jq_queue (job_spec_id,
                                   job_spec_file,
                                   job_type,
                                   workflow_id,
                                   priority) VALUES
          """
        
        numberOfJobs = len(jobSpecDicts)
        for job in jobSpecDicts:
            sqlStr += """( "%s", "%s", "%s", "%s", %s ) """ % (
                job["JobSpecId"], job['JobSpecFile'],
                job['JobType'], job['WorkflowSpecId'],
                job['WorkflowPriority']
                )
            if job == jobSpecDicts[-1]:
                sqlStr += ";"
            else:
                sqlStr += ",\n"
        

        Session.execute(sqlStr)
        Session.execute("SELECT LAST_INSERT_ID()")
        firstJobIndex = Session.fetchone()[0]
        sqlStr2 = "INSERT INTO jq_site (job_index, site_index) VALUES\n"
        lastJobIndex = firstJobIndex + numberOfJobs
        for jobIndex in range(firstJobIndex, lastJobIndex):
            for siteIndex in sitesList:
                sqlStr2 += " (%s, %s)" % (jobIndex, siteIndex)
                if jobIndex == lastJobIndex -1:
                    if siteIndex == sitesList[-1]:
                        sqlStr2 += ";"
                    else:
                        sqlStr2 += ",\n"
                else:
                    sqlStr2 += ",\n"
                    
        logging.debug(sqlStr2)
        Session.execute(sqlStr2)
        
        return
        
        
    def insertJobSpecsForSites(self, listOfSites, *jobSpecDicts):
        """
        _insertJobSpecsForSites_

        Insert a set of jobSpecs that all have the same sites.
        This is more efficient if the list of sites is the same
        for all jobs.

        Must have keys:
        "JobSpecId"
        "JobSpecFile"
        "JobType"
        "WorkflowSpecId"
        "WorkflowPriority"

        """
        jobSpecDicts = list(jobSpecDicts)
        sitesList = [ self.getSiteIndex(x) for x in listOfSites]
        
        _INSERTLIMIT = 2000

        if sitesList == []:
            sitesList.append("NULL")
        while len(jobSpecDicts) > 0:
            segment = jobSpecDicts[0:_INSERTLIMIT]
            jobSpecDicts = jobSpecDicts[_INSERTLIMIT:]
            map(self.validateJobSpecDict, segment)
            self.__directInsertJobSpecsWithSites(sitesList, *segment)
            
        return

        
        
    
    def insertJobSpec(self, jobSpecId, jobSpecFile, jobType, workflowId,
                      workflowPriority, sitesList):
        """
        _insertJobSpecs_

        Insert a single job spec entry with a list of sites.
                
        """
        jobSpecDict = {
            "JobSpecId" : jobSpecId,
            "JobSpecFile": jobSpecFile,
            "JobType": jobType,
            "WorkflowSpecId": workflowId,
            "WorkflowPriority": workflowPriority,
            "SiteList": sitesList

            }
        
        self.validateJobSpecDict(jobSpecDict)

        
        sqlStr = \
        """INSERT INTO jq_queue (job_spec_id,
                                 job_spec_file,
                                 job_type,
                                 workflow_id,
                                 priority)
        VALUES (  "%s", "%s", "%s", "%s", %s ) """ % (
        jobSpecDict["JobSpecId"], jobSpecDict['JobSpecFile'],
        jobSpecDict['JobType'], jobSpecDict['WorkflowSpecId'],
        jobSpecDict['WorkflowPriority']
        )
        dbCur = Session.get_cursor()
        dbCur.execute(sqlStr)
        dbCur.execute("SELECT LAST_INSERT_ID()")
        jobIndex = dbCur.fetchone()[0]
        
        if len(jobSpecDict['SiteList']) == 0:
            jobSpecDict['SiteList'].append("NULL")
        sqlStr2 = "INSERT INTO jq_site (job_index, site_index) VALUES "
        for siteIndex in jobSpecDict['SiteList']:
            sqlStr2 += " (%s, %s)," % (jobIndex, siteIndex)
        sqlStr2 = sqlStr2[:-1]
        sqlStr2 += ";"
        dbCur.execute(sqlStr2)
        
        return






    def retrieveJobsAtSites(self, count = 1, jobType = None,
                            workflow = None, *sites):
        """
        _retrieveJobsAtSites_

        Get a list of size count matching job indexes from the DB tables
        matched by:

        optional workflow id
        optional job type
        required list of site index values.
        
        """
        sqlStr = \
        """
        SELECT DISTINCT jobQ.job_index FROM jq_queue jobQ LEFT OUTER JOIN
        jq_site siteQ ON jobQ.job_index = siteQ.job_index WHERE status = 'new'

        """
        
        if workflow != None:
            sqlStr +=" AND workflow_id=\"%s\" " % workflow

        if jobType != None:
            sqlStr +=  " AND job_type=\"%s\" " % jobType
                
        sqlStr += " AND "


        if len(sites) > 0:
            siteStr = ""
            for s in sites:
                siteStr += "%s," % s
            siteStr = siteStr[:-1]
            
            sqlStr += " ( siteQ.site_index IN (%s) " % siteStr
            sqlStr += " OR siteQ.site_index IS NULL ) "
        else:
            sqlStr += " siteQ.site_index IS NULL "
        
        sqlStr += " ORDER BY priority DESC, time DESC LIMIT %s;" % count
        
        Session.execute(sqlStr)
        result = Session.fetchall()
        result = [ x[0] for x in result ]
        
        return result



    def retrieveJobs(self, count = 1, jobType = None,
                     workflow = None):
        """
        _retrieveJobs_

        Retrieve Jobs without specifying site information

        """
        sqlStr = \
        """
        SELECT DISTINCT job_index FROM jq_queue WHERE status = 'new' 
        """

        if workflow != None:
            sqlStr +=" AND workflow_id=\"%s\" " % workflow

        if jobType != None:
            sqlStr +=  " AND job_type=\"%s\" " % jobType
            
        sqlStr += " ORDER BY priority DESC, time DESC LIMIT %s;" % count
        Session.execute(sqlStr)
        result = Session.fetchall()
        result = [ x[0] for x in result ]
        
        return result
                

    def countJobsForSite(self, siteIndex):
        """
        _countJobsForSite_

        Return a count of the number of jobs for a given site index

        """
        sqlStr = \
        """
        SELECT DISTINCT COUNT(jobQ.job_index) FROM jq_queue jobQ
         LEFT OUTER JOIN jq_site siteQ
          ON jobQ.job_index = siteQ.job_index WHERE status = 'new'
           AND siteQ.site_index = %s """ % siteIndex 
        
        Session.execute(sqlStr)
        result = Session.fetchone()[0]
        return int(result)

    def countJobsForWorkflow(self, workflow, jobType = None):
        """
        _countJobsForWorkflow_

        Return total job count for a given workflow, optionally
        counts by type if provided.

        """
        sqlStr = \
        """
        SELECT DISTINCT COUNT(job_index) FROM jq_queue WHERE
          workflow_id = \"%s\" AND status = 'new' """ % workflow

        if jobType != None:
            sqlStr += " AND job_type=\"%s\" " % jobType
        sqlStr += ";"
        Session.execute(sqlStr)
        result = Session.fetchone()[0]
        return int(result)


    def retrieveJobDetails(self, *indices):
        """
        _retrieveJobDetails_

        Extract a list of job details matching the job_index provided
        and return the details as a list of dictionaries

        """
        sqlStr = \
        """
        SELECT job_index, job_spec_id, job_spec_file,
               job_type, workflow_id FROM jq_queue WHERE job_index IN 
        """
        if len(indices) == 0:
            return {}
        sqlStr += " ( "
        sqlStr += str(reduce(reduceList, indices))
        sqlStr += " );"
        Session.execute(sqlStr)

        jobs = Session.fetchall()

        result = []

        [ result.append({
            'JobIndex' : x[0],
            'JobSpecId' : x[1],
            'JobSpecFile' : x[2],
            'JobType' : x[3],
            'WorkflowSpecId' : x[4],
            }) for x in jobs ]
        
        return result
        
        
        
    def flagAsReleased(self, *indices):
        """
        _flagAsReleased_

        For the job indices in the list provided, flag the jobs as
        released status
        """
        sqlStr = \
        """
        UPDATE  jq_queue SET status = 'released', time = NOW() WHERE job_index
          IN 
        """
        if len(indices) == 0:
            return
        sqlStr += " ( "
        sqlStr += str(reduce(reduceList, indices))
        sqlStr += " );"
        Session.execute(sqlStr)
        return

    

    def cleanOut(self, timeInterval):
        """
        _cleanOut_

        Clean out all released status jobs that have existed for a time
        longer that the time interval provided.

        time format is a "00:00:00" type string
        
        """
        sqlStr = \
        """
        DELETE FROM jq_queue WHERE status = 'released' 
          AND time < ADDTIME(CURRENT_TIMESTAMP,'-%s')
        """ % timeInterval
        Session.execute(sqlStr)
        return
        
        
