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
from ProdAgent.WorkflowEntities.Workflow import associateSiteToWorkflow

from ProdAgent.ResourceControl import ResourceControlAPI

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
        
        Deprecated -to be removed in a later release

        Import details of all sites known by the PA to match
        site index values by site name and se

        """
        logging.error("""JobQueueDB::loadSiteMatchData called - this function is deprecated
        Please remove this function call""")
#        resConDB = ResourceControlDB()
#        self.siteMatchData = resConDB.siteMatchData()
#
#        [ self.siteIndexByName.__setitem__(x['SiteName'], x['SiteIndex'])
#          for x in self.siteMatchData ]
#
#        [ self.siteIndexBySE.__setitem__(x['SEName'], x['SiteIndex'])
#          for x in self.siteMatchData ]
#        
#        return

    def getSiteIndex(self, siteId):
        """
        _getSiteIndex_

        Get the site indices for the site id provided, first, site names
        are checked, then se names and all matching indices are returned.

        In the event of no match, [] is returned

        """
        sites = []
        allSites = ResourceControlAPI.allSiteData()

        # first search by name
        for site in allSites:
         if site["SiteName"] == siteId:
           sites.append(site["SiteIndex"])

        # if not found try SE
        if len(sites) == 0:
          for site in allSites:
            if site["SEName"] == siteId:    
              sites.append(site["SiteIndex"])

        return sites
        

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
            dictInstance['SiteList'] = ['NULL']
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
                if len(siteMatch) == 0:
                    msg = "Unable to match site name for job spec with sites:\n"
                    msg += "%s\n" % siteId
                    allSiteData = ResourceControlAPI.allSiteData()
                    raise JobQueueDBError(
                        msg,
                        UnknownSites = siteId,
                        KnownSites = [ x["SiteName"] for x in allSiteData] + \
                        [ x["SEName"] for x in allSiteData]
                        )
                else:
                    newSites.update(siteMatch)
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
        sites = set()
        for siteId in listOfSites:
             if siteId == "NULL":
                sites.add("NULL")
             else:
                 siteMatch = self.getSiteIndex(siteId)
                 logging.debug("siteMatch = %s" % siteMatch)
                 [sites.add(x) for x in siteMatch]
        sitesList = list(sites)
        
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
        
        sqlStr += " ORDER BY priority DESC, time LIMIT %s;" % count
        
        Session.execute(sqlStr)
        result = Session.fetchall()
        result = [ x[0] for x in result ]
        
        return result


    def queueLength(self, jobType = None):
        """
        _queueLength_

        Return the total number of pending jobs of the type provided.
        If type is not set, then all types are included
        
        """
        
        sqlStr = \
        """
        SELECT COUNT(job_index) FROM jq_queue WHERE status = 'new'

        """
        if jobType != None:
            sqlStr +=  " AND job_type=\"%s\" " % jobType
        sqlStr += ";"
        Session.execute(sqlStr)
        result = Session.fetchone()
        return int(result[0])

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
            
        sqlStr += " ORDER BY priority DESC, time LIMIT %s;" % count
        Session.execute(sqlStr)
        result = Session.fetchall()
        result = [ x[0] for x in result ]
        
        return result

    def retrieveReleasedJobsAtSites(self, count = 1, jobType = None,
                                    workflow = None, *sites):
        """
        _retrieveReleasedJobsAtSites_

        Get a list of size count matching job indexes from the DB tables
        matched by:

        optional workflow id
        optional job type
        required list of site index values.

        that have been released from the DB.

        This is a history method for job queue
        
        """
        sqlStr = \
        """
        SELECT DISTINCT jobQ.job_index FROM jq_queue jobQ LEFT OUTER JOIN
        jq_site siteQ ON jobQ.job_index = siteQ.job_index WHERE status = 'released'

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



    def retrieveReleasedJobs(self, count = 1, jobType = None,
                             workflow = None):
        """
        _retrieveReleasedJobs_

        Retrieve released Jobs without specifying site information

        This is a history method for job queue

        """
        sqlStr = \
        """
        SELECT DISTINCT job_index FROM jq_queue WHERE status = 'released' 
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
        indexString = str(reduce(reduceList, indices))
        sqlStr += " ( "
        sqlStr += indexString
        sqlStr += " )"
        sqlStr += " ORDER BY FIELD(job_index, "
        sqlStr += indexString
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
        
        
        
#    def flagAsReleased(self, *indices):
#        """
#        _flagAsReleased_
#
#        For the job indices in the list provided, flag the jobs as
#        released status
#        """
#        sqlStr = \
#        """
#        UPDATE  jq_queue SET status = 'released', time = NOW() WHERE job_index
#          IN 
#        """
#        
#        sqlStr += " ( "
#        sqlStr += str(reduce(reduceList, indices))
#        sqlStr += " );"
#        Session.execute(sqlStr)
#        return


    def flagAsReleased(self, siteIndex = None, *indices):
        """
        flag jobs as released at a site
        """
        if len(indices) == 0:
            return
        
        sqlStr = """UPDATE jq_queue SET status = 'released', time = NOW()"""
        
        if siteIndex is not None:
            sqlStr = """%s, released_site = %s""" % (sqlStr, siteIndex)
        
        sqlStr = """%s WHERE job_index IN ( %s )""" % \
                        (sqlStr, str(reduce(reduceList, indices)))
        
        Session.execute(sqlStr)
        
#        if siteIndex is not None:
#            sqlStr = """UPDATE jq_queue SET released_to = %s WHERE job_index
#             IN ( %s )""" % (siteIndex, str(reduce(reduceList, indices)))
#            Session.execute(sqlStr)
        
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


    def removeWorkflow(self, workflowSpecId):
        """
        _removeWorkflow_

        Remove all jobs queued and released for the workflow spec id
        provided

        """
        sqlStr = "delete from jq_queue where workflow_id=\"%s\";" % (
            workflowSpecId,)
        Session.execute(sqlStr)
        return
        
    def retrieveJobsAtSitesNotWorkflow(self, count = 1, jobType = None,
                                       notWFL = None, *sites):
        """
        _retrieveJobsAtSitesNotWorkflow_
        
        Get a list of size count matching job indexes from the DB tables
        matched by:
        
        optional not matching workflow id
        optional job type
        required list of site index values.
        
        """
        sqlStr = \
               """
               SELECT DISTINCT jobQ.job_index FROM jq_queue jobQ LEFT OUTER JOIN
               jq_site siteQ ON jobQ.job_index = siteQ.job_index WHERE status =
               'new'
               
               """

        sqlStr = \
               """
               SELECT DISTINCT jobQ.job_index
               FROM jq_queue jobQ LEFT OUTER JOIN (
                 jq_site siteQ,
                 ( SELECT
                   job_index,
                   IF( workflow_id IN
                     ( SELECT DISTINCT workflow_id FROM jq_queue WHERE status = 'released')
                   ,1,0) extra_priority
                 FROM jq_queue ) AS jq_extra
               ) ON ( jobQ.job_index = siteQ.job_index AND jobQ.job_index = jq_extra.job_index )
               WHERE status = 'new'
               """
               

        ## because this is a list of all workflows that won't run,
        ## an empty list means any workflow.
        ## notWFL is a commaseparated list of workflow ids or None
        
        if notWFL:
            notWFLquoted='\''+notWFL.replace(',','\',\'')+'\''
            sqlStr +=" AND NOT workflow_id IN (%s) " % notWFLquoted
            
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


            """
            siteMax SQL query
            assumes jq_queue.workflow_id is similar to we_workflow_site_assoc.workflow_id
            """

            maxSitesSQL = \
                        """
                        AND workflow_id IN (
                          SELECT aso.workflow_id FROM we_workflow_site_assoc aso WHERE
                          (aso.site_index IN (%s) OR aso.site_index IS NULL)
                        ) OR workflow_id NOT IN (
                          SELECT aso.workflow_id FROM we_workflow_site_assoc aso
                          GROUP BY aso.workflow_id
                        )
                        """ % siteStr
                                    

            
        else:
            sqlStr += " siteQ.site_index IS NULL "

        sqlStr += " ORDER BY (priority + extra_priority ) DESC, time DESC LIMIT %s;" % count

        Session.execute(sqlStr)
        result = Session.fetchall()
        result = [ x[0] for x in result ]
        
        return result


    def retrieveJobsAtSitesNotWorkflowSitesMax(self, count = 1, jobType = None,
                                       notWFL = None, *sites):
        """
        _retrieveJobsAtSitesNotWorkflowSitesMax_
        
        Get a list of size count matching job indexes from the DB tables
        matched by:
        
        optional not matching workflow id
        optional job type
        required list of site index values.
        
        """

        if len(sites) > 0:
            siteStr = ""
            for s in sites:
                siteStr += "%s," % s
            siteStr = siteStr[:-1]

        else:
            logging.info("This won't work with constraints without site restriction")
            return

        logging.debug("Sites: %s" %siteStr)

        """
        all site indices that this siteStr can handle (because they share the same SE)
        """
        #TODO: replace this with appropriate api call
        sqlStr = \
               """
               SELECT site_index FROM rc_site
               WHERE se_name IN
                 (SELECT se_name FROM rc_site WHERE site_index IN (%s))
                AND is_active = "true" 
               ;
               """ % siteStr

        Session.execute(sqlStr)
        result = Session.fetchall()
        site_group = [ str(x[0]) for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("All site_index that this site can process: %s" %site_group)
        site_group_txt=','.join(site_group)
                                                        
        """
        main goal is to limit the number of WFs spread over sites and keeping as much resources busy:
        - sites_max are assigned on release
        - favour WFs that are already 'in process'
        
        implement max_sites in JQ selection.
        - should be easier than eg in RM (RM has no view on the JQ, JQ has access to RM info.)
        - misprediction is impossible (as new workflows are assigned to sites that can process jobs!)
        
        favouring based on priorities
        - no interference with regular priorities (eg 10*priority + extra_priorities)
        
        Split the new jobs up in 4 categories (based on siteStr): PN, XN, FN and RN.
        RN = All new jobs - (PN + XN + FN)
        
        max_sites increases are dealt with
        - decreases are ignored
        - setting the sites_max to NULL is equivalent to sites_max = +INF
        
        Can't work with pure SQL subqueries, as performance is very bad.
        """

        """
        select all non-valid workflows based on group property
        """
        sqlStr= \
                """
                SELECT DISTINCT workflow_id FROM we_workflow_site_assoc
                WHERE site_index IN
                  (SELECT site_index FROM rc_site_attr
                    WHERE attr_name = 'group' AND attr_value NOT IN
                      (SELECT DISTINCT attr_value FROM rc_site_attr WHERE site_index IN (%s) AND attr_name = 'group')
                  ); 
                """ %siteStr
        Session.execute(sqlStr)
        result = Session.fetchall()
        wf_not_in_group = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("WFs not in any of this sites group: %s" %wf_not_in_group)
        wf_not_in_group_txt='\''+'\',\''.join(wf_not_in_group)+'\''


        """
        drain modes
        - drain: value 'current'
        - drain: value 'merge'
        """
        sqlStr= \
                """
                SELECT attr_value FROM rc_site_attr
                WHERE attr_name = 'drain' AND site_index IN (%s);
                """ %siteStr
        Session.execute(sqlStr)
        result = Session.fetchall()
        drain_modes = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("drain modes for this siteStr: %s" %drain_modes)
        drainSql=''
        if 'merge' in drain_modes:
            drainSql += """ AND jobQ.job_type != 'Merge' """
        if 'processing' in drain_modes:
            drainSql += """ AND jobQ.job_type != 'Processing' """ 
        
        sqlStr= "SELECT DISTINCT workflow_id FROM jq_queue;"
        Session.execute(sqlStr)
        result = Session.fetchall()
        all_wfs = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("all WFs in jq_queue: %s" %all_wfs)
        
        sqlStr= "SELECT DISTINCT id FROM we_Workflow;"
        Session.execute(sqlStr)
        result = Session.fetchall()
        all_we_wfs = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("all WFs in we_Workflow: %s" %all_we_wfs)

        ## workflows not known to we_Workflows
        all_rest=[]
        for wf in all_wfs:
            if wf not in all_we_wfs:
                all_rest.append(wf)
        logging.debug("all WFs not known to we_Workflow: %s" %all_rest)

        """
        PN: (Processing New jobs) new jobs of workflows that are already in processing (ie have jobs in state 'released')
        - with sites_max set: only on siteStr
        - sites_max = NULL
        """

        sqlStr= "SELECT DISTINCT workflow_id FROM jq_queue WHERE status = 'released';"
        Session.execute(sqlStr)
        result = Session.fetchall()
        wf_has_released = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("all WFs with a job in state released: %s" %wf_has_released)

        wf_in_we_Workflow_has_released=[]
        for wf in wf_has_released:
            if wf in all_we_wfs:
                wf_in_we_Workflow_has_released.append(wf)
        logging.debug("all WFs in we_Workflow with a job in state released: %s" %wf_in_we_Workflow_has_released)

        ## workflows with an assigned site_index ALWAYS have jobs in state released!!
        ## hmmm, doesn't work in dummy mode. lets implement it just to make sure
        sqlStr= "SELECT DISTINCT workflow_id FROM we_workflow_site_assoc;"
        Session.execute(sqlStr)
        result = Session.fetchall()
        wf_with_site_aso = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("all WFs in we_workflow_site_assoc: %s" %wf_with_site_aso)

        sqlStr= "SELECT DISTINCT workflow_id FROM we_workflow_site_assoc WHERE site_index IN (%s) AND site_index IS NOT NULL;" % siteStr
        Session.execute(sqlStr)
        result = Session.fetchall()
        wf_sites_max_on_site = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("all WFs in we_workflow_site_assoc with sites_max already on this site: %s" %wf_sites_max_on_site)

        sqlStr= "SELECT DISTINCT id FROM we_Workflow WHERE max_sites IS NULL;"
        Session.execute(sqlStr)
        result = Session.fetchall()
        wf_sites_max_is_NULL = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("all WFs in we_Workflow without max_sites: %s" %wf_sites_max_is_NULL)

        pn_wfs=[]
        for wf in wf_has_released+wf_with_site_aso:
            if wf not in wf_sites_max_on_site+wf_sites_max_is_NULL+all_rest: continue
            if wf not in pn_wfs:
                pn_wfs.append(wf)
     
        logging.debug("all WFs in PN: %s" %pn_wfs)
        pn_wfs_txt='\''+'\',\''.join(pn_wfs)+'\''

        """
        XNb: (eXclusive New jobs type b) new jobs with sites_max set and
        - number of assigned sites < sites_max
        - siteStr not yet as a site
        XN: (eXclusive New jobs) XNb jobs that are no PN 
        """

        sqlStr = \
               """
               SELECT we_Workflow.id FROM
               we_Workflow LEFT OUTER JOIN
               ( SELECT workflow_id, count(site_index) number FROM we_workflow_site_assoc
                 WHERE workflow_id NOT IN
                   ( SELECT DISTINCT workflow_id FROM we_workflow_site_assoc WHERE site_index IN (%s))
                 GROUP BY workflow_id
               ) AS b ON b.workflow_id = we_Workflow.id
               WHERE
                 (
                  ( we_Workflow.max_sites > b.number AND we_Workflow.id = b.workflow_id)
                  OR
                  ( b.number IS NULL AND b.workflow_id IS NULL )
                 ) AND we_Workflow.max_sites IS NOT NULL
               ;
               """ %siteStr

        Session.execute(sqlStr)
        result = Session.fetchall()
        xnb_wfs = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("all WFs in XNb: %s" %xnb_wfs)

        xn_wfs = []
        for wf in xnb_wfs:
            if wf not in pn_wfs:
                xn_wfs.append(wf)
        logging.debug("all WFs in XN: %s" %xn_wfs)
        xn_wfs_txt='\''+'\',\''.join(xn_wfs)+'\''

        ## list of workflows that fullfill a valid sites_max constraint for this siteStr
        sites_max_wfs=wf_sites_max_is_NULL+wf_sites_max_on_site+xn_wfs
        logging.debug("all WFs that fullfill a sites_max constraint: %s" %sites_max_wfs)
        
        """
        FN: (Filler New jobs): new jobs that are no PN and with workflows
        - sites_max NULL
        - not in we_Workflow (also assumed to have sites_max NULL)
        """

        sqlStr= "SELECT DISTINCT workflow_id FROM jq_queue WHERE status = 'new';"
        Session.execute(sqlStr)
        result = Session.fetchall()
        wf_has_new = [ x[0] for x in result ]
        logging.debug("sqlStr: %s" % sqlStr.replace('\n',''))
        logging.debug("all WFs in jq_queue with new jobs: %s" %wf_has_new)
        
        fn_wfs=[]
        for wf in wf_has_new:
            if wf in pn_wfs: continue
            if wf in wf_sites_max_is_NULL+all_rest:
                fn_wfs.append(wf)
        logging.debug("all WFs in FN: %s" %fn_wfs)
        fn_wfs_txt='\''+'\',\''.join(fn_wfs)+'\''

        """
        RN: (Rest New jobs) rest, these CANNOT be handled by this siteStr.
        """

        rn_wfs=[]
        for wf in wf_has_new:
            if wf not in fn_wfs+xn_wfs+pn_wfs:
                rn_wfs.append(wf)
        logging.debug("all WFs in RN: %s" %rn_wfs)
        rn_wfs_txt='\''+'\',\''.join(rn_wfs)+'\''


        """
        Ordering: 
        - priority
        - use order={} to set the ordering (strict >)
        PN > XN > FN: {'pn':1,'xn':1}
        FN > XN > PN: {'pn':-1,'xn':-1}
        XN > FN > PN: {'pn':-1,'xn':1}
        PN > FN > XN: {'pn':1,'xn':-1}
        - no equality between PN, XN or FN priorities possible
        
        - further ordering by WF or random?
        --> by FW, don't start new WFs unnecessary.
        """

        order={'pn':1,'xn':1}

        sqlStr = \
               """
               SELECT jobQ.job_index,jobQ.workflow_id
               FROM jq_queue jobQ LEFT OUTER JOIN (
                 jq_site siteQ,
                   ( SELECT
                     job_index,
                     IF( workflow_id IN (%s), %s,0) extra_pn,
                     IF( workflow_id IN (%s), %s,0) extra_xn,
                     IF( workflow_id IN (%s), 1,0) extra_fn           
                   FROM jq_queue WHERE status = 'new'
                   ) AS jq_extra
               ) ON (jobQ.job_index = siteQ.job_index AND jobQ.job_index = jq_extra.job_index )
               WHERE jobQ.status = 'new'
               AND jobQ.workflow_id NOT IN (%s)
        """ %(pn_wfs_txt,order['pn'],xn_wfs_txt,order['xn'],fn_wfs_txt,rn_wfs_txt)

        ## because this is a list of all workflows that won't run,
        ## an empty list means any workflow.
        ## notWFL is a commaseparated list of workflow ids or None
        
        if notWFL:
            notWFLquoted='\''+notWFL.replace(',','\',\'')+'\''
            
            sqlStr +=" AND NOT jobQ.workflow_id IN (%s) " % notWFLquoted
            
        if jobType != None:
            sqlStr +=  " AND jobQ.job_type=\"%s\" " % jobType

        ## this is different from JobType
        sqlStr += drainSql

        sqlStr +=" AND NOT jobQ.workflow_id IN (%s) " % wf_not_in_group_txt

        """
        use site_group here instead of siteStr!!!
        """

        sqlStr += \
               """
               AND ( siteQ.site_index IN (%s) 
                   OR siteQ.site_index IS NULL
               ) 
               ORDER BY
                 ( 10*priority + 4*extra_pn + 2*ABS(extra_pn)*extra_xn + (ABS(extra_pn)*ABS(extra_xn)-(ABS(extra_pn) +ABS(extra_xn))+1)*extra_fn) DESC,
               jobQ.workflow_id
               LIMIT %s
               ;
               """ %(site_group_txt,count)

        logging.debug("SQL used: %s" % sqlStr.replace('\n',''))

        Session.execute(sqlStr)
        result = Session.fetchall()

        logging.debug("Number of jobs found %s" %len(result))

        jobs_to_be_released=[]
        add_new_site=[]
        for x in result:
            if x[1] not in pn_wfs:
                add_new_site.append(x[1])
                pn_wfs.append(x[1])
            jobs_to_be_released.append(x[0])

        """
        assign siteStr to sites_max
        """
        for wf in add_new_site:
            logging.debug("Adding new workflow %s to site %s for sites_max"%(wf,siteStr))
            for siteindex in sites:
                associateSiteToWorkflow(wf,siteindex)

        """
        the end
        """
        return jobs_to_be_released


    def getSiteForReleasedJob(self, job_spec_id):
        """
        get resourceControl site id for given released jobspec
        """
        
#        sqlStr = """SELECT jq_site.site_index FROM jq_queue, jq_site
#                    WHERE jq_queue.job_index = jq_site.job_index
#                    AND jq_queue.status = 'released'
#                    AND jq_queue.job_spec_id = '%s'""" % job_spec_id

        sqlStr = """SELECT released_site FROM jq_queue
                    WHERE job_spec_id = '%s'""" % job_spec_id
        
        Session.execute(sqlStr)
        # by definition job can only be released for one site
        return Session.fetchone()[0]
        
        
    def countQueuedActiveJobs(self, sites=None, jobTypes=None):
        """
        get number of queued jobs at sites and 
        """
        
        if sites is None:
            sites = ()
        if jobTypes is None:
            jobTypes = ()
        
        sqlStr = \
        """
        SELECT COUNT(jobQ.job_index), we_Job.job_type, siteQ.released_to
        FROM jq_queue jobQ
        LEFT OUTER JOIN jq_site siteQ ON jobQ.job_index = siteQ.job_index
        LEFT OUTER JOIN we_Job ON jobQ.job_spec_id = we_Job.id 
        WHERE jobQ.status = 'released'
        AND we_Job.status IN ('released', 'create', 'submit', 'inProgress')
        """

        if len(sites) > 0:
            siteStr = ""
            for s in sites:
                siteStr += "%s," % s
            siteStr = siteStr[:-1]
            
            sqlStr += " AND siteQ.site_index IN (%s) " % siteStr
            #sqlStr += " OR siteQ.site_index IS NULL ) "
        #else:
        #    sqlStr += " siteQ.site_index IS NULL "
        
        if len(jobTypes) > 0:
            typeStr = ""
            for t in jobTypes:
                typeStr += "%s," % s
            typeStr = typeStr[:-1]
            
            sqlStr += " AND job_type IN (%s) " % siteStr
        
        sqlStr += " GROUP BY we_Job.job_type, siteQ.site_index"
        
        Session.execute(sqlStr)
        temp = Session.fetchall()
        result = {}
        [ result.setdefault(site, {}).__setitem__(type, int(jobs)) for \
                                                 jobs, type, site in temp ]
        
        for site in result.keys():
            result[site]['Total'] = sum(result[site].values())
        
        return result

        
