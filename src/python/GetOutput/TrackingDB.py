#!/usr/bin/env python
"""
_TrackingDB_

"""

__version__ = "$Id: TrackingDB.py,v 1.1.2.2 2008/03/28 15:36:51 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.2 $"

from ProdAgentBOSS.BOSSCommands import directDB

class TrackingDB:
    """
    _TrackingDB_
    """

    def __init__(self, session):
        """
        __init__
        """

        self.session = session
        


    def getJobsStatistic(self):
        """
        __setJobs__

        set job status for a set of jobs
        """

        # build query 
        query = """
        select status, count( status ) from bl_runningjob
        where closed='N' group by  status
        """

        rows = directDB.select(self.session, query)

        return rows


    def getUnprocessedJobs( self, grlist ) :
        """
        __getUnprocessedJobs__
        
        select jobs not yet assigned to a status query group
        """

        query_addin = "where group_id is not null "
        if grlist != '':
            query_addin = "where group_id not in (%s) " % str(grlist)
            
        # some groups with threads working on
        query = " select task_id, count(job_id) from jt_group %s" % query_addin
        query += " group by task_id order by count(job_id) desc"

        rows = directDB.select(self.session, query)
        return rows


    def getGroupTasks(self, group):
        """
        __getGroupTasks__

        retrieves tasks for a given group
        """

        query = """
        select distinct(g.task_id),t.user_proxy from jt_group g,bl_task t 
        where g.group_id=%s and g.task_id=t.ID order by t.user_proxy,t.ID
        """ % str(group)

        rows = directDB.select(self.session, query)

        return rows


    def getTaskSize(self, taskId ):

        query = "select max(job_id) from  jt_group where task_id=" \
                + taskId

        rows = directDB.selectOne(self.session, query)
        return rows

    def getUnassociatedJobs(self):

        query = \
              'select j.task_id,j.job_id from bl_runningjob j' \
              + ' left join jt_group g' \
              + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
              + ' where g.job_id IS NULL and j.job_id IS NOT NULL' \
              + " and j.closed='N' order by j.task_id"

        rows = directDB.select(self.session, query)
        return rows



    def getAssociatedJobs(self):
        """
        remove all finished jobs from a specific group.

        jobs assigned: all jobs in the group

        """

        query = \
              'select g.task_id,g.job_id from bl_runningjob j' \
              + 'right join jt_group g' \
              + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
              + ' where j.job_id IS NULL ' \
              + " or j.status in ('SE','SD','SA')"

        rows = directDB.select(self.session, query)
        return rows

        

    def addForCheck(self, taskId, jobId ):
        query = \
              "insert into jt_group(group_id, task_id, job_id)" + \
              " values(''," + str( taskId ) + ',' + str( jobId ) + \
              ') on duplicate key update group_id=group_id'

        directDB.modify(self.session, query)
 


    def removeFromCheck(self, group, taskId, jobId ):
        query = \
                'delete from jt_group where group_id=' + str(group) \
                        + ' and task_id=' + str( taskId ) \
                        + ' and job_id=' + str( jobId )

        directDB.modify(self.session, query)


    def setTaskGroup( self, group, taskList ) :
        
        query = \
              'update jt_group set group_id=' + str(group) + \
              ' where task_id in (' + taskList  + ')'

        directDB.modify(self.session, query)

