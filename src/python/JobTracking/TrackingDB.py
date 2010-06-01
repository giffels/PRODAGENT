#!/usr/bin/env python
"""
_TrackingDB_

"""

__version__ = "$Id: TrackingDB.py,v 1.7 2009/05/07 21:28:54 gcodispo Exp $"
__revision__ = "$Revision: 1.7 $"

import time

class TrackingDB:
    """
    _TrackingDB_
    """

    def __init__(self, bossSession):
        """
        __init__
        """

        self.bossSession = bossSession


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

        rows = self.bossSession.select(query)

        return rows


    def getTaskScheduler(self, taskId):
        """
        __getTaskScheduler__

        retrieve scheduler used by task
        """

        # build query
        query = """
        select distinct( scheduler ) from bl_runningjob
        where task_id=%s and scheduler is not NULL
        """ % str( taskId )

        rows = self.bossSession.selectOne(query)

        return rows


    def getUnprocessedJobs( self, grlist ) :
        """
        __getUnprocessedJobs__

        select jobs not yet assigned to a status query group
        """

        queryAddin = "where group_id is not null "
        if grlist != '':
            queryAddin = "where group_id not in (%s) " % str(grlist)

        # some groups with threads working on
        query = " select task_id, count(job_id) from jt_group %s" % queryAddin
        query += " group by task_id order by count(job_id) desc"

        rows = self.bossSession.select(query)

        if rows is None:
            return []

        return [ key for key in rows ]


    def getGroupTasks(self, group):
        """
        __getGroupTasks__

        retrieves tasks for a given group
        """

        ### query = """
        ### select distinct(g.task_id),t.user_proxy from jt_group g,bl_task t
        ### where g.group_id=%s and g.task_id=t.ID order by t.user_proxy,t.ID
        ### """ % str(group)

        query = "select distinct(task_id) from jt_group where group_id=" \
                + str(group)

        rows = self.bossSession.select(query)

        return [int(key[0]) for key in rows ]


    def getTaskSize(self, taskId ):
        """
        __getTaskSize__

        how many jobs in the task
        """

        query = "select max(job_id) from  jt_group where task_id=" \
                + taskId

        rows = self.bossSession.selectOne(query)
        return rows


    def getUnassociatedJobs(self):
        """
        __getUnassociatedJobs__

        select active jobs not yet associated to a status query group
        """
        ### query = \
        ###       'select j.task_id,j.job_id from ' \
        ###       + ' (select task_id,job_id from bl_runningjob ' \
        ###       + " where closed='N' and scheduler_id IS NOT NULL " \
        ###       + " and process_status like '%handled') j " \
        ###       + ' left join jt_group g ' \
        ###       + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
        ###       + ' where g.job_id IS NULL  order by j.task_id'

        query = \
              'select j.task_id,j.job_id from bl_runningjob j' \
              + ' left join jt_group g' \
              + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
              + ' where g.job_id IS NULL ' \
              + " and j.closed='N' and j.scheduler_id IS NOT NULL" \
              + " and j.process_status like '%handled'" \
              + " order by j.task_id"

        rows = self.bossSession.select(query)
        return rows


    def getAssociatedJobs(self):
        """
        __getAssociatedJobs__

        select active jobs associated to a status query group

        """

        query = \
              'select g.task_id,g.job_id from jt_group g left join ' \
              + ' (select task_id,job_id from bl_runningjob ' \
              + " where closed='N' and scheduler_id IS NOT NULL " \
              + " and process_status like '%handled') j " \
              + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
              + ' where j.job_id IS NULL'

        rows = self.bossSession.select(query)
        return rows


    def addForCheck(self, taskId, jobId ):
        """
        __addForCheck__

        insert job in the query queue
        """

        query = \
              "insert into jt_group(group_id, task_id, job_id)" + \
              " values(''," + str( taskId ) + ',' + str( jobId ) + \
              ') on duplicate key update group_id=group_id'

        self.bossSession.modify(query)


    def removeFromCheck(self, group, taskId, jobId ):
        """
        __removeFromCheck__

        remove job from the query queue
        """

        query = \
                'delete from jt_group where group_id=' + str(group) \
                        + ' and task_id=' + str( taskId ) \
                        + ' and job_id=' + str( jobId )

        self.bossSession.modify(query)


    def setTaskGroup( self, group, taskList ) :
        """
        __setTaskGroup__

        assign tasks to a given group
        """

        query = \
              'update jt_group set group_id=' + str(group) + \
              ' where task_id in (' + taskList  + ')'

        self.bossSession.modify(query)


    def getStuckJobs( self, status, timeout, begin, end='CURRENT_TIMESTAMP' ) :
        """
        __setTaskGroup__

        assign tasks to a given group
        """

        states = ','.join( [ "'%s'" % s for s in status ] )

        query = '''
              select j.name from bl_job j, bl_runningjob r
              where r.status in (%s) and r.process_status="handled"
              and (%s-%s)>%s and j.task_id=r.task_id and j.job_id=r.job_id and
              j.submission_number=r.submission''' \
        % ( states, end, begin, timeout )

        rows = self.bossSession.select(query)

        if rows is None:
            return []

        return [str(key[0]) for key in rows ]



    def processBulkUpdate( self, jlist, processStatus, skipStatus=None ) :
        """
        __setTaskGroup__

        assign tasks to a given group
        """

        #jlist = ','.join( [ str(job.runningJob['id']) for job in jobList ] )

        if skipStatus is not None:
            toSkip = " and status not in ('" +  "','".join( skipStatus ) + "')"
        else :
            toSkip = ''

        if processStatus in ['failed', 'output_requested'] :
            tsString = "', output_request_time='" + \
                       time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime() )
        else :
            tsString = ''


        query = \
              "update bl_runningjob set process_status='" + processStatus + \
              tsString + "' where id in (" + jlist + ")" + toSkip

        self.bossSession.modify(query)

