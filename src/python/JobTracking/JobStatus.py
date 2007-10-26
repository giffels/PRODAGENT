#!/usr/bin/env python
"""
_JobStatus_

Deals with job status operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__revision__ = "$Id: JobStatus.py,v 1.1.2.7 2007/10/23 09:40:54 gcodispo Exp $"
__version__ = "$Revision: 1.1.2.7 $"

import logging
from ProdAgentBOSS.BOSSCommands import BOSS, checkUserProxy
from BossSession import BossError, SchedulerError

from BossSession import SUBMITTED
import os, traceback
from time import sleep

###############################################################################
# Class: JobStatus                                                            #
###############################################################################

class JobStatus:
    """
    A static instance of this class deals with job status operations
    """

    params = {'delay' : 30, 'jobsToPoll' : 100}     # parameters

    def __init__(self):
        """
        Attention: in principle, no instance of this static class is created!
        """
        
        pass

    @classmethod
    def setParameters(cls, params):
        """
        set parameters
        """

        cls.params = params

    @classmethod
    def doWork(cls, group):
        """
        get the status of the jobs in the group.

        jobs assigned: all jobs in the group.

        """

        logging.info("Getting job status for jobs in group " + str(group))

        query = """
        select distinct(g.task_id),t.TASK_INFO from jt_group g,TASK t 
        where g.group_id=%s and g.task_id=t.ID order by t.TASK_INFO,t.ID
        """ % str(group)

        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()

        # query BOSS for user certificates  
        (adminSession, out) = \
                       BOSS.performBossQuery(adminSession, query)
        del( adminSession )

        # process by certificate
        tasks = out.split()[2:]
        ntask = len(tasks)/2
        tasklist = ''
        prevcert = ''
        i = 0

        while i < ntask :
            task = tasks[i*2]
            cert = tasks[i*2+1]
            i += 1

            # if same certificate group or just first entry, append
            if cert == prevcert or tasklist == '' :
                tasklist += task + ','
                prevcert = cert

                # if not last task, get next, otherwise process
                if i != ntask :
                    continue
                
            # else if not same certificate, but anyway the last,
            # process current tasklist and step back to process the last
            elif cert != prevcert and i == ntask :
                i -= 1

            # evaluate valid certificates and perform the query
            try :
                checkUserProxy( prevcert )
                tasklist = tasklist[:-1]

                # ask BOSS for LB query
                logging.info('BOSS query for tasks ' + tasklist)
                cls.bossQuery( tasklist, ntask, prevcert )
            except :
                logging.info(
                        "cert path " + prevcert + \
                        " does not exists: skipping tasks " + tasklist
                        )

            # if reached this point, there is at least one task left
            # the current task goes anyway in the next query
            tasklist = task + ','
            prevcert = cert

        sleep(cls.params['delay'])


    @classmethod
    def bossQuery( cls, tasklist, taskn, cert ):
        """
        Perform the LB query through BOSS
        """
        
        subQuery = 1
        jobRange = "all"
        jobs = int ( cls.params['jobsToPoll'] )

        # if just one task, evaluate if the size requires further splits
        if taskn == 1:
            query = "select max(job_id) from  jt_group where task_id=" \
                    + tasklist

            # get a BOSS session
            adminSession = BOSS.getBossAdminSession()
            
            # query BOSS for user certificates  
            (adminSession, out) = \
                           BOSS.performBossQuery(adminSession, query)
            del( adminSession )
            
            # define number of LB query
            val = out.split()[1].strip()
	    subQuery = int( int( val ) / jobs ) + 1

        # perform the query for the task range or for the job range in the task
        for i in range ( subQuery ) :
            if subQuery > 1:
                jobRange = str( i * jobs ) + ':' + str( (i + 1) * jobs)

            logging.debug( 'LB query jobs ' + jobRange \
                           +  ' of task ' + tasklist )
            # query group of tasks
            try :
                bossSession = BOSS.getBossSession()

                # set the environment
                os.environ["X509_USER_PROXY"] = cert

                # actual query
                bossSession.query(SUBMITTED, tasklist, jobRange)

                # clear BossSession to release memory
                bossSession.clear()
                del ( bossSession )
                logging.info("LB status retrieved for jobs " + jobRange \
                             + ' of task ' + tasklist )
            except SchedulerError,e:
                logging.error("Failed to retrieve status for jobs " \
                             + e.__str__() + ' of task ' + tasklist )

            except BossError,e:
                logging.error( "BOSS Error : " + e.__str__() )


    @classmethod
    def addNewJobs(cls):
        """
        include new jobs in the set of jobs to be watched for.

        jobs assigned: all new jobs.

        """
        
        query = \
              'select j.TASK_ID,j.CHAIN_ID from JOB j left join jt_group g' \
              + ' on (j.TASK_ID=g.task_id and j.CHAIN_ID=g.job_id) ' \
              + ' where g.job_id IS NULL and j.CHAIN_ID IS NOT NULL' \
              + " and j.STATUS not in ('SE','SD','SA')" \
              + ' order by j.TASK_ID'

        try:
            adminSession = BOSS.getBossAdminSession()
            # perform BOSS query
            (adminSession, out) = \
                           BOSS.performBossQuery(adminSession, query)
            joblist = out.split()[2:]
            for i in range(len(joblist)/2):
                query = "insert into jt_group(group_id, task_id, job_id)" + \
                        "  values(''," \
                        + joblist[i*2] + ',' + joblist[i*2+1] \
                        + ') on duplicate key update group_id=group_id'
                # perform BOSS query
                (adminSession, out) = \
                               BOSS.performBossQuery(adminSession, query)
                logging.debug(
                    "Adding jobs to queue with BOSS id "\
                    +  joblist[i*2] + '.' + joblist[i*2+1]
                    )
        except BossError,e:
            logging.error( "BOSS Error : " + e.__str__() )
        except StandardError, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )


    @classmethod
    def removeFinishedJobs(cls, group):
        """
        remove all finished jobs from a specific group.

        jobs assigned: all jobs in the group

        """

        query = \
              'select g.task_id,g.job_id from JOB j right join jt_group g' \
              + ' on (j.TASK_ID=g.task_id and j.CHAIN_ID=g.job_id) ' \
              + ' where j.CHAIN_ID IS NULL ' \
              + " or j.STATUS in ('SE','SD','SA')"
        try:
            adminSession = BOSS.getBossAdminSession()
            # perform BOSS query
            (adminSession, out) = \
                           BOSS.performBossQuery(adminSession, query)
            joblist = out.split()[2:]
            for i in range(len(joblist)/2):
                query = 'delete from jt_group where group_id=' + str(group) \
                        + ' and task_id=' + joblist[i*2] \
                        + ' and job_id=' + joblist[i*2+1]
                (adminSession, out) = \
                               BOSS.performBossQuery(adminSession, query)
                logging.debug(
                    "Removing jobs for group " + str(group) \
                    + " with BOSS id " +  joblist[i*2] + '.' + joblist[i*2+1]
                    )
        except BossError,e:
            logging.error( "BOSS Error : " + e.__str__() )
        except StandardError, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )


