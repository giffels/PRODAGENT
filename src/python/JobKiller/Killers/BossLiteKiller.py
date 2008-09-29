#!/usr/bin/env python
"""
_BOSSKiller_

Killer plugin for killing BOSS jobs

"""

__revision__ = "$Id: BossLiteKiller.py,v 1.14 2008/09/29 12:15:26 gcodispo Exp $"
__version__ = "$Revision: 1.14 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import logging
import traceback

from JobKiller.Registry import registerKiller
from JobKiller.KillerExceptions import InvalidJobException, \
                                       JobNotSubmittedException

from ProdAgent.WorkflowEntities import JobState
from ProdAgentCore.ProdAgentException import ProdAgentException

# BossLite dependencies
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import BossLiteError, SchedulerError
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Common.System import executeCommand

class BossLiteKiller:
    """
    _BossLiteKiller_

    """

    componentDir = None
    schedulerConfig = { 'timeout' : 300 }
    # schedulerConfig = { 'timeout' : 300,
    #                     'skipWMSAuth' : 1 }

    def __init__(self, args):
        """

        """

        try:
            self.componentDir = args['ComponentDir']
        except KeyError:
            msg = "Component directory is not defined"
            logging.error(msg)
            raise Exception, msg

        # create here the BossSessions
        self.bliteSession = BossLiteAPI('MySQL', dbConfig)


    def killJob(self, jobSpecId, erase=False):
        """

        Arguments:

          JobSpecId -- the job id.
          erase -- remove job info from BOSS database

        Return:

          none

        """
        # jobSpecId is job['name'] for BossLite # Fabio
        logging.info("BossLiteKiller.killJob(%s)" % jobSpecId)

        # verify that the job exists
        try:
            stateInfo = JobState.general(jobSpecId)
        except StandardError, ex:
            msg = "Cannot retrieve JobState Information for %s\n" % jobSpecId
            msg += str(ex)
            logging.error(msg)
            raise InvalidJobException, msg

        # verify that it has not finished
        if stateInfo['State'] == "finished":
            msg = "Cannot kill job %s, since it has finished\n" % jobSpecId
            logging.error(msg)
            raise JobNotSubmittedException, msg

        # get job information from BossLite
        # load the job by name
        job = None
        try:
            job = self.bliteSession.loadJobByName(jobSpecId)
            self.bliteSession.getRunningInstance( job )

        # deal with BOSS specific error
        except BossLiteError, err:
            msg = "Cannot get information for job %s, BOSS error: %s" % \
                  (jobSpecId, str(err))
            logging.error(msg)
            raise Exception, msg

        # kill command through BOSS
        try:
            task = self.bliteSession.getTaskFromJob( job )
            schedSession = BossLiteAPISched( self.bliteSession, \
                                             self.schedulerConfig, task )

            # updating task status, avoiding kill of not finished jobs
            # task = schedSession.query( task, jobsToKill, queryType='parent' )
            self.updateStatus( task, job['jobId'], schedSession )

            # actual kill
            schedSession.kill(task, job['jobId'])

        except SchedulerError, err:
            msg = "Cannot kill job %s, BOSS error: %s" % (jobSpecId, str(err))
            logging.error(msg)
            raise Exception, msg

        if job.runningJob['status'] != 'K' :
            logging.info('Warning: job %s is in status: %s' % \
                         (jobSpecId, job.runningJob['statusScheduler'] ) )

        # archive if requested
        if erase:
            self.bliteSession.archive(job)
        return


    def killWorkflow(self, workflowSpecId):
        """

        Arguments:

          workflowSpecId -- the workflow id.

        Return:

          none

        """

        logging.info("BossLiteKiller.killWorkflow(%s)" % workflowSpecId)

        # get job ids for workflows workflowSpecId
        jobs = JobState.retrieveJobIDs([workflowSpecId])

        totalJobs = len(jobs)
        if totalJobs == 0:
            logging.info("No jobs associated to the workflow %s" % \
                         workflowSpecId)
            return

        skippedJobs = 0

        # kill all jobs
        for job in jobs:

            jobName = job[0]

            # kill each one independently
            try:
                self.killJob(jobName)

            # if job is not found (may be finished right now), killJob()
            # has printed the error message. Try the next one
            except InvalidJobException, msg:
                logging.debug("job %s not yet created, no need to kill it" % \
                             jobName)
                skippedJobs += 1

            # not yet submitted, no need to kill it
            except JobNotSubmittedException, msg:
                logging.debug(
                   "job %s not yet submitted or finished, no need to kill it" \
                   % jobName)

            # other error, stop
            except Exception, msg:
                msg = "Cannot kill jobs for workflow %s: %s" % \
                      (workflowSpecId, str(msg))
                logging.error(msg)
                raise Exception, msg

        # write information if skipped jobs
        if skippedJobs == totalJobs:
            logging.error("No jobs killed at all")
            return

        if skippedJobs > 0:
            logging.error("%s jobs skipped from a total of %s." % \
                          (skippedJobs, totalJobs))
            return

    def eraseJob(self, jobSpecId):
        """

        Arguments:

          JobSpecId -- the job id.

        Return:

          none

        """

        logging.info("BossLiteKiller.eraseJob(%s)" % jobSpecId)

        # kill job
        self.killJob(jobSpecId, erase=True)

        # set number of executions to be equal to the maximum number of
        # allowed retries so jobs will not be resubmitted, or even
        # not submitted at all if they have not been submitted yet
        try:
            JobState.doNotAllowMoreSubmissions([jobSpecId])
        except ProdAgentException, ex:
            msg = "Updating max racers fields failed for job %s\n" % jobSpecId
            msg += str(ex)
            logging.error(msg)
            raise

        # remove all entries
        JobState.cleanout(jobSpecId)

    def eraseWorkflow(self, workflowSpecId):
        """

        Arguments:

          workflowSpecId -- the workflow id.

        Return:

          none

        """

        logging.info("BossLiteKiller.eraseWorkflow(%s)" % workflowSpecId)

        # get job ids for workflows workflowSpecId
        jobs = JobState.retrieveJobIDs([workflowSpecId])

        totalJobs = len(jobs)
        if totalJobs == 0:
            logging.info("No jobs associated to the workflow %s" % \
                         workflowSpecId)
            return

        skippedJobs = 0

        # kill all jobs
        for job in jobs:

            jobName = job[0]

            # kill each one independently
            try:
                self.eraseJob(jobName)

            # if job is not found (may be finished right now), killJob()
            # has printed the error message. Try the next one
            except InvalidJobException, msg:
                logging.debug("job %s not yet created, no need to kill it" % \
                             jobName)
                skippedJobs += 1

            # not yet submitted, no need to kill it
            except JobNotSubmittedException, msg:
                logging.debug(
                    "job %s not yet submitted, no need to kill it" % jobName)

            # other error, stop
            except Exception, msg:
                msg = "Cannot kill jobs for workflow %s: %s" % \
                      (workflowSpecId, str(msg))
                logging.error(msg)
                raise Exception, msg

        # write information if skipped jobs
        if skippedJobs == totalJobs:
            logging.error("No jobs killed at all")
            return

        if skippedJobs > 0:
            logging.error("%s jobs skipped from a total of %s." % \
                          (skippedJobs, totalJobs))
            return

    def killTask(self, taskSpecId):
        """

        Arguments:

          TaskSpecId -- the task id.

        Return:

          none

        """

        logging.info("BossLiteKiller.killTask(%s)" % taskSpecId)

        ### load task
        # set default selection for jobs to kill
        splittedPayload = taskSpecId.split(':')

        taskSpecId = splittedPayload[0]
        # No more needed, already in task object # proxy = splittedPayload[1]

        jobsToKill = 'all'
        # Retrive jobs to kill from payload
        if len(splittedPayload) == 3 and splittedPayload[2] != 'all':
            jobsToKill = eval(str(splittedPayload[2]))
            logging.info("Jobs Recovered by Payload = " + str(jobsToKill))

        # get task specification
        task = None
        try:
            task = self.bliteSession.loadTaskByName(taskSpecId, jobsToKill)
        except BossLiteError, err:
            msg = "Cannot get information for task %s, BOSS error: %s" % \
                  (taskSpecId, str(err))
            logging.error(msg)

        if task is None:
            # no, signal error
            msg = "Cannot get BossLite task information for %s\n" % taskSpecId
            logging.error(msg)
            raise JobNotSubmittedException, msg

        ## build list of jobs to be killed
        logging.info("Taskid: "+ taskSpecId )

        ## perform the actual kill
        # do not allow resubmisions for them
        try:
            # kill
            schedSession = BossLiteAPISched( self.bliteSession, \
                                             self.schedulerConfig, task )

            # updating task status, avoiding kill of not finished jobs
            # task = schedSession.query( task, jobsToKill, queryType='parent' )
            task = self.updateStatus( task, schedSession )

            # actual kill
            logging.info("Jobs to kill: " + str(jobsToKill) )
            schedSession.kill(task, jobsToKill)

        # deal with scheduler error
        except SchedulerError, err:
            if err.value.find( 'Invalid scheduler' ) != -1 :
                msg = \
                    "No jobs submitted in task %s in the specified range: %s" \
                    % (taskSpecId, jobsToKill)
            else:
                msg = "Cannot kill task %s, BOSS error: %s" % \
                      (taskSpecId, str(err))
            logging.error( msg )
            raise Exception, msg

        # deal with BOSS specific error
        except BossLiteError, err:
            msg = "Cannot get information for task %s, BOSS error: %s" % \
                  (taskSpecId, str(err))
            logging.error(msg)
            raise Exception, msg

        # archive
        killedJobs = []
        jobSpecId = []
        for job in task.jobs:

            jobSpecId.append(job['name'])

            if job.runningJob['status'] == 'K':

                killedJobs.append(str(job['jobId']))
                # BossLite archive
                try :
                    self.bliteSession.archive(job)
                except BossLiteError, err:
                    msg = "Cannot update job %s, BOSS error: %s" % \
                          (job['name'], str(err))
                    logging.error(msg)
            else:
                logging.info('Warning: job %s in status %s' % \
                             ( job['name'], \
                               job.runningJob['statusScheduler'] ) )
    
        logging.info("Jobs "+ str(killedJobs) +" killed and Archived")
        logging.info("JobSpecId list: "+ str(jobSpecId) + "\n")
        JobState.doNotAllowMoreSubmissions(jobSpecId)
        logging.info("Jobs %s are not allowed for further resubmission" \
                     % str(jobSpecId))

        return


    def updateStatus(self, task, schedSession):
        """
        update jobs status, bypasing UI bug
        """

        scheduler = schedSession.schedConfig['name']
        if task['user_proxy'] is None :
            task['user_proxy'] = ''

        jobRange = ','.join( str(job['jobId']) for job in task.jobs )

        # updating task status, avoiding kill of not finished jobs
        command = 'python $PRODAGENT_ROOT/lib/JobTracking/QueryStatus.py ' + \
                  str(task['id']) + ' ' + str(jobRange) + ' ' \
                  + scheduler + ' ' + task['user_proxy']

        msg, ret = executeCommand( command, len( task.jobs ) * 30 )
        logging.debug( "QUERY MESSAGE : \n%s " % msg )

        task = self.bliteSession.loadTask(task['id'], jobRange)

        return task


# register the killer plugin
registerKiller(BossLiteKiller, BossLiteKiller.__name__)

