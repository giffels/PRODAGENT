#!/usr/bin/env python
"""
_BOSSKiller_

Killer plugin for killing BOSS jobs

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import logging

from JobKiller.Registry import registerKiller
from JobKiller.KillerExceptions import InvalidJobException, \
                                       JobNotSubmittedException
 
from BossSession import BossSession, SchedulerError, BossError

from JobState.JobStateAPI import JobStateInfoAPI, JobStateChangeAPI

from ProdAgentCore.ProdAgentException import ProdAgentException

class BOSSKiller:
    """
    _BOSSKiller_

    """

    bossConfigDir = None

    def __init__(self, args):
        """

        """

        try:
            self.bossConfigDir = args['bossConfigDir']
        except KeyError:
            msg = "BOSS config directory is not defined"
            logging.error(msg)
            raise Exception, msg

    def killJob(self, jobSpecId, erase=False):
        """

        Arguments:

          JobSpecId -- the job id.
          erase -- remove job info from BOSS database

        Return:

          none

        """

        logging.info("BOSSKiller.killJob(%s)" % jobSpecId)

        # verify that the job exists
        try:
            stateInfo = JobStateInfoAPI.general(jobSpecId)
        except StandardError, ex:
            msg = "Cannot retrieve JobState Information for %s\n" % jobSpecId
            msg += str(ex)
            logging.error(msg)
            raise InvalidJobException, msg

        # verify that it has not finished
        if stateInfo['State'] == "finished":
            msg = "Cannot kill job %s, since it has finished\n" % jobSpecId
            logging.error(msg)
            raise Exception, msg

        # set number of executions to be equal to the maximum number of
        # allowed racers so jobs will not be resubmitted, or even
        # not submitted at all if they have not been submitted yet
        try:
            JobStateChangeAPI.doNotAllowMoreSubmissions([jobSpecId])
        except ProdAgentException, ex:
            msg = "Updating max racers fields failed for job %s\n" % jobSpecId
            msg += str(ex)
            logging.error(msg)
            raise

        # get job information from BOSS
        try:
            bossSession = BossSession(self.bossConfigDir)
            task = bossSession.loadByName(jobSpecId)

            # should be single task
            if not len(task) == 1:
                msg = "  Cannot get BOSS task information for %s\n" % jobSpecId
                msg += "  This job has not been submitted or has finished\n"
                msg += "  Any case, it will not be resubmitted"
                logging.error(msg)
                raise JobNotSubmittedException, msg

        # deal with BOSS specific error 
        except (SchedulerError, BossError), err:
            msg = "Cannot get information for task %s, BOSS error: %s" % \
                  (jobSpecId, str(err))
            logging.error(msg)
            raise Exception, msg

        # get task id and job
        taskId = task.keys()[0]
        job = task[taskId] 

        # kill command through BOSS
        try:
            job.kill("1")

            # archive if requested
            if erase:
                job.archive("1")

        # deal with BOSS specific error
        except (SchedulerError, BossError), err:
            msg = "Cannot get information for task %s, BOSS error: %s" % \
                  (jobSpecId, str(err))
            logging.error(msg)
            raise Exception, msg

    def killWorkflow(self, workflowSpecId):
        """

        Arguments:

          workflowSpecId -- the workflow id.

        Return:

          none

        """

        logging.info("BOSSKiller.killWorkflow(%s)" % workflowSpecId)

        # get job ids for workflows workflowSpecId
        jobs = JobStateInfoAPI.retrieveJobIDs([workflowSpecId])

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
                logging.debug("job %s not yet submitted, no need to kill it" % \
                             jobName)

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

        logging.info("BOSSKiller.eraseJob(%s)" % jobSpecId)

        # kill job
        self.killJob(jobSpecId, erase=True)

        # remove all entries
        JobStateChangeAPI.cleanout(jobSpecId)

    def eraseWorkflow(self, workflowSpecId):
        """

        Arguments:

          workflowSpecId -- the workflow id.

        Return:

          none

        """

        logging.info("BOSSKiller.eraseWorkflow(%s)" % workflowSpecId)

        # get job ids for workflows workflowSpecId
        jobs = JobStateInfoAPI.retrieveJobIDs([workflowSpecId])

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
                logging.debug("job %s not yet submitted, no need to kill it" % \
                             jobName)

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

        logging.info("BOSSKiller.killTask(%s)" % taskSpecId)

        return

# register the killer plugin

registerKiller(BOSSKiller, BOSSKiller.__name__)

