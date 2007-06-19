#!/usr/bin/env python
"""
_BOSSKiller_

Killer plugin for killing BOSS jobs

"""

__revision__ = "$Id: BOSSKiller.py,v 1.3 2007/05/04 14:17:22 ckavka Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import logging
import os

from JobKiller.Registry import registerKiller
from JobKiller.KillerExceptions import InvalidJobException, \
                                       JobNotSubmittedException
 
from BossSession import BossSession, SchedulerError, BossError

from BossSession import SUBMITTED

from JobState.JobStateAPI import  JobStateChangeAPI, \
                                  JobStateInfoAPI

from ProdAgentCore.ProdAgentException import ProdAgentException

class BOSSKiller:
    """
    _BOSSKiller_

    """

    bossConfigDir = None
    componentDir = None

    def __init__(self, args):
        """

        """

        try:
            self.bossConfigDir = args['bossConfigDir']
            self.componentDir = args['ComponentDir']
        except KeyError:
            msg = "BOSS config and/or component directory is not defined"
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
        # allowed retries so jobs will not be resubmitted, or even
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
            task = bossSession.loadByJobName(jobSpecId)

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
            job.kill("1", 180)

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

        # get task information from BOSS
        try:

            # get the user's proxy if specified
            splittedPayload = taskSpecId.split(':') 

            # export it if necessary
            if len(splittedPayload) > 1:
                logging.info("requires proxy export")

                # perform proxy export 
                proxy = splittedPayload[1]
                taskSpecId = splittedPayload[0]
                previousEnv = os.environ['X509_USER_PROXY']
                os.environ['X509_USER_PROXY'] = str(proxy)
                logging.info("proxy environment set = " + \
                             str(os.environ['X509_USER_PROXY']))

            # create a BOSS session
            bossSession = BossSession(self.bossConfigDir, '2', \
                                      self.componentDir + '/bossLog.log')

            # get task specification
            task = bossSession.loadByJobName(taskSpecId)

            # it should be single task
            if not len(task) == 1:

                # no, signal error
                msg = "Cannot get BOSS task information for %s\n" % taskSpecId
                logging.error(msg)
                raise JobNotSubmittedException, msg

        # deal with BOSS specific error
        except BossError, err:
            msg = "Cannot get information for task %s, BOSS error: %s" % \
                  (taskSpecId, str(err))
            logging.error(msg)
            raise Exception, msg

        # get task
        taskId = task.keys()[0]

        # select jobs names
        logging.info("Taskid: "+ str(taskId))
        jobSpecId = []
        try:
            njobs = task[taskId].load(SUBMITTED)
            jobs = task[taskId].jobsDict()
            logging.info("LOAD RESULT: "+ str(njobs) + "     " + str(jobs))

            # process all jobs
            for jobid, jobMap in jobs.iteritems():
                job = task[taskId].Job(jobid)
                jobSpecId.append(job.staticInfo()["NAME"])

        # error
        except (BossError), err:
            msg = "Cannot get information for task %s, BOSS error: %s" % \
                  (taskSpecId, str(err))
            logging.error(msg) 
        logging.info("Now controll jobs status: ")
       
        # is there any job? 
        if len(jobSpecId) > 0:

            # yes, process all of them
            for jid in jobSpecId:   

                # verify that the job exists
                try:
                    stateInfo = JobStateInfoAPI.general(str(jid))

                    # verify that it has not finished
                    if stateInfo['State'] in ["finished"]:
                        msg = "job %s is terminated, cannot be killed\n" % \
                              str(jid)
                        logging.info(msg)
                        del jid

                # error, remove it
                except StandardError, ex:
                    msg = "Cannot retrieve JobState Information for %s\n" % \
                          str(jid)
                    msg += str(ex)
                    logging.info(msg)
                    del jid

            # do not allow resubmisions for it
            try:
                logging.info("JobSpecId list: "+ str(jobSpecId)) 
                JobStateChangeAPI.doNotAllowMoreSubmissions(jobSpecId)

            # error, operation failed
            except ProdAgentException, ex:
                msg = "Cannot stop resubmissions for jobs %s: " % \
                      str(jobSpecId)
                msg += str(ex)
                logging.error(msg)
                raise

            # kill all jobs in task
            logging.info("Try to kill Task "+str(taskId))
        
            try: 
                task[taskId].kill("all")

            # error
            except SchedulerError, err:
                msg = "Warning : Some jobs may not been killed, probably " + \
                      "because they are in done status: "+ str(err) + " " + \
                      str(SchedulerError)
                logging.error(msg)
             
            # remove information from BOSS database
            task[taskId].archive("all")
            logging.info("Task killed and Archived")  
       
        # no jobs to kill 
        else:
            logging.info("No jobs to kill in BOSS DB")
        
        # restore old proxy if different from current one
        if len(splittedPayload) > 1:
            if previousEnv != str(proxy):
                os.environ['X509_USER_PROXY'] = previousEnv

        return

# register the killer plugin

registerKiller(BOSSKiller, BOSSKiller.__name__)

