#!/usr/bin/env python
"""
_BOSSKiller_

Killer plugin for killing BOSS jobs

"""

__revision__ = "$Id: BossLiteKiller.py,v 1.1.2.3 2008/04/29 15:24:20 farinafa Exp $"
__version__ = "$Revision: 1.1.2.3 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import logging
import os

from JobKiller.Registry import registerKiller
from JobKiller.KillerExceptions import InvalidJobException, \
                                       JobNotSubmittedException

from ProdAgent.WorkflowEntities import JobState
from ProdAgentCore.ProdAgentException import ProdAgentException
 
# BossLite dependencies
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched

class BossLiteKiller:
    """
    _BossLiteKiller_

    """

    componentDir = None

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
        pass  


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
            jobList = self.bossSession.loadJobByName(jobSpecId)
            # should be single unique item
            if not len(jobList) == 1:
                msg = "  Cannot get BOSS task information for %s\n" % jobSpecId
                msg += "  This job has not been submitted or has finished\n"
                msg += "  Any case, it will not be resubmitted"
                logging.error(msg)
                raise JobNotSubmittedException, msg
            job = jobList[0]

        # deal with BOSS specific error 
        except (SchedulerError, BossError), err:
            msg = "Cannot get information for task %s, BOSS error: %s" % \
                  (jobSpecId, str(err))
            logging.error(msg)
            raise Exception, msg

        # kill command through BOSS
        task = bossLiteSession.loadTask(job['taskId'], False)
        schedulerConfig = { \
                          'name' : job.runningJob['scheduler'], \
                          'user_proxy' : task['user_proxy'], \
                          }
        # kill
        bliteSched = BossLiteAPISched(self.bliteSession, schedulerConfig)
        bliteSched.kill(job['taskId'], [ job['jobId'] ])

        # archive if requested
        if erase:
            self.bliteSession.archive(j)
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
        task = self.bliteSession.loadTaskByName(taskSpecId)
        if task is None:
                # no, signal error
                msg = "Cannot get BossLite task information for %s\n" % taskSpecId
                logging.error(msg)
                raise JobNotSubmittedException, msg

        ## build list of jobs to be killed
        logging.info("Taskid: "+ str(task['id']) )

        jobsReadyToKill = []
        if str(jobsToKill) == "all":
            jobsReadyToKill += range(len(task.jobs))
        else:
            jobsReadyToKill += jobsToKill

        # filter the killing list according job statuses
        jobSpecId = []
        scheduler = ''
        for j in task.jobs:

            if j.runningJob['scheduler'] is not None:
                scheduler = j.runningJob['scheduler']
                
            logging.info("Working on job: \n" + str(j.runningJob) + "\n")
            if j['jobId'] not in jobsReadyToKill:
                continue
  
            if j.runningJob['status'] not in ['SS','R', 'SR']:
                logging.info("Unable to kill Job #"+str(j['jobId'])+" : Status is "+str(j.runningJob['statusScheduler']) )
                jobsReadyToKill.remove(j['jobId'])
                continue

            if JobState.general(j['name'])['State'] in ['finished']:
                msg = "Job %s is terminated, cannot be killed\n" % str(j['jobId'])
                logging.info(msg)
                jobsReadyToKill.remove(j['jobId'])
                continue

            jobSpecId.append(j['name'])
            pass

        if len(jobsReadyToKill) == 0:
            logging.info("No jobs to kill for BossLite")
            return

        ## perform the actual kill
        # do not allow resubmisions for them
        logging.info("JobSpecId list: "+ str(jobSpecId) + "\n")
        JobState.doNotAllowMoreSubmissions(jobSpecId)
 
        schedulerConfig = { \
                          'name' : scheduler, \
                          'user_proxy' : task['user_proxy'], \
                          } 
        # kill
        bliteSched = BossLiteAPISched(self.bliteSession, schedulerConfig)
        logging.info("Jobs to kill: "+ str(jobsReadyToKill) )
        bliteSched.kill(task, jobsReadyToKill)

        # archive
        for j in task.jobs:
            if j['jobId'] not in jobsReadyToKill:
                continue
            self.bliteSession.archive(j)

        logging.info("Jobs "+ str(jobsReadyToKill) +" killed and Archived")
        return

# register the killer plugin
registerKiller(BossLiteKiller, BossLiteKiller.__name__)

