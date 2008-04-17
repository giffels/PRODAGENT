#!/usr/bin/env python
"""
_Joboutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id: JobOutput.py,v 1.1.2.11 2008/04/17 17:30:59 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.11 $"

import logging
import os

# BossLite import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Scheduler import Scheduler
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.Common.Exceptions import TaskError

###############################################################################
# Class: JobOutput                                                            #
###############################################################################

class JobOutput:
    """
    A static instance of this class deals with job get output operations
    """

    # default parameters
    params = {'maxGetOutputAttempts' : 3,
              'componentDir' : None,
              'dbConfig' : None}

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
    def requestOutput(cls, job):
        """
        __requestOutput__
        
        request output for job.

        """

        # verify status
        if job.runningJob['processStatus'] != 'handled':
            logging.error("Job %s.%s is in status %s, cannot request output" \
                          % (job['taskId'], job['jobId'], job.runningJob['processStatus']))
            return

        job.runningJob['processStatus'] = 'output_requested'

        # commit and close session
        try :
            bossLiteSession = BossLiteAPI('MySQL', dbConfig)
            bossLiteSession.updateDB( job.runningJob )
        except TaskError:
            logging.error("Output for job %s.%s cannot be requested" % \
                          (job['taskId'], job['jobId'] ) )

        logging.debug("getoutput request for %s successfully enqueued" % job['jobId'])

    @classmethod
    def doWork(cls, job):
        """
        __doWork__
        
        get the output of the job specified.

        *** thread safe ***

        """

        try:

            logging.debug("Getting output for job %s.%s" % \
                          (job['taskId'], job['jobId']))

            # open database
            bossLiteSession = BossLiteAPI('MySQL', dbConfig)

            # verify the status 
            status = job.runningJob['processStatus']
            outdir = job.runningJob['outputDirectory']

            # output retrieved before, then recover interrupted operation
            if status == 'output_retrieved':
                logging.warning("Enqueuing previous ouput for job %s.%s" % \
                                (job['taskId'], job['jobId']))
                return job

            # non expected status, abandon processing for job
            if status != 'in_progress' and status != 'failed':
                logging.error("Cannot get output for job %s.%s, status is %s" \
                              % (job['taskId'], job['jobId'], status) )
                return job

            # inconsistent status
            if status == 'in_progress' and job.runningJob['closed'] == 'Y':
                logging.warning("Enqueuing previous ouput for job %s.%s" % \
                                (job['taskId'], job['jobId']))
                job.runningJob['processStatus'] = 'output_retrieved'
                bossLiteSession.updateDB( job )
                return job

            # both for failed and done, a scheduler instance is needed:
            scheduler = None
            task = bossLiteSession.loadTask(job['taskId'])
            if task['user_proxy'] is None:
                task['user_proxy'] = ''
                    
            schedulerConfig = {'name' : job.runningJob['scheduler'],
                               'user_proxy' : task['user_proxy'] ,
                               'service' : job.runningJob['service'] }
            try:
                scheduler = Scheduler.Scheduler(
                    job.runningJob['scheduler'], schedulerConfig )
            except SchedulerError, err:
                logging.info('Can not get scheduler for job %s.%s' % \
                             (job['taskId'], job['jobId'] ))
                logging.info( '[%s]'%str(err) )
                return job

            # job failed: perform postMortem operations and notify the failure
            if status == 'failed':
                try:
                    scheduler.postMortem( job, outdir + '/loggingInfo.log' )
                    job.runningJob['statusHistory'].append( \
                        'retrieved logging-info')
                    logging.info('Retrieved logging info for job %s.%s in %s' \
                                 % (job['taskId'], job['jobId'], outdir ))
                except SchedulerError, err:
                    logging.info('Can not get logging.info for job %s.%s' % \
                                 (job['taskId'], job['jobId'] ))
                    logging.info( '[%s]'%str(err) )
                    job.runningJob['statusHistory'].append( \
                        'failed to retrieve logging-info')

                # perform a BOSS archive operation
                job.runningJob['processStatus'] = 'failure_handled'
                bossLiteSession.updateDB( job )
                bossLiteSession.archive( job )

                return job

            #  get output, trying at most maxGetOutputAttempts
            retry = 0
            output = "output successfully retrieved"
            while retry < cls.params['maxGetOutputAttempts']:
                retry += 1

                logging.info("job %s.%s retrieval attempt: %d" % \
                             (job['taskId'], job['jobId'], retry))
                # perform get output operation
                try:
                    # Temporary workaround
                    try:
                        userProxy = os.environ["X509_USER_PROXY"]
                    except KeyError:
                        userProxy = ''
                    os.environ["X509_USER_PROXY"] = task['user_proxy']
                    scheduler.getOutput( job, outdir)
                    output = "output successfully retrieved"
                    os.environ["X509_USER_PROXY"] = userProxy
                    logging.info('Retrieved output for job %s.%s in %s' % \
                                 (job['taskId'], job['jobId'], outdir ))
                    break

                # error
                except SchedulerError, msg:
                    output = str(msg)
                    job.runningJob['statusHistory'].append(output)
                    logging.error("job %s.%s retrieval failed: %s" % \
                                  (job['taskId'], job['jobId'], str(msg)) )
                    if str( msg ).find( "Proxy Expired" ) != -1 :
                        break
                    elif str( msg ).find( "has been purged" ) != -1 :
                        job['status'] = 'E'
                        job['statusScheduler'] = 'Cleared'
                        bossLiteSession.archive( job )
                        break
                except TaskError, msg:
                    output = str(msg)
                    job.runningJob['statusHistory'].append(output)
                    logging.error("job %s.%s retrieval failed: %s" % \
                                  (job['taskId'], job['jobId'], str(msg)) )
                except StandardError, msg:
                    output = str(msg)
                    job.runningJob['statusHistory'].append(output)
                    logging.error("job %s.%s retrieval failed: %s" % \
                                  (job['taskId'], job['jobId'], str(msg)))
                except :
                    import traceback
                    msg = traceback.format_exc()
                    output = str(msg)
                    job.runningJob['statusHistory'].append(output)
                    logging.error("job %s.%s retrieval failed: %s" % \
                                  (job['taskId'], job['jobId'], str(msg)) )

            logging.info("job %s.%s retrieval status: %s" % \
                          (job['taskId'], job['jobId'], output))

            # log status & update
            try:
                job.runningJob['statusHistory'].append(output)
                bossLiteSession.updateDB( job )
            except TaskError, msg:
                logging.error("job %s.%s retrieval failed: %d" % str(msg))

            # return job info
            return job

        # thread has failed
        except StandardError, msg:

            # show error message 
            msg = "GetOutputThread exception: %s" % str(msg)
            logging.error(msg)

            # return also the id
            return job
 
    @classmethod
    def recreateOutputOperations(cls, pool):
        """
        __recreateOutputOperations__

        recreate interrupted get output operations. Two types of operations
        can be in interrupted states: 1) operations that have been
        requested but never performed, and 2) operations that have been
        performed but never processed (meaning that output was retrieved
        but no messages were sent based on it).

        """

        logging.debug("Recreating interrupted operations")

        # open database
        bossLiteSession = BossLiteAPI('MySQL', dbConfig)

        # get interrupted operations
        jobs = bossLiteSession.loadJobsByRunningAttr( 
            { 'processStatus' : 'in_progress' } ) + \
            bossLiteSession.loadJobsByRunningAttr(
            { 'processStatus' : 'output_retrieved' } )

        numberOfJobs = len(jobs)

        logging.debug("Going to recreate %s get output requests" % \
                      numberOfJobs)

        # enqueue requests
        for job in jobs:
            pool.enqueue(job, job)

        logging.debug("Recreated %s get output requests" % numberOfJobs)

    @classmethod
    def setDoneStatus(cls, job):
        """
        __setDoneStatus__

        signal finished status for get output operation

        """

        logging.debug("set done status for job %s.%s" % \
                      (job['taskId'], job['jobId']))

        bossLiteSession = BossLiteAPI('MySQL', dbConfig)

        # update job status
        job['processStatus'] = 'processed'
        bossLiteSession.updateDB( job )

        logging.debug("Output processing done for job %s.%s" % \
                      (job['taskId'], job['jobId']))


