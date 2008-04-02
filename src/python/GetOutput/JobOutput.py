#!/usr/bin/env python
"""
_Joboutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id: JobOutput.py,v 1.1.2.2 2008/03/28 15:36:51 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.2 $"

import logging
import os

# BossLite import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Scheduler import Scheduler

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
        except :
            logging.error("Output for job %s.%s cannot be requested" % \
                          (job['taskId'], job['jobId'] ) )

        logging.debug("getoutput request for %s successfully enqueued" % job['jobId'])

    @classmethod
    def doWork(cls, job):
        """
        get the output of the job specified.

        *** thread safe ***

        """

        try:

            logging.debug("Getting output for job" + str(job))

            # open database
            bossLiteSession = BossLiteAPI('MySQL', dbConfig)

            # verify the status 
            status = job.runningJob['processStatus']

            # create directory
            outdir = cls.params['componentDir'] + \
                     '/BossJob_%s_%s/Submission_%s' % \
                     (job['taskId'], job['jobId'], job['submissionNumber'])
            try:
                os.makedirs( outdir )
            except OSError, err:
                if  err.errno == 17:
                    # existing dir
                    pass
                else :
                    # cannot create directory, go to next job
                    logging.error("Cannot create directory : " + str(err))
                    return

            # output retrieved before, then recover interrupted operation
            if status == 'output_retrieved':
                logging.warning("Enqueuing previous ouput for job %s" % \
                              str(job) )
                return job

            # non expected status, abandon processing for job
            if status != 'in_progress':
                logging.error("Cannot get output for job %s, status is %s" % \
                              (job['jobId'], status) )
                return job

            #  get output, trying at most maxGetOutputAttempts
            retry = 0
            while retry < cls.params['maxGetOutputAttempts']:

                # perform get output operation
                try:
                    task = bossLiteSession.loadTask(job['taskId'])
                    if task['user_proxy'] is None:
                        task['user_proxy'] = ''
                    schedulerConfig = {'name' : job.runningJob['scheduler'],
                                       'user_proxy' : task['user_proxy'] ,
                                       'service' : job.runningJob['service'] }
                    scheduler = Scheduler.Scheduler( 
                        job.runningJob['scheduler'], schedulerConfig
                        )
                    scheduler.getOutput( job, outdir)
                    break

                # error, update retry counter
                except StandardError, msg:
                    logging.error(str(msg))
                    output = str(msg)
                    retry += 1

            # FIXME: find a better way to report errors
            job.runningJob['statusHistory'].append(output)
            bossLiteSession.updateDB( job )

            # check for empty output
            # if output == '':
            #    output = "error: no output retrived by BOSSGetoutput command"

            # update quotes in output
            #output = output.replace("'", '"')

            # update info in database
            #updateStatus = db.setJobInfo(jobId, status = 'output_retrieved', \
            #                             output = output)
            #if updateStatus != 1:
            #    logging.warning("Output not updated for job %s: %s" % \
            #                    (jobId,updateStatus))
            #else:
            #    logging.debug("Output for job %s successfully enqueued" % \
            #                  jobId)

            # done, commit and finish
            #session.commit()
            #session.close()

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
        _recreateOutputOperations_

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
        _setDoneStatus_

        signal finished status for get output operation

        """

        logging.debug("set done status for: " + str(job))

        bossLiteSession = BossLiteAPI('MySQL', dbConfig)

        # update job status
        job['processStatus'] = 'output_processed'
        bossLiteSession.updateDB( job )

        logging.debug("Output processing done for %s", str(job))


