#!/usr/bin/env python
"""
_Joboutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id: JobOutput.py,v 1.1.2.3 2008/04/02 15:28:27 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.3 $"

import logging
import os

# BossLite import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Scheduler import Scheduler
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.Common.Exceptions import TaskError
import os

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

            logging.debug("Getting output for job %s.%s" % \
                          (job['taskId'], job['jobId']))

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
                    return job

            # output retrieved before, then recover interrupted operation
            if status == 'output_retrieved':
                logging.warning("Enqueuing previous ouput for job %s.%s" % \
                                (job['taskId'], job['jobId']))
                return job

            # non expected status, abandon processing for job
            if status != 'in_progress':
                logging.error("Cannot get output for job %s.%s, status is %s" \
                              % (job['taskId'], job['jobId'], status) )
                return job

            #  get output, trying at most maxGetOutputAttempts
            retry = 0
            output = "output successfully retrieved"
            while retry < cls.params['maxGetOutputAttempts']:

                logging.info("job %s.%s retrieval attempt: %d" % \
                             (job['taskId'], job['jobId'], retry))
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
                    # Temporary workaround
                    try:
                        userProxy = os.environ["X509_USER_PROXY"]
                    except KeyError:
                        userProxy = ''
                    os.environ["X509_USER_PROXY"] = task['user_proxy']
                    scheduler.getOutput( job, outdir)
                    os.environ["X509_USER_PROXY"] = userProxy
                    break

                # error, update retry counter
                except SchedulerError, msg:
                    output = str(msg)
                    #job.runningJob['statusHistory'].append(output)
                    logging.error("job %s.%s retrieval failed: %s" % \
                                  (job['taskId'], job['jobId'], str(msg)) )
                    if str( msg ).find( "Proxy Expired" ) != -1 :
                        break
                    retry += 1
                except TaskError, msg:
                    output = str(msg)
                    #job.runningJob['statusHistory'].append(output)
                    logging.error("job %s.%s retrieval failed: %s" % \
                                  (job['taskId'], job['jobId'], str(msg)) )
                    retry += 1
                except StandardError, msg:
                    output = str(msg)
                    #job.runningJob['statusHistory'].append(output)
                    logging.error("job %s.%s retrieval failed: %s" % \
                                  (job['taskId'], job['jobId'], str(msg)))
                    retry += 1
                except :
                    import traceback
                    msg = traceback.format_exc()
                    output = str(msg)
                    #job.runningJob['statusHistory'].append(output)
                    logging.error("job %s.%s retrieval failed: %s" % \
                                  (job['taskId'], job['jobId'], str(msg)) )
                    retry += 1

            logging.info("job %s.%s retrieval status: %s" % \
                          (job['taskId'], job['jobId'], output))
            # FIXME: find a better way to report errors
            try:
                job.runningJob['statusHistory'].append(output)
                bossLiteSession.updateDB( job )
            except TaskError, msg:
                logging.error("job %s.%s retrieval failed: %d" % str(msg))

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

        logging.debug("set done status for job %s.%s" % \
                      (job['taskId'], job['jobId']))

        bossLiteSession = BossLiteAPI('MySQL', dbConfig)

        # update job status
        job['processStatus'] = 'output_processed'
        bossLiteSession.updateDB( job )

        logging.debug("Output processing done for job %s.%s" % \
                      (job['taskId'], job['jobId']))


