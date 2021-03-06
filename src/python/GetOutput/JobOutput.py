#!/usr/bin/env python
"""
_JobOutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id: JobOutput.py,v 1.30 2010/01/28 16:31:10 gcodispo Exp $"
__revision__ = "$Revision: 1.30 $"

import logging
import os
import traceback
import threading
import socket
from copy import deepcopy

# BossLite import
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Common.Exceptions import DbError
from ProdCommon.BossLite.Common.Exceptions import BossLiteError
from GetOutput.JobHandling import JobHandling

###############################################################################
# Class: JobOutput                                                            #
###############################################################################

class JobOutput:
    """
    A static instance of this class deals with job get output operations
    """

    # default parameters
    params = {'maxGetOutputAttempts' : 3,
              'sessionPool' : None,
              'skipWMSAuth' : None,
              'OutputLocation' : None,
              'timeout' : 600,
              'jobHandlingParams' : None
              }

    failureCodes = ['A', 'K']

    schedulerConfig = { 'timeout' : 300 }


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

        if params['skipWMSAuth'] is not None :
            cls.schedulerConfig['skipWMSAuth'] = 1
            logging.info('Skipping WMS ssl authentication')

        cls.params.update( params )
        cls.params['OutputLocation'] = \
                              cls.params['jobHandlingParams']['OutputLocation']


    @classmethod
    def doWork(cls, job):
        """
        __doWork__

        get the output of the job specified.
        return job when successful,
               None if the job does not need/allow further processing

        *** thread safe ***

        """

        try:

            ret = None
            logging.debug("%s: Getting output" % cls.fullId( job ) )

            # open database
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])

            # instantiate JobHandling object
            jHParams = deepcopy(cls.params['jobHandlingParams'])
            jHParams['bossLiteSession'] = bossLiteSession
            jobHandling = JobHandling( jHParams )

            # verify the status
            status = job.runningJob['processStatus']

            # a variable to check if the output already retrieved
            skipRetrieval = False

            # output retrieved before, then recover interrupted operation
            if status == 'output_retrieved':
                logging.warning("%s: Enqueuing previous ouput" % \
                                cls.fullId( job ) )
                skipRetrieval = True

            # non expected status, abandon processing for job
            elif status != 'in_progress' :
                logging.error("%s: Cannot get output, status is %s" % \
                              (cls.fullId( job ), status) )
                return

            # inconsistent status
            if status == 'in_progress' and job.runningJob['closed'] == 'Y':
                logging.warning(
                    "%s in status %s: Enqueuing previous output" % \
                    (cls.fullId( job ), status) )
                job.runningJob['processStatus'] = 'output_retrieved'
                bossLiteSession.updateDB( job )
                skipRetrieval = True

            logging.debug("%s: Processing output" % cls.fullId( job ) )

            if skipRetrieval :
                # job failed: perform postMortem operations and notify failure
                if job.runningJob['status'] in cls.failureCodes:
                    ret = jobHandling.performErrorProcessing(job)
                else:
                    ret = jobHandling.performOutputProcessing(job)

            else :
                ret = cls.action( bossLiteSession, job, jobHandling )

            logging.debug("%s : Processing output finished" % \
                          cls.fullId( job ) )

            # update status
            if ret is not None:
                job.runningJob['processStatus'] = 'processed'
                job.runningJob['closed'] = 'Y'
                # Fabio  # possible fix for SubSuccess bug
                job.runningJob['status'] = 'E'
                bossLiteSession.updateDB( job )
                ##

            else:
                # allow job to be reprocessed
                cls.recoverStatus( job, bossLiteSession )

                # if the job has not to be reprocessed
                if int( job.runningJob['getOutputRetry'] ) >= \
                       int( cls.params['maxGetOutputAttempts'] ) :

                    logging.error( "%s: LAST ATTEMPT RETRIEVAL FAILED!!!" % \
                                   cls.fullId( job ) )
                    
                    # set as failed
                    job.runningJob['status'] = 'A'
                    job.runningJob['processStatus'] = 'failed'
                    job.runningJob['statusScheduler'] = 'Abandoned'
                    job.runningJob['statusReason'] = \
                                           'GetOutput failed %s times' \
                                           % cls.params['maxGetOutputAttempts']

            # perform update
            bossLiteSession.updateDB( job )
            return ret

        # thread has failed because of a Bossite problem
        except BossLiteError, err:

            # allow job to be reprocessed
            cls.recoverStatus( job, bossLiteSession )

            # show error message
            logging.error( "%s failed to process output : %s" % \
                           ( cls.fullId( job ), str(err) ) )

        # thread has failed
        except Exception, ex :

            # allow job to be reprocessed
            cls.recoverStatus( job, bossLiteSession )

            # show error message
            logging.error(
                '%s: GetOutputThread exception: [%s]\nTraceback: %s' % \
                ( cls.fullId( job ), str(ex), str( traceback.format_exc() ) ) )

        # thread has failed
        except :

            # allow job to be reprocessed
            cls.recoverStatus( job, bossLiteSession )

            # show error message
            logging.error( "%s: GetOutputThread traceback: %s" % \
                           ( cls.fullId( job ), traceback.format_exc() ) )



    @classmethod
    def action(cls, bossLiteSession, job, jobHandling):
        """
        perform an action on the configuration bases
        """

        ret = None
        schedSession = None
        try:
            # both for failed and done, a scheduler instance is needed:
            task = bossLiteSession.getTaskFromJob( job )
            schedSession = BossLiteAPISched( bossLiteSession, \
                                             cls.schedulerConfig, task )

            # build needed output directory
            job.runningJob['outputDirectory'] = \
                                            jobHandling.buildOutdir(job, task)

            job.runningJob['getOutputRetry'] = 1 + \
                                        int( job.runningJob['getOutputRetry'] )

            # job failed: perform postMortem operations and notify failure
            if job.runningJob['status'] in cls.failureCodes:
                job = cls.handleFailed( job, task, schedSession)
                if job is None:
                    return
                ret = jobHandling.performErrorProcessing(job)

            # output at destination: just purge service
            elif cls.params['OutputLocation'] == 'SE' or \
                     cls.params['OutputLocation'] == 'SEcopy':
                job = cls.purgeService( job, task, schedSession)
                if job is None:
                    return
                ret = jobHandling.performOutputProcessing(job)

            # get output, trying at most maxGetOutputAttempts
            else :
                job = cls.getOutput( job, task, schedSession)
                if job is None:
                    return
                ret = jobHandling.performOutputProcessing(job)

            # return job info
            return ret

        except BossLiteError, err:
            logging.error('%s: Can not get scheduler : [%s]' % \
                          (cls.fullId( job ), str(err) ))

            # allow job to be reprocessed
            cls.recoverStatus( job, bossLiteSession )



    @classmethod
    def handleFailed(cls, job, task, schedSession ):
        """
        __handleFailed__

        perform postmortem and archive for failed jobs

        """

        try:
            logging.info('%s: Retrieving logging info' % cls.fullId( job ))
            outfile = job.runningJob['outputDirectory'] + '/loggingInfo.log'
            schedSession.postMortem( task, outfile = outfile )
            if not os.path.exists(outfile) :
                logging.error( '%s: Can not get logging info' % \
                               cls.fullId( job  ) )
                return

            logging.info('%s: Retrieved logging info in %s' \
                         % (cls.fullId( job ), outfile ))

        except BossLiteError, err:
            logging.error( '%s: Can not get logging info : [%s]' % \
                           ( cls.fullId( job ), str(err) ) )

            # proxy expired: invalidate job and empty return
            if err.value.find( "Proxy Expired" ) != -1 :
                job.runningJob['closed'] = 'Y'

            return

        return job


    @classmethod
    def purgeService(cls, job, task, schedSession ):
        """
        __purgeService__

        clean up for jobs already retrieved

        """

        try :
 
            statusSched = job.runningJob['status']
            
            oldTimeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout( cls.params['timeout'] )
            schedSession.purgeService( task )
            socket.setdefaulttimeout(oldTimeout)

            if statusSched == 'UE' :
                job.runningJob['status'] = 'UE'

            # log warnings and errors collected by the scheduler session
            log = str(schedSession.getLogger())
            if log is not None:
                logging.info( str(log ) )

        except socket.timeout, to :
            logging.warning( "%s: Warning, failed to purge : %s" \
                             % (cls.fullId( job ), str(to) ) )
            job.runningJob['processStatus'] = 'output_retrieved'

            return job
            

        except BossLiteError, err:
            logging.warning( "%s: Warning, failed to purge : %s" \
                             % (cls.fullId( job ), str(err) ) )
            job.runningJob['processStatus'] = 'output_retrieved'

            # proxy expired: invalidate job and empty return
            if err.value.find( "Proxy Expired" ) != -1 :
                job.runningJob['closed'] = 'Y'

            return job


        return job


    @classmethod
    def getOutput(cls, job, task, schedSession ):
        """
        __getOutput__

        perform actual scheduler getOutput

        """

        #  get output, trying at most maxGetOutputAttempts
        logging.info("%s: retrieval attempt %s" % \
                     (cls.fullId( job ), job.runningJob['getOutputRetry']))

        #  perform get output operation
        try:

            outdir = job.runningJob['outputDirectory']

            oldTimeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout( cls.params['timeout'] )
            schedSession.getOutput( task, outdir=outdir)
            socket.setdefaulttimeout(oldTimeout)

            job.runningJob['processStatus'] = 'output_retrieved'

            logging.info('%s: Retrieved output in %s' % \
                         (cls.fullId( job ), outdir ))

            # log warnings and errors collected by the scheduler session
            log = str(schedSession.getLogger())
            if log is not None:
                logging.info( log )

        # socket timeout
        except socket.timeout, to :
            logging.error("%s: retrieval failed: %s" % \
                          (cls.fullId( job ), str(to) ) )

        # scheduler interaction error
        except BossLiteError, err:

            logging.error("%s: retrieval failed: %s" % \
                          (cls.fullId( job ), str(err) ) )
            logging.info( "BossLiteLogger : %s " % \
                          str(schedSession.getLogger()) )

            # proxy expired: invalidate job and empty return
            if err.value.find( "Proxy Expired" ) != -1 :
                job.runningJob['closed'] = 'Y'

            # not ready for GO: waiting for next round
            elif err.message().find("Output not yet Ready") != -1 or \
                     err.message().find( "Job current status doesn" ) != -1:
                logging.error("%s in status %s: waiting next round" % \
                              (cls.fullId( job ), job.runningJob['status'])
                              )
                # allow job to be reprocessed
                job.runningJob['processStatus'] = 'handled'

            else :
                # oops: What to do?!?!
                logging.error("%s: no action taken: [%s]" % \
                              (cls.fullId( job ), str(err) ) )

            # in case of errors does not return...
            return

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

        try :
            # open database
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])

            # get interrupted operations
            jobs = bossLiteSession.loadJobsByRunningAttr(
                { 'processStatus' : 'in_progress' } )
            jobs.extend(
                bossLiteSession.loadJobsByRunningAttr(
                { 'processStatus' : 'output_retrieved' } )
                )
        except DbError, msg:
            logging.error('Error updating DB : %s ' % str(msg))

        except Exception, msg:
            logging.error('Unknown Error updating DB : %s ' % str(msg))

        numberOfJobs = len(jobs)

        logging.debug("Going to recreate %s get output requests" % \
                      numberOfJobs)

        # enqueue requests
        for job in jobs:

            try:
                pool.enqueue(job['id'], job)

            except Exception, err:
                logging.error( "%s: failed restoring : %s" % \
                               (cls.fullId( job ), str(err) ) )
            except:
                logging.error( "%s: failed restoring: %s" % \
                               (cls.fullId( job ), \
                                str( traceback.format_exc() ) ) )

        logging.debug("Recreated %s get output requests" % numberOfJobs)



    @classmethod
    def recoverStatus( cls, job, bossLiteSession ) :
        """
        __recoverStatus__

        allow job to be reprocessed
        to be used in case of not job related failures
        """

        # allow job to be reprocessed
        try :
            logging.info( '%s: Recovering job status for next retry' \
                          %  cls.fullId( job ) )
            if job.runningJob['status'] in cls.failureCodes:
                job.runningJob['processStatus'] = 'failed'
            else:
                job.runningJob['processStatus'] = 'output_requested'
            job.runningJob['closed'] = 'N'
            bossLiteSession.updateDB( job )
        except:
            logging.warning(
               '%s: unable to recover job status, restart component to retry' \
               %  cls.fullId( job ) )


    @classmethod
    def fullId( cls, job ):
        """
        __fullId__

        compose job primary keys in a string
        """

        return '[' + threading.currentThread().getName() + \
               '] Job ' + str( job['taskId'] ) + '.' \
               + str( job['jobId'] ) + '.' \
               + str( job['submissionNumber'] )
