#!/usr/bin/env python
"""
_JobOutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id: JobOutput.py,v 1.15 2008/10/07 17:07:27 gcodispo Exp $"
__revision__ = "$Revision: 1.15 $"

import logging
import os
import traceback
import threading

# BossLite import
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Common.Exceptions import DbError
from ProdCommon.BossLite.Common.Exceptions import BossLiteError

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
              'sessionPool' : None,
              'OutputLocation' : None,
              'dropBoxPath' : None
              }

    failureCodes = ['A', 'K', 'SA']

    schedulerConfig = { 'timeout' : 300 } #,
    #                    'skipWMSAuth' : 1 }

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

        cls.params.update( params )


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

            logging.debug("%s: Getting output" % cls.fullId( job ) )

            # open database
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])

            # verify the status
            status = job.runningJob['processStatus']

            # output retrieved before, then recover interrupted operation
            if status == 'output_retrieved':
                logging.warning("%s: Enqueuing previous ouput" % \
                                cls.fullId( job ) )
                return job

            # non expected status, abandon processing for job
            if status != 'in_progress' :
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
                return job

            schedSession = None
            try:
                # both for failed and done, a scheduler instance is needed:
                task = bossLiteSession.getTaskFromJob( job )
                schedSession = BossLiteAPISched( bossLiteSession, \
                                                 cls.schedulerConfig, task )

                # build needed output directory
                job.runningJob['outputDirectory'] = cls.buildOutdir(job, task)

                # job failed: perform postMortem operations and notify failure
                if status in cls.failureCodes:
                    job = cls.handleFailed( job, task, schedSession)

                # output at destination: just purge service
                elif cls.params['OutputLocation'] == 'SE' or \
                         cls.params['OutputLocation'] == 'SEcopy':
                    cls.purgeService( job, task, schedSession)

                # get output, trying at most maxGetOutputAttempts
                else :
                    job = cls.getOutput( job, task, schedSession)

                # if success, the job is returned: update and return!
                if job is not None:
                    bossLiteSession.updateDB( job )

                # return job info
                return job

            except BossLiteError, err:
                logging.error('%s: Can not get scheduler : [%s]' % \
                              (cls.fullId( job ), str(err) ))

                # allow job to be reprocessed
                cls.recoverStatus( job, bossLiteSession )

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

        # return also the id
        # return job


    @classmethod
    def handleFailed(cls, job, task, schedSession ):
        """
        __handleFailed__

        perform postmortem and archive for failed jobs

        """

        try:
            outfile = job.runningJob['outputDirectory'] + '/loggingInfo.log'
            schedSession.postMortem( task, outfile = outfile )
            logging.info('%s: Retrieved logging info in %s' \
                         % (cls.fullId( job ), outfile ))
        except BossLiteError, err:
            logging.info( '%s: Can not get logging info : [%s]' % \
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
            schedSession.purgeService( task )
            if statusSched == 'UE' :
                job.runningJob['status'] = 'UE'

            # log warnings and errors collected by the scheduler session
            log = str(schedSession.getLogger())
            if log is not None:
                logging.info( log )

        except BossLiteError, err:
            logging.warning( "%s: Warning, failed to purge : %s" \
                             % (cls.fullId( job ), str(err) ) )
            job.runningJob['processStatus'] = 'output_retrieved'
            
            # proxy expired: invalidate job and empty return
            if err.value.find( "Proxy Expired" ) != -1 :
                job.runningJob['closed'] = 'Y'

            return

        return job


    @classmethod
    def getOutput(cls, job, task, schedSession ):
        """
        __getOutput__

        perform actual scheduler getOutput

        """

        #  get output, trying at most maxGetOutputAttempts
        retry = 0

        while retry < int( cls.params['maxGetOutputAttempts'] ):
            retry += 1

            logging.info("%s: retrieval attempt %d" % \
                         (cls.fullId( job ), retry))

            #  perform get output operation
            try:
                outdir = job.runningJob['outputDirectory']
                schedSession.getOutput( task, outdir=outdir)
                job.runningJob['processStatus'] = 'output_retrieved'

                logging.info('%s: Retrieved output in %s' % \
                             (cls.fullId( job ), outdir ))

                # log warnings and errors collected by the scheduler session
                log = str(schedSession.getLogger())
                if log is not None:
                    logging.info( log )

                # success: stop processing
                break

            # scheduler interaction error
            except BossLiteError, err:

                # if the job has not to be reprocessed
                if retry >= int( cls.params['maxGetOutputAttempts'] ) :

                    logging.error( "%s: LAST ATTEMPT RETRIEVAL FAILED!!!" % \
                                   cls.fullId( job ) )

                    # set as failed
                    job.runningJob['processStatus'] = 'failed'
                    job.runningJob['status'] = 'DA'
                    cls.handleFailed( job, task, schedSession )
                
                logging.error("%s: retrieval failed: %s" % \
                              (cls.fullId( job ), str(err) ) )
                logging.info( "BossLiteLogger : %s " % \
                              str(schedSession.getLogger()) )

                # proxy expired: invalidate job and empty return
                if err.value.find( "Proxy Expired" ) != -1 :
                    job.runningJob['closed'] = 'Y'
                    return

                # not ready for GO: waiting for next round
                elif err.message().find( "Job current status doesn" ) != -1:
                    logging.error("%s in status %s: waiting next round" % \
                                  (cls.fullId( job ), job.runningJob['status'])
                                  )
                    # allow job to be reprocessed
                    job.runningJob['processStatus'] = 'handled'
                    return

                else :
                    # oops: What to do?!?!
                    logging.error("%s: no action taken: [%s]" % \
                                  (cls.fullId( job ), str(err) ) )
                    continue

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
                pool.enqueue(job, job)

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
            if job.runningJob['status'] == 'SD':
                job.runningJob['processStatus'] = 'output_requested'
            else:
                job.runningJob['processStatus'] = 'failed'
            bossLiteSession.updateDB( job )
        except:
            logging.warning(
               '%s: unable to recover job status, restart component to retry' \
               %  cls.fullId( job ) )



    @classmethod
    def buildOutdir( cls, job, task ) :
        """
        __buildOutdir__

        compose outdir name and make the directory
        """

        # try with boss db
        if job.runningJob['outputDirectory'] is not None :
            outdir = job.runningJob['outputDirectory']

        # try to compose the path from task
        else :
            # SE?
            if cls.params['OutputLocation'] == "SE" or \
                cls.params['OutputLocation'] == "SEcopy" :
                outdir = \
                       cls.params['dropBoxPath'] + '/' + task['name'] + '_spec'

            # fallback to task directory
            elif task['outputDirectory'] is not None \
                   and task['outputDirectory'] != '' :
                outdir = task['outputDirectory']

            # fallback to the component directory
            else :
                outdir = cls.params['componentDir']


            # FIXME: get outdir
            outdir = "%s/BossJob_%s_%s/Submission_%s/" % \
                 (outdir, job['taskId'], job['jobId'], job['submissionNumber'])

        # make outdir
        logging.info("%s: Creating directory %s" % \
                     (cls.fullId( job ), outdir))
        try:
            os.makedirs( outdir )
        except OSError, err:
            if  err.errno == 17:
                # existing dir
                pass
            else :
                logging.error("%s: Cannot create directory %s : %s" % \
                     (cls.fullId( job ), outdir, str(err)))
                raise err

        # return outdir
        return outdir


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
