#!/usr/bin/env python
"""
_JobOutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id: JobOutput.py,v 1.1.2.44 2008/08/22 15:31:51 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.44 $"

import logging
import os
import traceback

# BossLite import
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Common.Exceptions import JobError
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
    def requestOutput(cls, job):
        """
        __requestOutput__

        request output for job.

        """

        # verify status
        if job.runningJob['processStatus'] != 'handled':
            logging.error(
                "Job %s is in status %s, cannot request output" % \
                ( cls.fullId( job ), job.runningJob['processStatus']))
            return

        job.runningJob['processStatus'] = 'output_requested'

        # commit and close session
        try :
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])
            bossLiteSession.updateDB( job.runningJob )
        except JobError, err:
            logging.error("Job %s: output for cannot be requested : %s" % \
                          (cls.fullId( job ), str( err ) ) )
        except Exception, err:
            logging.error(
                "Job %s: Unknown Error, output cannot be requested : %s" % \
                          (cls.fullId( job ), str( err ) ) )

        logging.debug("Job %s: getoutput request successfully enqueued" % \
                      cls.fullId( job ) )

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

            logging.debug("Job %s: Getting output" % cls.fullId( job ) )

            # open database
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])

            # verify the status
            status = job.runningJob['processStatus']

            # output retrieved before, then recover interrupted operation
            if status == 'output_retrieved':
                logging.warning("Job %s: Enqueuing previous ouput" % \
                                cls.fullId( job ) )
                return job

            # non expected status, abandon processing for job
            if status != 'in_progress' and status != 'failed':
                logging.error("Job %s: Cannot get output, status is %s" % \
                              (cls.fullId( job ), status) )
                return

            # inconsistent status
            if status == 'in_progress' and job.runningJob['closed'] == 'Y':
                logging.warning(
                    "Job %s in status %s: Enqueuing previous output" % \
                    (cls.fullId( job ), status) )
                job.runningJob['processStatus'] = 'output_retrieved'
                bossLiteSession.updateDB( job )
                return job

            schedSession = None
            try:
                # both for failed and done, a scheduler instance is needed:
                task = bossLiteSession.getTaskFromJob( job )
                schedulerConfig = { 'timeout' : 300 }
                schedSession = BossLiteAPISched( bossLiteSession, \
                                                 schedulerConfig, task )

                # build needed output directory
                job.runningJob['outputDirectory'] = cls.buildOutdir(job, task)

                # job failed: perform postMortem operations and notify failure
                if status == 'failed':
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
                logging.error('Job %s: Can not get scheduler : [%s]' % \
                              (cls.fullId( job ), str(err) ))

            # update
            #try:
            #    bossLiteSession.updateDB( job )
            #except JobError, msg:
            #    logging.error("WARNING, job %s.%s UPDATE failed: %s" % \
            #                  (job['taskId'], job['jobId'], str(msg) ) )

            # return job info
            #return job

        # thread has failed
        except Exception, ex :

            # show error message
            logging.error(
                'Job %s: GetOutputThread exception: [%s]\nTraceback: %s' % \
                ( cls.fullId( job ), str(ex), str( traceback.format_exc() ) ) )

        # thread has failed
        except :

            # show error message
            logging.error( "Job %s: GetOutputThread traceback: %s" % \
                           ( cls.fullId( job ), traceback.format_exc() ) )

        # return also the id
        # return job


    @classmethod
    def handleFailed(cls, job, task, schedSession ):
        """
        __handleFailed__

        perform postmortem and archive for failed jobs

        """

        if len( task.jobs ) != 1 :
            logging.error( "ERROR: too many jobs loaded %s" % len( task.jobs ))
            return

        if id( task.jobs[0] ) != id( job ) :
            logging.error( "Fatal ERROR: mismatching job" )
            return

        try:
            outfile = job.runningJob['outputDirectory'] + '/loggingInfo.log'
            schedSession.postMortem( task, outfile = outfile )
            logging.info('Job %s: Retrieved logging info in %s' \
                         % (cls.fullId( job ), outfile ))
        except BossLiteError, err:
            logging.info( 'Job %s: Can not get logging info : [%s]' % \
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

        if len( task.jobs ) != 1 :
            logging.error( "ERROR: too many jobs loaded %s" % len( task.jobs ))
            return

        if id( task.jobs[0] ) != id( job ) :
            logging.error( "Fatal ERROR: mismatching job" )
            return

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
            logging.warning( "Job %s: Warning, failed to purge : %s" \
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

        if len( task.jobs ) != 1 :
            logging.error( "ERROR: too many jobs loaded %s" % len( task.jobs ))
            return

        if id( task.jobs[0] ) != id( job ) :
            logging.error( "Fatal ERROR: mismatching job" )
            return

        #  get output, trying at most maxGetOutputAttempts
        retry = 0

        while retry < int( cls.params['maxGetOutputAttempts'] ):
            retry += 1

            logging.info("Job %s: retrieval attempt %d" % \
                         (cls.fullId( job ), retry))

            #  perform get output operation
            try:
                outdir = job.runningJob['outputDirectory']
                schedSession.getOutput( task, outdir=outdir)
                job.runningJob['processStatus'] = 'output_retrieved'

                logging.info('Job %s: Retrieved output in %s' % \
                             (cls.fullId( job ), outdir ))

                # log warnings and errors collected by the scheduler session
                log = str(schedSession.getLogger())
                if log is not None:
                    logging.info( log )

                # success: stop processing
                break

            # scheduler interaction error
            except BossLiteError, err:
                logging.error("Job %s: retrieval failed: %s" % \
                              (cls.fullId( job ), str(err) ) )
                logging.error("%s" % job)
                logging.error("%s" % job.runningJob)
                logging.info( "BossLiteLogger : %s " % \
                              str(schedSession.getLogger()) )

                logging.error( str( traceback.format_exc() ) )
                # proxy expired: invalidate job and empty return
                if err.value.find( "Proxy Expired" ) != -1 :
                    job.runningJob['closed'] = 'Y'
                    return

                # purged: probably already retrieved. Archive
                elif err.message().find( "has been purged" ) != -1 :
                    job.runningJob['status'] = 'E'
                    job.runningJob['statusScheduler'] = 'Cleared'
                    job.runningJob['closed'] = 'Y'
                    job.runningJob['processStatus'] = 'output_retrieved'
                    break

                # not ready for GO: waiting for next round
                elif err.message().find( "Job current status doesn" ) != -1:
                    logging.error( 
                        "Job %s in status %s: waiting next round" % \
                        (cls.fullId( job ), job.runningJob['status'])
                        )
                    return

                else :
                    # oops: What to do?!?!
                    logging.error("Job %s: no action taken: [%s]" % \
                                  (cls.fullId( job ), str(err) ) )
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
                pool.enqueue(job, job)

            except Exception, err:
                logging.error( "Job %s: failed restoring : %s" % \
                               (cls.fullId( job ), str(err) ) )
            except:
                logging.error( "Job %s: failed restoring: %s" % \
                               (cls.fullId( job ), \
                                str( traceback.format_exc() ) ) )

        logging.debug("Recreated %s get output requests" % numberOfJobs)


    @classmethod
    def setDoneStatus(cls, job):
        """
        __setDoneStatus__

        signal finished status for get output operation

        """

        logging.debug("Job %s: set done status" % cls.fullId( job ) )

        try :
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])

            # update job status
            job['processStatus'] = 'processed'
            bossLiteSession.updateDB( job )
        except DbError, msg:
            logging.error('Job %s: Error updating DB : %s ' % str(msg))

        except Exception, msg:
            logging.error('Job %s: Unknown Error updating DB : %s ' % \
                          (cls.fullId( job ), str(msg)))

        logging.debug("Job %s: Output processing done" % cls.fullId( job ) )



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
        logging.info("Job %s: Creating directory %s" % \
                     (cls.fullId( job ), outdir))
        try:
            os.makedirs( outdir )
        except OSError, err:
            if  err.errno == 17:
                # existing dir
                pass
            else :
                logging.error("Job %s: Cannot create directory %s : %s" % \
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

        return str( job['taskId'] ) + '.' \
               + str( job['jobId'] ) + '.' \
               + str( job['submissionNumber'] )
