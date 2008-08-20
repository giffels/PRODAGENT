#!/usr/bin/env python
"""
_JobOutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id: JobOutput.py,v 1.4 2008/08/20 12:08:48 gcodispo Exp $"
__revision__ = "$Revision: 1.4 $"

import logging
import os

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
                "Job %s.%s is in status %s, cannot request output" % \
                (job['taskId'], job['jobId'], job.runningJob['processStatus']))
            return

        job.runningJob['processStatus'] = 'output_requested'

        # commit and close session
        try :
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])
            bossLiteSession.updateDB( job.runningJob )
        except JobError, err:
            logging.error("Output for job %s.%s cannot be requested : %s" % \
                          (job['taskId'], job['jobId'], str( err ) ) )
        except Exception, err:
            logging.error("Unknown Error :Output for job %s.%s " + \
                          "cannot be requested : %s" % \
                          (job['taskId'], job['jobId'], str( err ) ) )

        logging.debug("getoutput request for %s.%s successfully enqueued" % \
                      (job['taskId'], job['jobId'] ) )

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

            logging.debug("Getting output for job %s.%s" % \
                          (job['taskId'], job['jobId']))

            # open database
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])

            # verify the status
            status = job.runningJob['processStatus']

            # output retrieved before, then recover interrupted operation
            if status == 'output_retrieved':
                logging.warning("Enqueuing previous ouput for job %s.%s" % \
                                (job['taskId'], job['jobId']))
                return job

            # non expected status, abandon processing for job
            if status != 'in_progress' and status != 'failed':
                logging.error("Cannot get output for job %s.%s, status is %s" \
                              % (job['taskId'], job['jobId'], status) )
                return

            # inconsistent status
            if status == 'in_progress' and job.runningJob['closed'] == 'Y':
                logging.warning("Enqueuing previous ouput for job %s.%s" % \
                                (job['taskId'], job['jobId']))
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
                elif cls.params['OutputLocation'] == 'SE':
                    cls.purgeService( job, task, schedSession)

                # get output, trying at most maxGetOutputAttempts
                else :
                    job = cls.getOutput( job, task, schedSession)

            except BossLiteError, err:
                logging.error('Can not get scheduler for job %s.%s : [%s]' % \
                              (job['taskId'], job['jobId'], str(err) ))

            # update
            try:
                bossLiteSession.updateDB( job.runningJob )
            except JobError, msg:
                logging.error("WARNING, job %s.%s UPDATE failed: %s" % \
                              (job['taskId'], job['jobId'], str(msg) ) )


            # return job info
            return job

        # thread has failed
        except Exception, ex :

            # show error message
            import traceback
            logging.error( '[%s]' % str(ex) )
            logging.error( "GetOutputThread exception: %s" % \
                           str( traceback.format_exc() ) )

            # return also the id
            return job


    @classmethod
    def handleFailed(cls, job, task, schedSession ):
        """
        __handleFailed__

        perform postmortem and archive for failed jobs

        """

        if len( task.jobs ) != 1 :
            logging.error( "ERROR: too many jobs loaded %s" % len( task.jobs ))
            return job

        if id( task.jobs[0] ) != id( job ) :
            logging.error( "Fatal ERROR: mismatching job" )
            return job

        try:
            outfile = job.runningJob['outputDirectory'] + '/loggingInfo.log'
            schedSession.postMortem( task, outfile = outfile )
            logging.info('Retrieved logging info for job %s.%s in %s' \
                         % (job['taskId'], job['jobId'], outfile ))
        except BossLiteError, err:
            logging.info('Can not get logging info for job %s.%s' % \
                         (job['taskId'], job['jobId'] ))
            logging.info( '[%s]' % str(err) )

            # proxy expired: invalidate job and empty return
            if err.value.find( "Proxy Expired" ) != -1 :
                job.runningJob['closed'] = 'Y'

        # log warnings and errors collected by the scheduler session
        logging.info( str(schedSession.getLogger()) )

        return job


    @classmethod
    def purgeService(cls, job, task, schedSession ):
        """
        __purgeService__

        clean up for jobs already retrieved

        """

        if len( task.jobs ) != 1 :
            logging.error( "ERROR: too many jobs loaded %s" % len( task.jobs ))
            return job

        if id( task.jobs[0] ) != id( job ) :
            logging.error( "Fatal ERROR: mismatching job" )
            return job

        try :
            statusSched = job.runningJob['status']
            schedSession.purgeService( task )
            if statusSched == 'UE' :
                job.runningJob['status'] = 'UE'
        except BossLiteError, err:
            logging.warning( "Warning: failed to purge job %s.%s : %s" \
                             % (job['taskId'], job['jobId'], str(err) ) )
            job.runningJob['processStatus'] = 'output_retrieved'
            
            # proxy expired: invalidate job and empty return
            if err.value.find( "Proxy Expired" ) != -1 :
                job.runningJob['closed'] = 'Y'

        # log warnings and errors collected by the scheduler session
        logging.info( str(schedSession.getLogger()) )

        return job


    @classmethod
    def getOutput(cls, job, task, schedSession ):
        """
        __getOutput__

        perform actual scheduler getOutput

        """

        if len( task.jobs ) != 1 :
            logging.error( "ERROR: too many jobs loaded %s" % len( task.jobs ))
            return job

        if id( task.jobs[0] ) != id( job ) :
            logging.error( "Fatal ERROR: mismatching job" )
            return job

        #  get output, trying at most maxGetOutputAttempts
        retry = 0
        output = ''

        while retry < int( cls.params['maxGetOutputAttempts'] ):
            retry += 1

            logging.info("job %s.%s retrieval attempt: %d" % \
                         (job['taskId'], job['jobId'], retry))


            #  perform get output operation
            try:
                outdir = job.runningJob['outputDirectory']
                schedSession.getOutput( task, outdir=outdir)
                output = "output successfully retrieved"
                job.runningJob['processStatus'] = 'output_retrieved'

                logging.info('Retrieved output for job %s.%s in %s' % \
                             (job['taskId'], job['jobId'], outdir ))

                # success: stop processing
                break

            # scheduler interaction error
            except BossLiteError, err:
                logging.error("job %s.%s retrieval failed: %s" % \
                              (job['taskId'], job['jobId'], str(err) ) )

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
                        "waiting next round for job %s.%s in status %s" % \
                        (job['taskId'], job['jobId'], job.runningJob['status'])
                        )
                    break

                else :
                    # oops: What to do?!?!
                    logging.error("no action taken for job %s.%s: [%s]" % \
                                  (job['taskId'], job['jobId'], str(err) ) )

            # log status
            logging.info("job %s.%s retrieval status: %s" % \
                          (job['taskId'], job['jobId'], output))

        # log warnings and errors collected by the scheduler session
        logging.info( str(schedSession.getLogger()) )

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

        try :
            bossLiteSession = \
                           BossLiteAPI('MySQL', pool=cls.params['sessionPool'])

            # update job status
            job['processStatus'] = 'processed'
            bossLiteSession.updateDB( job )
        except DbError, msg:
            logging.error('Error updating DB : %s ' % str(msg))

        except Exception, msg:
            logging.error('Unknown Error updating DB : %s ' % str(msg))

        logging.debug("Output processing done for job %s.%s" % \
                      (job['taskId'], job['jobId']))



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
            if cls.params['OutputLocation'] == "SE" :
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
        logging.info("Creating directory: " + outdir)
        try:
            os.makedirs( outdir )
        except OSError, err:
            if  err.errno == 17:
                # existing dir
                pass
            else :
                logging.error("Cannot create directory : " + str(err))
                raise err

        # return outdir
        return outdir
