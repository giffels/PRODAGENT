#!/usr/bin/env python
"""
_Joboutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id: JobOutput.py,v 1.1.2.27 2008/05/12 12:38:30 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.27 $"

import logging
import os

# BossLite import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError
from ProdCommon.BossLite.Common.Exceptions import DbError

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
              'dbConfig' : None,
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
            logging.error("Job %s.%s is in status %s, cannot request output" \
                          % (job['taskId'], job['jobId'], job.runningJob['processStatus']))
            return

        job.runningJob['processStatus'] = 'output_requested'

        # commit and close session
        try :
            bossLiteSession = BossLiteAPI('MySQL', dbConfig)
            bossLiteSession.updateDB( job.runningJob )
        except JobError:
            logging.error("Output for job %s.%s cannot be requested" % \
                          (job['taskId'], job['jobId'] ) )

        logging.debug("getoutput request for %s.%s successfully enqueued" % \
                      (job['taskId'], job['jobId'] ) )

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
            schedSession = None
            try:
                task = bossLiteSession.loadTask(job['taskId'], False)
                if task['user_proxy'] is None:
                    task['user_proxy'] = ''
                task.appendJob( job )

                schedulerConfig = {'name' : job.runningJob['scheduler'],
                                   'user_proxy' : task['user_proxy'] ,
                                   'service' : job.runningJob['service'] }
                schedSession = BossLiteAPISched( bossLiteSession, \
                                                 schedulerConfig )

                # build needed output directory
                job.runningJob['outputDirectory'] = cls.buildOutdir(job, task)

            except SchedulerError, err:
                logging.error('Can not get scheduler for job %s.%s : [%s]' % \
                              (job['taskId'], job['jobId'], str(err)))
                return job

            except TaskError, err:
                logging.error('Can not get scheduler for job %s.%s : [%s]' % \
                              (job['taskId'], job['jobId'], str(err)))
                return job

            except Exception, err:
                logging.error('Can not handle job %s.%s : [%s]' % \
                              (job['taskId'], job['jobId'], str(err)))
                return job

            # job failed: perform postMortem operations and notify the failure
            if status == 'failed':
                job = cls.handleFailed( job, task, schedSession)

            #  get output, trying at most maxGetOutputAttempts
            elif cls.params['OutputLocation'] == 'SE':
                try:
                    schedSession.purgeService( task )
                except SchedulerError, msg:
                    output = str(msg)
                    job.runningJob['statusHistory'].append(output)
                    logging.warning("Warning: failed to purge job %s.%s : %s" \
                                    % (job['taskId'], job['jobId'], output ) )
                job.runningJob['processStatus'] = 'output_retrieved'
            else :
                job = cls.getOutput( job, task, schedSession)

            # log status & update
            try:
                bossLiteSession.updateDB( job )
            except JobError, msg:
                logging.error("WARNING, job %s.%s UPDATE failed: %s" % \
                              (job['taskId'], job['jobId'], str(msg) ) )


            # return job info
            return job

        # thread has failed
        except :

            import traceback
            msg = traceback.format_exc()

            # show error message
            msg = "GetOutputThread exception: %s" % str(msg)
            logging.error(msg)

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

        outfile = job.runningJob['outputDirectory'] + \
                  '/loggingInfo.log'
        try:
            schedSession.postMortem( task, outfile = outfile )
            job.runningJob['statusHistory'].append( \
                        'retrieved logging-info')
            logging.info('Retrieved logging info for job %s.%s in %s' \
                         % (job['taskId'], job['jobId'], outfile ))
        except SchedulerError, err:
            logging.info('Can not get logging info for job %s.%s' % \
                         (job['taskId'], job['jobId'] ))
            logging.info( '[%s]' % str(err) )
            job.runningJob['statusHistory'].append( \
                        'failed to retrieve logging-info')

        job.runningJob['processStatus'] = 'failure_handled'

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
            outdir = job.runningJob['outputDirectory']
            try:
                schedSession.getOutput( task, outdir=outdir)
                output = "output successfully retrieved"
                job.runningJob['statusHistory'].append(output)
                job.runningJob['processStatus'] = 'output_retrieved'

                logging.info('Retrieved output for job %s.%s in %s' % \
                             (job['taskId'], job['jobId'], outdir ))

                # success: stop processing
                break

            # scheduler interaction error
            except SchedulerError, msg:
                output = str(msg)
                job.runningJob['statusHistory'].append(output)
                logging.error("job %s.%s retrieval failed: %s" % \
                              (job['taskId'], job['jobId'], output ) )

                # proxy expired: skip!
                if output.find( "Proxy Expired" ) != -1 :
                    break

                # purged: probably already retrieved. Archive
                elif output.find( "has been purged" ) != -1 :
                    job.runningJob['status'] = 'E'
                    job.runningJob['statusScheduler'] = 'Cleared'
                    job.runningJob['closed'] = 'Y'
                    job.runningJob['processStatus'] = 'output_retrieved'
                    break

                # not ready for GO: waiting for next round
                elif output.find( "Job current status doesn" ) != -1:
                    job.runningJob['statusHistory'].append(
                        "waiting next round")
                    logging.error(
                        "waiting next round for job %s.%s in status %s" % \
                        (job['taskId'], job['jobId'], job.runningJob['status'])
                        )
                    break
                # not ready for GO: waiting for next round
                elif output.find( "Job current status doesn" ) :
                    job.runningJob['statusHistory'].append(
                        "waiting next round")
                    logging.error(
                        "empty outfile for job %s.%s: waiting next round" % \
                        (job['taskId'], job['jobId'])
                        )
                    break

            # oops: db error! What to do?!?!
            except TaskError, msg:
                output = str(msg)
                job.runningJob['statusHistory'].append(output)
                logging.error("job %s.%s retrieval failed: %s" % \
                              (job['taskId'], job['jobId'], output ) )
                break

            # as dirty as needed: any unknown error
            except :
                import traceback
                msg = traceback.format_exc()
                output = str(msg)
                job.runningJob['statusHistory'].append(output)
                logging.error("job %s.%s retrieval failed: %s" % \
                              (job['taskId'], job['jobId'], output ) )
                break

            # log status
            logging.info("job %s.%s retrieval status: %s" % \
                          (job['taskId'], job['jobId'], output))


        # return job
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
            bossLiteSession = BossLiteAPI('MySQL', dbConfig)

            # get interrupted operations
            jobs = bossLiteSession.loadJobsByRunningAttr(
                { 'processStatus' : 'in_progress' } )
            jobs.extend(
                bossLiteSession.loadJobsByRunningAttr(
                { 'processStatus' : 'output_retrieved' } )
                )
        except DbError, msg:
            logging.error('Error updating DB : %s ' % str(msg))

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
            bossLiteSession = BossLiteAPI('MySQL', dbConfig)

            # update job status
            job['processStatus'] = 'processed'
            bossLiteSession.updateDB( job )
        except DbError, msg:
            logging.error('Error updating DB : %s ' % str(msg))

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
        elif task['outputDirectory'] is not None \
               and task['outputDirectory'] != '' :
            outdir = task['outputDirectory']

        # fallback to the component directory
        else :
            outdir = cls.params['componentDir']

        # SE?
        if cls.params['OutputLocation'] == "SE" :
            outdir = cls.params['dropBoxPath'] + '/' + task['name'] + '_spec'

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
