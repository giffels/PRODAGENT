#!/USr/bin/env python
"""
_JobStatus_

Deals with job status operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__revision__ = "$Id: JobStatus.py,v 1.1.2.25 2008/05/27 08:20:32 gcodispo Exp $"
__version__ = "$Revision: 1.1.2.25 $"

from JobTracking.TrackingDB import TrackingDB
from ProdCommon.BossLite.API.BossLiteAPI import parseRange
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteDB import  BossLiteDB
from ProdCommon.BossLite.Common.Exceptions import TaskError
#from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
#from ProdCommon.BossLite.Common.Exceptions import SchedulerError

import traceback
import logging
from time import sleep
from os import popen4


###############################################################################
# Class: JobStatus                                                            #
###############################################################################

class JobStatus:
    """
    A static instance of this class deals with job status operations
    """

    params = {'delay' : 30, 'jobsToPoll' : 100}     # parameters

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
    def doWork(cls, group):
        """
        get the status of the jobs in the group.

        jobs assigned: all jobs in the group.

        """

        logging.info("Getting job status for jobs in group " + str(group))

        try:
            # get DB sessions
            bossSession = BossLiteAPI( "MySQL", cls.params['dbConfig'] )
            db = TrackingDB( bossSession.bossLiteDB )
            tasks = db.getGroupTasks(group)

            for taskId in tasks :
                cls.bossQuery( bossSession, int(taskId) )

        except Exception, ex:
            logging.error( "JobTrackingThread exception: %s" \
                           % str( traceback.format_exc() ) )

        sleep(cls.params['delay'])


    @classmethod
    def bossQuery( cls, bossSession, taskId ):
        """
        Perform the LB query through BOSS
        """

        logging.info('Retrieving status for jobs of task ' + str(taskId) )

        # default values
        offset = 0
        loop = True
        jobRange = ''
        runningAttrs = {'processStatus': '%handled',
                        'closed' : 'N',
                        'submissionTime' : '20%'}
        jobsToPoll = cls.params['jobsToPoll']

        # perform query
        while loop :
            try :
                task = bossSession.load(
                    taskId, runningAttrs=runningAttrs, \
                    strict=False, \
                    limit=jobsToPoll, offset=offset )[0]

                if task.jobs == [] :
                    loop = False
                    break
                else:
                    offset += jobsToPoll

                if task['user_proxy'] is None :
                    task['user_proxy'] = ''

                # # this is the correct way...
                # Scheduler session
                # schedulerConfig = {'name' : task.jobs[0].runningJob['scheduler'],
                #                    'user_proxy' : task['user_proxy'],
                #                    'service' : task.jobs[0].runningJob['service'] }
                #
                # schedSession = BossLiteAPISched( bossSession, schedulerConfig )
                #
                # task = schedSession.query( task, queryType='parent' )
                #
                # for job in task.jobs :
                #     print job.runningJob['jobId'], \
                #           job.runningJob['schedulerId'], \
                #           job.runningJob['statusScheduler'], \
                #           job.runningJob['statusReason']

                # # this is workaround for the glite bug...
                jobRange = '%s:%s' % ( task.jobs[0]['jobId'], \
                                       task.jobs[-1]['jobId'] )

                command = \
                        'python ' + \
                        '$PRODAGENT_ROOT/lib/JobTracking/QueryStatus.py ' + \
                        str(taskId) + ' ' + jobRange + ' ' + \
                        task.jobs[0].runningJob['scheduler'] + ' ' + \
                        task['user_proxy']

                logging.debug('EXECUTING: ' + str(command))
                pin, pout = popen4( command )
                msg = pout.read()
                logging.debug( "SUBPROCESS MESSAGE : \n" + msg )

                # log the end of the query
                logging.info('LB status retrieved for jobs ' \
                             + jobRange + ' of task ' + str(taskId) )
                del task, msg, pin, pout, command

            except MemoryError, e:
                logging.fatal("PROBLEM!!! " + \
                         "Memory run out trying to retrieve status for jobs " \
                              + jobRange + ' of task ' + str(taskId) \
                              + ' : ' + str( e ) )
                logging.error( "JobTrackingThread exception: %s" \
                               % str( traceback.format_exc() ) )
                break

            except TaskError, e:
                logging.error("Failed to retrieve status for jobs " \
                              + jobRange + ' of task ' + str(taskId) \
                              + ' : ' + str( e ) )
                offset += int(cls.params['jobsToPoll'])

            except StandardError, e:
                logging.error("Failed to retrieve status for jobs " \
                              + jobRange + ' of task ' + str(taskId) \
                              + ' : ' + str( e ) )
                logging.error( traceback.format_exc() )
                offset += int(cls.params['jobsToPoll'])


    @classmethod
    def addNewJobs(cls):
        """
        include new jobs in the set of jobs to be watched for.

        jobs assigned: all new jobs.

        """

        try:

            session = BossLiteDB ("MySQL", cls.params['dbConfig'] )
            db = TrackingDB( session )
            joblist = db.getUnassociatedJobs()

            # in case of empty results
            if joblist is None:
                logging.debug( "No new jobs to be added in query queues")
                return

            for pair in joblist:
                db.addForCheck( pair[0],  pair[1] )

                #logging.debug(\
                #    "Adding jobs to queue with BOSS id "\
                #    +  str( pair[0] ) + '.' + str( pair[1] )\
                #    )
            session.close()
            del( joblist )

        except StandardError, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )


    @classmethod
    def removeFinishedJobs(cls, group):
        """
        remove all finished jobs from a specific group.

        jobs assigned: all jobs in the group

        """

        try:
            session = BossLiteDB ("MySQL", cls.params['dbConfig'] )
            db = TrackingDB( session )
            joblist = db.getAssociatedJobs()

            # in case of empty results
            if joblist is None:
                logging.debug( "No finished jobs to be removed from query queues")
                return

            for pair in joblist:
                db.removeFromCheck( group, pair[0],  pair[1],  )
                #logging.debug(
                #    "Removing jobs for group " + str(group) \
                #    + " with BOSS id " +  str( pair[0] ) + '.' \
                #    + str( pair[1] )\
                #    )
            session.close()
            del( joblist )
        except StandardError, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )

