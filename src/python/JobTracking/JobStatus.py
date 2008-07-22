#!/USr/bin/env python
"""
_JobStatus_

Deals with job status operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__revision__ = "$Id: JobStatus.py,v 1.1.2.32 2008/07/14 17:37:15 gcodispo Exp $"
__version__ = "$Revision: 1.1.2.32 $"

from JobTracking.TrackingDB import TrackingDB
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLitePoolDB import BossLitePoolDB
from ProdCommon.BossLite.Common.Exceptions import DbError, TaskError, TimeOut
#from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
#from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.Common.System import executeCommand

import traceback
import logging
from time import sleep


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
            bossSession = BossLiteAPI("MySQL", pool=cls.params['sessionPool'])
            db = TrackingDB( bossSession.bossLiteDB )
            tasks = db.getGroupTasks(group)

            for taskId in tasks :
                cls.bossQuery( bossSession, int(taskId) )

        except DbError, ex:
            logging.error( "JobTrackingThread exception: %s" % ex )

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
                        'closed' : 'N'}
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
                # schedulerConfig = { 'timeout' : len( task.jobs ) * 30 }
                #
                # schedSession = \
                #        BossLiteAPISched( bossSession, schedulerConfig, task )
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

                # retrieve scheduler
                # FIXME : to be replaced with a task specific field
                scheduler = None
                for job in task.jobs :
                    if job.runningJob['scheduler'] is not None:
                        scheduler = job.runningJob['scheduler']
                        break

                if scheduler is None:
                    logging.error( 'Unable to retrieve Scheduler, ' + \
                                   'skip check for jobs ' +  jobRange + \
                                   ' of task ' + str(taskId) )
                    continue
                    

                command = \
                        'python ' + \
                        '$PRODAGENT_ROOT/lib/JobTracking/QueryStatus.py ' + \
                        str(taskId) + ' ' + jobRange + ' ' + scheduler + \
                        ' ' + task['user_proxy']

                logging.debug('EXECUTING: ' + str(command))
                msg = executeCommand( command, len( task.jobs ) * 30 )
                logging.debug( "SUBPROCESS MESSAGE : \n" + msg )

                # log the end of the query
                logging.info('LB status retrieved for jobs ' \
                             + jobRange + ' of task ' + str(taskId) )
                del task, msg, command

            except MemoryError, e:
                logging.fatal("PROBLEM!!! " + \
                         "Memory run out trying to retrieve status for jobs " \
                              + jobRange + ' of task ' + str(taskId) \
                              + ' : ' + str( e ) )
                logging.error( "JobTrackingThread exception: %s" \
                               % str( traceback.format_exc() ) )
                break

            except TimeOut, e:
                logging.error("Failed to retrieve status for jobs " \
                              + jobRange + ' of task ' + str(taskId) \
                              + ' : ' + str( e ) )
                logging.error( "PARTIAL SUBPROCESS MESSAGE : \n" \
                               + e.commandOutput() )
                offset += int(cls.params['jobsToPoll'])

            except TaskError, e:
                logging.error("Failed to retrieve status for jobs " \
                              + jobRange + ' of task ' + str(taskId) \
                              + ' : ' + str( e ) )
                offset += int(cls.params['jobsToPoll'])

            except Exception, e:
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

            session = BossLitePoolDB( "MySQL", pool=cls.params['sessionPool'] )
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

        except DbError, ex:
            logging.error( 'Failed to remove jobs from queues: %s ' % ex )

        except Exception, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )


    @classmethod
    def removeFinishedJobs(cls, group):
        """
        remove all finished jobs from a specific group.

        jobs assigned: all jobs in the group

        """

        try:
            session = BossLitePoolDB( "MySQL", pool=cls.params['sessionPool'] )
            db = TrackingDB( session )
            joblist = db.getAssociatedJobs()

            # in case of empty results
            if joblist is None:
                logging.debug(
                    "No finished jobs to be removed from query queues" )
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

        except DbError, ex:
            logging.error( 'Failed to remove jobs from queues: %s ' % ex )

        except Exception, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )

