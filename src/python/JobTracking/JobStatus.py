#!/USr/bin/env python
"""
_JobStatus_

Deals with job status operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__revision__ = "$Id: JobStatus.py,v 1.5 2008/09/24 09:09:39 gcodispo Exp $"
__version__ = "$Revision: 1.5 $"

import threading
from JobTracking.TrackingDB import TrackingDB
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLitePoolDB import BossLitePoolDB
from ProdCommon.BossLite.Common.Exceptions import BossLiteError, TimeOut
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

        logging.info("%s Getting job status for jobs in group %s" \
                     %( cls.fullId(), str(group) ) )

        try:
            # get DB sessions
            bossSession = BossLiteAPI("MySQL", pool=cls.params['sessionPool'])
            db = TrackingDB( bossSession.bossLiteDB )
            tasks = db.getGroupTasks(group)

            for taskId in tasks :
                cls.bossQuery( bossSession, int(taskId) )

        except BossLiteError, ex:
            logging.error( "%s JobTrackingThread exception: %s" \
                           %( cls.fullId(), ex ) )

        except Exception, ex:
            logging.error( "%s JobTrackingThread exception: %s" \
                           % ( cls.fullId(), str( traceback.format_exc() ) ) )

        sleep(cls.params['delay'])


    @classmethod
    def bossQuery( cls, bossSession, taskId ):
        """
        Perform the LB query through BOSS
        """

        logging.info('%s Retrieving status for jobs of task %s'  \
                     % ( cls.fullId(), str(taskId) ) )

        # default values
        offset = 0
        loop = True
        jobRange = ''
        runningAttrs = {'processStatus': '%handled',
                        'closed' : 'N'}
        jobsToPoll = cls.params['jobsToPoll']

        # get scheduler        
        db = TrackingDB( bossSession.bossLiteDB )
        scheduler = db.getTaskScheduler(taskId)
        if scheduler is None:
            logging.error(
                '%s Unable to retrieve Scheduler, skip check for task  %s' \
                % ( cls.fullId(), str(taskId) )
                )
            return
        del db

        # perform query
        while loop :
            try :
                task = bossSession.load(
                    taskId, runningAttrs=runningAttrs, \
                    strict=False, \
                    limit=jobsToPoll, offset=offset )

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

                command = \
                        'python ' + \
                        '$PRODAGENT_ROOT/lib/JobTracking/QueryStatus.py ' + \
                        str(taskId) + ' ' + jobRange + ' ' + scheduler + \
                        ' ' + task['user_proxy']

                logging.debug('%s EXECUTING: %s' \
                              % (cls.fullId(), str(command)))
                msg, ret = executeCommand( command, len( task.jobs ) * 30 )
                logging.debug( "%s SUBPROCESS MESSAGE : \n%s " % \
                               (cls.fullId(), msg ) )

                # log the end of the query
                logging.info('%s LB status retrieved for jobs %s of task %s' \
                             %(cls.fullId(), jobRange, str(taskId) ) )
                del task, msg, command

            except TimeOut, e:
                logging.error(
                    "%s Failed to retrieve status for jobs of task %s : %s" \
                    % (cls.fullId(), str(taskId), str( e ) ) )
                logging.error( "%s PARTIAL SUBPROCESS MESSAGE : \n%s" \
                               % (cls.fullId(),  e.commandOutput() ) )
                offset += int(cls.params['jobsToPoll'])

            except BossLiteError, e:
                logging.error(
                    "%s Failed to retrieve status for jobs of task %s : %s" \
                    % (cls.fullId(), str(taskId), str( e ) ) )
                offset += int(cls.params['jobsToPoll'])

            except Exception, e:
                logging.error(
                    "%s Failed to retrieve status for jobs of task %s : %s" \
                    % (cls.fullId(), str(taskId), str( e ) ) )
                logging.error( cls.fullId() + traceback.format_exc() )
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

        except BossLiteError, ex:
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
            logging.debug("Removed jobs from group %s" % str(group) )
            session.close()
            del( joblist )

        except BossLiteError, ex:
            logging.error( 'Failed to remove jobs from queues: %s ' % ex )

        except Exception, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )


    @classmethod
    def fullId( cls ):
        """
        __fullId__

        compose job primary keys in a string
        """

        return '[' + threading.currentThread().getName() + '] '
