#!/usr/bin/env python
"""
_GetOutputComponent_

"""

__version__ = "$Id: GetOutputComponent.py,v 1.16 2008/12/08 21:10:31 gcodispo Exp $"
__revision__ = "$Revision: 1.16 $"

import os
import logging
import traceback
import threading

# PA configuration
from ProdAgentDB.Config import defaultConfig as dbConfig
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdCommon.Database import Session
from ProdAgent.WorkflowEntities import JobState

# GetOutput
from GetOutput.JobOutput import JobOutput
from GetOutput.JobHandling import JobHandling

# BossLite support
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import BossLiteError, JobError

# Threads
from ProdCommon.ThreadTools.WorkQueue import WorkQueue

class GetOutputComponent:
    """
    _GetOutputComponent_

    Component that gets output from jobs

    """

    def __init__(self, **args):

        # set default values for parameters
        self.args = {}
        self.args.setdefault("PollInterval", 300)
        self.args.setdefault("ComponentDir", "/tmp")
        self.args.setdefault("JobTrackingDir", None)
        self.args.setdefault("GetOutputPoolThreadsSize", 5)
        self.args.setdefault("jobsToPoll", 1000)
        self.args.setdefault("OutputLocation", "local")
        self.args.setdefault("CacheDir", None)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("verbose", 1) # tmp for testing
        self.args.setdefault("configDir", None)
        self.args.setdefault('maxGetOutputAttempts', 3)
        self.args.setdefault('skipWMSAuth', None)
        self.args.update(args)

       # set up logging for this component
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'], \
                                                "ComponentLog")
        LoggingUtils.installLogHandler(self)

        logging.info("GetOutput Component Initializing...")

        # compute delay for get output operations
        delay = int(self.args['PollInterval'])
        if delay < 10:
            delay = 10 # a minimum value

        seconds = str(delay % 60)
        minutes = str((delay / 60) % 60)
        hours = str(delay / 3600)

        self.pollDelay = hours.zfill(2) + ':' + \
                         minutes.zfill(2) + ':' + \
                         seconds.zfill(2)

        # get configuration information
        if self.args["verbose"] == 1 :
            logging.getLogger().setLevel(logging.DEBUG)

        # temp for rebounce
        try :
            outputParams = {}
            outputParams["storageName"] = self.args["storageName"]
            outputParams["Protocol"]    = self.args["Protocol"]
            outputParams["storagePort"] = self.args["storagePort"]
        except KeyError:
            outputParams = None

        # initialize members
        self.ms = None
        self.database = dbConfig
        self.bossLiteSession = \
                             BossLiteAPI('MySQL', self.database, makePool=True)
        self.sessionPool = self.bossLiteSession.bossLiteDB.getPool()

        # set job handling parameters
        jobHandlingParams = {}
        jobHandlingParams['componentDir'] = self.args['JobTrackingDir']
        jobHandlingParams['CacheDir'] = self.args['CacheDir']
        jobHandlingParams['OutputLocation'] = self.args['OutputLocation']
        jobHandlingParams['OutputParams'] = outputParams

        # create pool thread for get output operations
        params = {}
        params['skipWMSAuth'] = self.args['skipWMSAuth']
        params['sessionPool'] = self.sessionPool
        params['jobHandlingParams'] = jobHandlingParams
        params['maxGetOutputAttempts'] = \
                                       int( self.args['maxGetOutputAttempts'] )

        logging.info("Number of threads : %s" % \
                     self.args["GetOutputPoolThreadsSize"])
        JobOutput.setParameters(params)
        self.pool = WorkQueue([JobOutput.doWork] * \
                              int(self.args["GetOutputPoolThreadsSize"]))

        # initialize job handling object
        jobHandlingParams['bossLiteSession'] = self.bossLiteSession
        logging.info("handleeee")
        self.jobHandling = JobHandling(jobHandlingParams)

        # recreate interrupted get output operations
        JobOutput.recreateOutputOperations(self.pool)

        # initialize Session
        Session.set_database(dbConfig)
        Session.connect()

        # some initializations
        self.jobLimit = int(self.args['jobsToPoll'])
        self.newJobs = []
        self.jobFinished = None
        self.finishedAttrs = {'processStatus': 'output_requested',
                              'closed' : 'N'}
        self.failedAttrs = { 'processStatus' : 'failed',
                             'closed' : 'N' }

        # component running, display info
        logging.info("GetOutput Component Started...")


    def __call__(self, event, payload):
        """
        __operator()__

        Respond to events to control debug level for this component

        """

        # process events
        if event == "GetOutputComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "GetOutputComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        elif event == "GetOutputComponent:pollDB":
            self.checkJobs()
            return

        # wrong event
        logging.info("Unexpected event %s(%s), ignored" % \
                     (str(event), str(payload)))
        return


    def checkJobs(self):
        """
        __checkJobs__

        Poll the DB for jobs to get output from.
        """

        # how many active threads?
        logging.debug("ACTIVE THREADS when starting poll cycle: %s" \
                     % threading.activeCount() )
        logging.debug(
            "JOBS IN QUEUE when starting poll cycle: PENDING %d, READY %d" \
            % (len(self.pool.callQueue), len(self.pool.resultsQueue) )
            )
        logging.info("Starting poll cycle")

        # process jobs having 'processStatus' : 'output_requested'
        self.pollJobs( self.finishedAttrs )

        logging.debug(
            "JOBS IN QUEUE when enqueued finished jobs: PENDING %d, READY %d" \
            % (len(self.pool.callQueue), len(self.pool.resultsQueue) )
            )

        # process outputs if ready
        loop = True
        while loop :
            loop = self.processOutput()
        logging.debug("Finished processing of outputs")

        logging.debug(
            "JOBS IN QUEUE when handled finished jobs: PENDING %d, READY %d" \
            % (len(self.pool.callQueue), len(self.pool.resultsQueue) )
            )

        # process jobs having 'processStatus' : 'failed',
        self.pollJobs( self.failedAttrs )

        # process failure reports if ready
        loop = True
        while loop :
            loop = self.processOutput()
        logging.debug("Finished processing of failed")

        # how many active threads?
        logging.debug("ACTIVE THREADS after processing: %s" \
                     % threading.activeCount() )
        logging.debug(
            "JOBS IN QUEUE when finished poll cycle: PENDING %d, READY %d" \
            % (len(self.pool.callQueue), len(self.pool.resultsQueue) )
            )

        # generate next polling cycle
        logging.info("Waiting %s for next get output polling cycle" % \
                     self.pollDelay)
        self.ms.publish("GetOutputComponent:pollDB", "", self.pollDelay)
        self.ms.commit()


    def pollJobs(self, runningAttrs, action=None ):
        """
        __pollJobs__

        basic structure for jobs polling

        """

        offset = 0
        loop = True

        while loop :

            logging.debug("Max jobs to be loaded %s:%s " % \
                         (str( offset ), str( offset + self.jobLimit) ) )

            self.newJobs = self.bossLiteSession.loadJobsByRunningAttr(
                runningAttrs=runningAttrs, \
                limit=self.jobLimit, offset=offset
                )

            logging.info("Polled jobs : " + str( len(self.newJobs) ) )

            # exit if no more jobs to query
            if self.newJobs == [] :
                loop = False
                break
            else :
                offset += self.jobLimit

            while self.newJobs != [] :

                try :
                    job = self.newJobs.pop()

                    if action is not None:
                        action( job )

                    job.runningJob['processStatus'] = 'in_progress'
                    self.bossLiteSession.updateDB( job )

                    self.pool.enqueue(job['id'], job)

                except BossLiteError, err:
                    logging.error( "failed request for job %s : %s" % \
                                   (JobOutput.fullId(job), str(err) ) )
                except Exception, err:
                    logging.error( "failed enqueue for job %s : %s" % \
                                   (JobOutput.fullId(job), str(err) ) )
                except:
                    logging.error( "failed enqueue job %s : %s" % \
                                   (JobOutput.fullId(job), \
                                    str( traceback.format_exc() ) ) )

                del( job )

            logging.debug('Finished enqueuing polled Jobs in threads')

            del self.newJobs[:]

        del self.newJobs[:]


    def processOutput(self):
        """
        __processOutput__
        """

        # take a job from the work queue
        self.jobFinished = None
        try:
            self.jobFinished = self.pool.dequeue()
        except Exception, err:
            logging.error( "failed in dequeue %s" % str(err) )
        except :
            logging.error( "failed in dequeue %s" % \
                           str( traceback.format_exc() ) )

        # no more jobs
        if self.jobFinished is None :
            logging.error( "No jobs to dequeue" )
            return False

        # bad entry
        elif self.jobFinished[1] is None:
            logging.error( "%s is Error" % \
                           JobOutput.fullId( self.jobFinished[1] ))
            return True

        # ok: job finished!
        else :
            job, success, reportfilename = self.jobFinished[1]
            if success :
                self.publishJobSuccess( job, reportfilename )
            else:
                self.publishJobFailed( job, reportfilename )
            logging.debug("%s: Finished" % JobOutput.fullId( job ) )

        return True


    def publishJobSuccess(self, job, reportfilename):
        """
        __publishJobSuccess__
        """

        # set failed job status
        if self.args['OutputLocation'] != "SE" :
            reportfilename = self.jobHandling.archiveJob(
                "Success", job, reportfilename )
        else :
            # archive job
            try :
                self.bossLiteSession.archive( job )
            except JobError:
                logging.error("Job %s : Unable to archive in BossLite" % \
                              JobOutput.fullId(job) )

        # publish success event
        self.ms.publish("JobSuccess", reportfilename)
        self.ms.commit()

        logging.info("Published JobSuccess with payload :%s" % \
                     reportfilename)
        self.notifyJobState(job)

        return


    def publishJobFailed(self, job, reportfilename):
        """
        __publishJobFailed__
        """

        # set failed job status
        if self.args['OutputLocation'] != "SE" :
            reportfilename = self.jobHandling.archiveJob(
                "Failed", job, reportfilename )
        else :
            # archive job
            try :
                self.bossLiteSession.archive( job )
            except JobError:
                logging.error("Job %s : Unable to archive in BossLite" % \
                              JobOutput.fullId(job) )

        # publish job failed event
        self.ms.publish("JobFailed", reportfilename)
        self.ms.commit()

        logging.info("Job %s : published JobFailed with payload: %s" % \
                     (JobOutput.fullId(job), reportfilename) )

        return


    def notifyJobState(self, job):
        """
        __notifyJobState__

        Notify the JobState DB of finished jobs
        """

        # set finished job state
        try:
            try:
                JobState.finished(job['name'])
                Session.commit()
            except :
                logging.warning(
                    "failed connection for JobState Notify, trying recovery" )
                self.recreateSession()
                JobState.finished(job['name'])
                Session.commit()

            logging.info("Job %s finished: JobState DB Notified" % \
                         JobOutput.fullId(job) )
        # error
        except Exception, ex:
            msg = "Error setting job state to finished for job: %s\n" \
                  % str(job['name'])
            msg += str(ex)
            logging.error(msg)
        except :
            msg = "Error setting job state to finished for job: %s\n" \
                  % str(job['name'])
            msg += traceback.format_exc()
            logging.error(msg)

        return


    def recreateSession(self):
        """
        __recreateSession__

        fix to recreate standard default session object
        """

        Session.set_database(self.database)

        # force a re connect operation
        try:
            Session.session['default']['connection'].close()
        except:
            pass
        Session.session = {}
        Session.set_database(self.database)
        Session.connect()


    def startComponent(self):
        """
        __startComponent__

        Start up this component

        """

        # create message server instances
        self.ms = MessageService()

        # register
        self.ms.registerAs("GetOutputComponent")

        # subscribe to messages
        self.ms.subscribeTo("GetOutputComponent:StartDebug")
        self.ms.subscribeTo("GetOutputComponent:EndDebug")
        self.ms.subscribeTo("GetOutputComponent:pollDB")

        # generate first polling cycle
        self.ms.remove("GetOutputComponent:pollDB")
        self.ms.publish("GetOutputComponent:pollDB", "")
        self.ms.commit()

        # wait for messages
        logging.info("waiting for mexico" )
        while True:

            # get a message
            mtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("GetOutputComponent: %s, %s" % (mtype, payload))

            # process it
            self.__call__(mtype, payload)

