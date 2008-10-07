#!/usr/bin/env python
"""
_GetOutputComponent_

"""

__version__ = "$Id: GetOutputComponent.py,v 1.8 2008/10/06 10:53:42 gcodispo Exp $"
__revision__ = "$Revision: 1.8 $"

import os
import logging
import traceback

# PA configuration
from ProdAgentDB.Config import defaultConfig as dbConfig
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdCommon.Database import Session

# GetOutput
from GetOutput.JobOutput import JobOutput
from GetOutput.JobHandling import JobHandling

# BossLite support
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import BossLiteError, DbError

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
        self.args.setdefault("dropBoxPath", None)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("verbose", 0)
        self.args.setdefault("configDir", None)
        self.args.setdefault('maxGetOutputAttempts', 3)
        self.args.update(args)

       # set up logging for this component
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'], \
                                                "ComponentLog")
        LoggingUtils.installLogHandler(self)
        logging.getLogger().setLevel(logging.DEBUG)

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
        #workingDir = self.args['ProdAgentWorkDir']
        #workingDir = os.path.expandvars(workingDir)
        self.verbose = (self.args["verbose"] == 1)

        # temp for rebounce
        try :
            self.outputParams = {}
            self.outputParams["storageName"] = self.args["storageName"]
            self.outputParams["Protocol"]    = self.args["Protocol"]
            self.outputParams["storagePort"] = self.args["storagePort"]
        except KeyError:
            self.outputParams = None

        # initialize members
        self.ms = None
        self.maxGetOutputAttempts = 3
        self.database = dbConfig
        self.bossLiteSession = \
                             BossLiteAPI('MySQL', self.database, makePool=True)
        self.sessionPool = self.bossLiteSession.bossLiteDB.getPool()

        # create pool thread for get output operations
        params = {}
        params['componentDir'] = self.args['JobTrackingDir']
        params['sessionPool'] = self.sessionPool
        params['OutputLocation'] = self.args['OutputLocation']
        params['dropBoxPath'] = self.args['dropBoxPath']
        params['maxGetOutputAttempts'] = \
                                       int( self.args['maxGetOutputAttempts'] )

        logging.info("Number of threads : %s" % \
                     self.args["GetOutputPoolThreadsSize"])
        JobOutput.setParameters(params)
        self.pool = WorkQueue([JobOutput.doWork] * \
                              int(self.args["GetOutputPoolThreadsSize"]))

        # recreate interrupted get output operations
        JobOutput.recreateOutputOperations(self.pool)

        # initialize Session
        Session.set_database(dbConfig)
        Session.connect()

        # some initializations
        self.jobLimit = int(self.args['jobsToPoll'])
        self.newJobs = []
        self.jobHandling = None
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
        import threading
        # how many active threads?
        logging.debug("ACTIVE THREADS when starting poll cycle: %s" \
                     % threading.activeCount() )
        logging.info("Starting poll cycle")

        # process jobs having 'processStatus' : 'output_requested'
        self.pollJobs( self.finishedAttrs )

        # process outputs if ready
        loop = True
        while loop :
            loop = self.processOutput(self.jobHandling.performOutputProcessing)
        logging.debug("Finished processing of outputs")

        # process jobs having 'processStatus' : 'failed',
        self.pollJobs( self.failedAttrs )

        # process failure reports if ready
        loop = True
        while loop :
            loop = self.processOutput(self.jobHandling.performErrorProcessing)
        logging.debug("Finished processing of failed")

        # how many active threads?
        logging.debug("ACTIVE THREADS after processing: %s" \
                     % threading.activeCount() )

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

                    self.pool.enqueue(job, job)

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

            del self.newJobs[:]

        del self.newJobs[:]


    def processOutput(self, action):
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
            logging.error( "%s: Error in dequeue" % \
                           JobOutput.fullId( self.jobFinished[0] ))
            return False

        # ok: job finished!
        else :
            job = self.jobFinished[1]
            logging.debug("%s: Processing output" % JobOutput.fullId( job ) )

        # perform processing
        try :
            action(job)

            # update status
            job.runningJob['processStatus'] = 'processed'
            job.runningJob['closed'] = 'Y'
            self.bossLiteSession.updateDB( job )
        except BossLiteError, err:
            logging.error( "%s failed to process output : %s" % \
                           (JobOutput.fullId( job ), str(err) ) )
        except Exception, err:
            logging.error( "%s failed to process output : %s" % \
                           (JobOutput.fullId( job ), str(err) ) )
        except :
            logging.error( "%s failed to process output : %s" % \
                           (JobOutput.fullId( job ), \
                            str( traceback.format_exc() )  ) )

        logging.debug("%s : Processing output finished" % \
                      JobOutput.fullId( job ) )

        return True


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

        # initialize job handling object
        params = {}
        params['baseDir'] = self.args['JobTrackingDir']
        params['jobCreatorDir'] = self.args["ComponentDir"]
        params['usingDashboard'] = None
        params['messageServiceInstance'] = self.ms
        params['OutputLocation'] = self.args['OutputLocation']
        params['OutputParams'] = self.outputParams
        params['bossLiteSession'] = self.bossLiteSession
        params['database'] = self.database
        logging.info("handleeee")
        self.jobHandling = JobHandling(params)

        # wait for messages
        logging.info("waiting for mexico" )
        while True:

            # get a message
            mtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("GetOutputComponent: %s, %s" % (mtype, payload))

            # process it
            self.__call__(mtype, payload)

