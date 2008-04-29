#!/usr/bin/env python
"""
_GetOutputComponent_

"""

__version__ = "$Id: GetOutputComponent.py,v 1.1.2.14 2008/04/24 09:23:59 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.14 $"

import os
import logging
from copy import deepcopy

# PA configuration
from ProdAgentDB.Config import defaultConfig as dbConfig
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdCommon.Database import Session

# GetOutput
from GetOutput.JobOutput import JobOutput
from JobTracking.JobHandling import JobHandling

# BossLite support 
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

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
        self.args.setdefault("PollInterval", 10)
        self.args.setdefault("ComponentDir", "/tmp")
        self.args.setdefault("JobTrackingDir", None)
        self.args.setdefault("GetOutputPoolThreadsSize", 5)
        self.args.setdefault("OutputLocation", "SE")
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
        self.jobTrackingDir = self.args['JobTrackingDir']
        self.componentDir = self.args["ComponentDir"]
        self.jobCreatorDir = self.componentDir # fix for boss lite
        self.outputLocation = self.args['OutputLocation']
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
        self.database = deepcopy(dbConfig)
        self.database['dbType'] = 'MySQL'
        self.bossLiteSession = BossLiteAPI('MySQL', self.database)

        # initialize job handling
        self.jobHandling = None

        # create pool thread for get output operations
        params = {}
        params['componentDir'] = self.jobTrackingDir
        params['dbConfig'] = self.database
        params['OutputLocation'] = self.outputLocation
        params['dropBoxPath'] = self.args['dropBoxPath']
        params['maxGetOutputAttempts'] = \
                                       int( self.args['maxGetOutputAttempts'] )

        JobOutput.setParameters(params)
        self.pool = WorkQueue([JobOutput.doWork] * \
                              int(self.args["GetOutputPoolThreadsSize"]))

        # recreate interrupted get output operations
        JobOutput.recreateOutputOperations(self.pool)

        # initialize Session 
        Session.set_database(dbConfig)
        Session.connect()

        # some initializations
        self.outputRequestedJobs = []
        self.jobFinished = []

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
        logging.info("Unexpected event %s(%), ignored" % \
                     (str(event), str(payload)))
        return


    def checkJobs(self):
        """
        __checkJobs__

        Poll the DB for jobs to get output from.
        """

        logging.info("Starting poll cycle")

        # get jobs that require output
        logging.debug("Start processing of outputs")
        self.outputRequestedJobs = self.bossLiteSession.loadJobsByRunningAttr(
            { 'processStatus' : 'output_requested' } )
        numberOfJobs = len(self.outputRequestedJobs)
        logging.info("Output requested for " + str( numberOfJobs ) + " jobs")

        while self.outputRequestedJobs != [] :
            
            # change status for jobs that require get output operations
            # TODO here was good the compund update... to be reimplemented
            job = self.outputRequestedJobs.pop()
            job.runningJob['processStatus'] = 'in_progress'
            self.bossLiteSession.updateDB( job )
            self.pool.enqueue(job, job)
            del( job )

        del self.outputRequestedJobs[:]
        logging.debug("Start processing of failed")

        # get jobs failed that require post-mortem operations
        self.outputRequestedJobs = self.bossLiteSession.loadJobsByRunningAttr(
            { 'processStatus' : 'failed' } )
        numberOfJobs = len(self.outputRequestedJobs)
        logging.info("Notify failure for " + str( numberOfJobs ) + " jobs")

        while self.outputRequestedJobs != [] :
            
            # change status for jobs that require get output operations
            # TODO here was good the compund update... to be reimplemented
            job = self.outputRequestedJobs.pop()
            self.pool.enqueue(job, job)
            del( job )

        del self.outputRequestedJobs[:]

        # process outputs if ready
        self.jobFinished = self.pool.dequeue()

        while self.jobFinished != None:

            # ignore non successful threads, error message was displayed
            if self.jobFinished[1] is None:
                continue

            # process output
            self.processOutput(self.jobFinished[1])

            # get new work
            self.jobFinished = self.pool.dequeue()

        logging.debug("Finished processing of outputs and failed")


        # generate next polling cycle
        logging.info("Waiting %s for next get output polling cycle" % \
                     self.pollDelay)
        self.ms.publish("GetOutputComponent:pollDB", "", self.pollDelay)
        self.ms.commit()


    def processOutput(self, job):
        """
        __processOutput__
        """

        logging.debug("Processing output for job: %s.%s" % \
                      ( job['jobId'], job['taskId'] ) )

        # perform processing
        self.jobHandling.performOutputProcessing(job)

        # update status
        job.runningJob['processStatus'] = 'processed'
        self.bossLiteSession.updateDB( job )
        logging.debug("Processing output for job %s.%s finished" % \
                      ( job['jobId'], job['taskId'] ) )


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
        params['baseDir'] = self.jobTrackingDir
        params['jobCreatorDir'] = self.jobCreatorDir
        params['usingDashboard'] = None
        params['messageServiceInstance'] = self.ms
        params['OutputLocation'] = self.outputLocation
        params['OutputParams'] = self.outputParams
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

