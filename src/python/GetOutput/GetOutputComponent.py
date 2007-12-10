#!/usr/bin/env python
"""
_GetOutputComponent_

"""

__version__ = "$Id$"
__revision__ = "$Revision$"

import os
import logging
from copy import deepcopy

# PA configuration
from ProdAgentDB.Config import defaultConfig as dbConfig
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdCommon.Database import Session
from ProdCommon.Database.SafeSession import SafeSession
from ProdCommon.Database.MysqlInstance import MysqlInstance

# BOSS
from ProdAgentBOSS.BOSSCommands import BOSS

# GetOutput
from GetOutput.JobOutput import JobOutput
from GetOutput.TrackingDB import TrackingDB
from JobTracking.JobHandling import JobHandling

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
        self.args.setdefault("Logfile", None)
        self.args.setdefault("verbose", 0)
        self.args.setdefault("configDir", None)
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
        workingDir = self.args['ProdAgentWorkDir']
        workingDir = os.path.expandvars(workingDir)
        self.jobTrackingDir = self.args['JobTrackingDir']

        # get BOSS configuration, set directores and verbose mode
        self.bossCfgDir = self.args['configDir']
        self.componentDir = self.args["ComponentDir"]
        self.jobCreatorDir = \
                os.path.expandvars(self.args["JobCreatorComponentDir"])
        self.verbose = (self.args["verbose"] == 1)
        self.bossCfgDir = self.args['configDir']
        BOSS.setBossCfgDir(self.bossCfgDir)
        logging.info("Using BOSS configuration from " + self.bossCfgDir)

        # initialize members
        self.ms = None
        self.maxGetOutputAttempts = 3
        self.database = dbConfig
        self.bossDatabase = deepcopy(dbConfig)
        self.bossDatabase['dbName'] += "_BOSS"
        self.bossDatabase['dbType'] = 'mysql'
        self.activeJobs = self.bossDatabase['dbName'] + ".jt_activejobs"
        self.dbInstance = MysqlInstance(self.bossDatabase)

        # initialize job handling
        self.jobHandling = None

        # create pool thread for get output operations
        params = {}
        params['bossDB'] = self.bossDatabase['dbName']
        params['bossCfgDir'] = self.bossCfgDir
        params['maxGetOutputAttempts'] = 3
        params['dbInstance'] = self.dbInstance
        JobOutput.setParameters(params)
        self.pool = WorkQueue([JobOutput.doWork] * \
                              int(self.args["GetOutputPoolThreadsSize"]))

        # recreate interrupted get output operations
        JobOutput.recreateOutputOperations(self.pool)

        # initialize Session 
        Session.set_database(dbConfig)
        Session.connect()

        # component running, display info
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("GetOutput Component Started...")

    def __call__(self, event, payload):
        """
        _operator()_

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
        _checkJobs_

        Poll the DB for jobs to get output from.
        """

        logging.info("Starting poll cycle")

        # create database session
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # remove processed jobs
        db.removeJobs(status="output_processed")
        session.commit()

        # get jobs that require output
        outputRequestedJobs = db.getJobs(status="output_requested")
        numberOfJobs = len(outputRequestedJobs)

        if numberOfJobs != 0:
            
            # change status for jobs that require get output operations
            modified = db.setJobs(outputRequestedJobs, status='in_progress')
            if modified != numberOfJobs:
                logging.warning("Only %s of %s jobs  to 'in_process'" % \
                                (modified, numberOfJobs))

            # commit changes to database before starting any thread!
            session.commit()

            # start request output thread for them
            for job in outputRequestedJobs:
                self.pool.enqueue(job, job)

        logging.debug("Start processing of outputs")

        # process outputs if ready
        jobFinished = self.pool.dequeue()

        while jobFinished != None:

            # ignore non successful threads, error message was displayed
            if jobFinished[1] is None:
                continue

            # process output
            self.processOutput(db, jobFinished[1])
            session.commit()

            # get new work
            jobFinished = self.pool.dequeue()

        logging.debug("Finished processing of outputs")

        # close database
        session.close()

        # generate next polling cycle
        logging.info("Waiting %s for next get output polling cycle" % \
                     self.pollDelay)
        self.ms.publish("GetOutputComponent:pollDB", "", self.pollDelay)
        self.ms.commit()

    def processOutput(self, db, jobId):
        """
        _processOutput_
        """

        logging.debug("Processing output for job: %s" % jobId)

        # get job information
        jobInfo = db.getJobInfo(jobId)
        if jobInfo == {}:
            logging.error("No information available for job %s" % jobId)
            return

        # perform processing
        self.jobHandling.performOutputProcessing(jobInfo)

        # update status
        modified = db.setJobInfo(jobId, status = 'output_processed')
        if modified != 1:
            logging.error("Cannot set processed status for job %s" % jobId)
            return

        logging.debug("Processing output for job %s finished" % jobId)

    def startComponent(self):
        """
        _startComponent_

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
        params['bossCfgDir'] = self.bossCfgDir
        params['baseDir'] = self.jobTrackingDir
        params['jobCreatorDir'] = self.jobCreatorDir
        params['usingDashboard'] = None
        params['messageServiceInstance'] = self.ms
        self.jobHandling = JobHandling(params)

        # wait for messages
        while True:

            # get a message
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("GetOutputComponent: %s, %s" % (type, payload))

            # process it
            self.__call__(type, payload)

