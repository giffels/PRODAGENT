#!/usr/bin/env python
"""
_GetOutputComponent_

"""

__version__ = "$Id: GetOutputComponent.py,v 1.1.2.9 2008/04/18 16:09:06 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.9 $"

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
        #workingDir = self.args['ProdAgentWorkDir']
        #workingDir = os.path.expandvars(workingDir)
        self.jobTrackingDir = self.args['JobTrackingDir']

        # get BOSS configuration, set directores and verbose mode
        self.componentDir = self.args["ComponentDir"]
        self.jobCreatorDir = self.componentDir # fix for boss lite
        self.verbose = (self.args["verbose"] == 1)

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
        params['maxGetOutputAttempts'] = 3

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
        outputRequestedJobs = self.bossLiteSession.loadJobsByRunningAttr(
            { 'processStatus' : 'output_requested' } )
        numberOfJobs = len(outputRequestedJobs)
        logging.info("Output requested for " + str( numberOfJobs ) + " jobs")

        if numberOfJobs != 0:
            
            # change status for jobs that require get output operations
            # TODO here was good the compund update... to be reimplemented
            for job in outputRequestedJobs:
                job.runningJob['processStatus'] = 'in_progress'
                job.runningJob['outputDirectory'] = \
                                            self.jobHandling.buildOutdir( job )
                self.bossLiteSession.updateDB( job )
                self.pool.enqueue(job, job)

        logging.debug("Start processing of failed")

        # get jobs failed that require post-mortem operations
        outputRequestedJobs = self.bossLiteSession.loadJobsByRunningAttr(
            { 'processStatus' : 'failed' } )
        numberOfJobs = len(outputRequestedJobs)
        logging.info("Notify failure for " + str( numberOfJobs ) + " jobs")

        if numberOfJobs != 0:
            
            # change status for jobs that require get output operations
            # TODO here was good the compund update... to be reimplemented
            for job in outputRequestedJobs:
                job.runningJob['outputDirectory'] = \
                                            self.jobHandling.buildOutdir( job )
                self.bossLiteSession.updateDB( job )
                self.pool.enqueue(job, job)

        # process outputs if ready
        jobFinished = self.pool.dequeue()

        while jobFinished != None:

            # ignore non successful threads, error message was displayed
            if jobFinished[1] is None:
                continue

            # process output
            self.processOutput(jobFinished[1])

            # get new work
            jobFinished = self.pool.dequeue()

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

        ## temporary workaround for OSB rebounce # Fabio
        self.osbRebounce( job )
        ## 
        logging.debug("Processing output for job %s.%s finished" % \
                      ( job['jobId'], job['taskId'] ) )

    ######################
    # TODO remove this temporary workaround once the OSB bypass problem will be fixed # Fabio
    # This is a mess and must be removed ASAP # Fabio
    def osbRebounce( self, job ):
        from ProdCommon.Storage.SEAPI.SElement import SElement
        from ProdCommon.Storage.SEAPI.SBinterface import SBinterface
         
        localOutDir = job.runningJob['outputDirectory']
        localOutputTgz = [ localOutDir +'/'+ f.split('/')[-1] for f in job['outputFiles'] if '.tgz' in f ]
        localOutputTgz = [ f for f in localOutputTgz if os.path.exists(f) ]

        logging.info( 'REBOUNCE DBG %s, %s, %s'%(localOutDir, localOutputTgz,[ localOutDir +'/'+ f.split('/')[-1] for f in job['outputFiles'] ] ) )

        if len(localOutputTgz)==0:
            return   

        task = self.bossLiteSession.loadTask( job['taskId'] )
        logging.info("Output rebounce: %s.%s " %( job['jobId'], job['taskId'] ) )
        seEl = SElement(self.args["storageName"], self.args["Protocol"], self.args["storagePort"])
        loc = SElement("localhost", "local")

        ## copy ISB ##
        sbi = SBinterface( loc, seEl )
        filesToClean = []
        for filetocopy in localOutputTgz:
            source = os.path.abspath(filetocopy)
            dest = os.path.join(task['outputDirectory'], os.path.basename(filetocopy))
            try: 
                ## logging.info( 'REBOUNCE DBG %s, %s'%(source, dest) ) 
                sbi.copy( source, dest, task['user_proxy'])
                filesToClean.append(source)
            except Exception, e:
                logging.info("Output rebounce transfer fail for %s.%s: %s " %( job['jobId'], job['taskId'], str(e) ) )
                continue 

        logging.info("Output rebounce completed for %s.%s " %( job['jobId'], job['taskId'] ) )
        for filetoclean in filesToClean:
            try: 
                os.remove( filetoclean )   
                pass
            except Exception, e:
                logging.info("Output rebounce local clean fail for %s.%s: %s " %( job['jobId'], job['taskId'], str(e) ) )
                continue
        logging.info("Output rebounce clean for %s.%s " %( job['jobId'], job['taskId'] ) )
        return 
    ######################

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

