#!/usr/bin/env python
"""
_TrackingComponent_

Tracking component implemented with a pool of threads for paraller polling
of job information.

When a job is completed, this component publish one of two events:

JobSuccess - The job completed successfully, the job report is extracted
job and made available. The location of the job report is the payload
for the JobSuccess event.

JobFailure - The job failed in someway and is abandoned, debug information
is retrieved and made available. The location of the error report is the
payload of the JobFailure event

"""

__revision__ = "$Id: TrackingComponent.py,v 1.47.2.23 2008/04/24 09:54:28 gcodispo Exp $"
__version__ = "$Revision: 1.47.2.23 $"

import os
import os.path
import logging
from copy import deepcopy

# PA configuration
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentDB.Config import defaultConfig as dbConfig

## other dependencies
from GetOutput.JobOutput import JobOutput
from JobTracking.JobHandling import JobHandling

# to be substituted with a BossLite implementation
from ProdAgentBOSS.BOSSCommands import directDB
from GetOutput.TrackingDB import TrackingDB

# BossLite support 
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

# Threads pool
from JobTracking.PoolScheduler import PoolScheduler
from JobTracking.JobStatus import JobStatus
from ProdCommon.ThreadTools.WorkQueue import WorkQueue

###############################################################################
# Class: TrackingComponent                                                    #
###############################################################################

class TrackingComponent:
    """
    _TrackingComponent_

    Server that periodically polls the BOSSDB to search for completed jobs,
    generating JobSuccess or JobFailed events.

    """

    def __init__(self, **args):
       
        # set default values for parameters 
        self.args = {}
        self.args.setdefault("PollInterval", 10)
        self.args.setdefault("QueryInterval", 300)
        self.args.setdefault("jobsToPoll", 100)
        self.args.setdefault("ComponentDir", "/tmp")
        self.args.setdefault("configDir", None)
        self.args.setdefault("ProdAgentWorkDir", None)
        self.args.setdefault("PoolThreadsSize", 5)
        self.args.setdefault("GetOutputPoolThreadsSize", 5)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("verbose", 0)
        self.args.setdefault("JobCreatorComponentDir", None)
        self.args.setdefault("dashboardInfo", \
                             {'use' : 'True', \
                              'address' : 'lxgate35.cern.ch', \
                              'port' : '8884'})
        self.args.update(args)

        # set up logging for this component
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'], \
                                                "ComponentLog")
        LoggingUtils.installLogHandler(self)
        logging.getLogger().setLevel(logging.DEBUG)

        logging.info("JobTracking Component Initializing...")

        # compute delay for get output operations
        delay = int(self.args['PollInterval'])
        if delay < 1:
            delay = 1 # a minimum value

        seconds = str(delay % 60)
        minutes = str((delay / 60) % 60)
        hours = str(delay / 3600)

        self.pollDelay = hours.zfill(2) + ':' + \
                         minutes.zfill(2) + ':' + \
                         seconds.zfill(2)
        
        # get configuration information
        self.componentDir = self.args['ComponentDir']

        self.directory = self.args["ComponentDir"]
        if self.args["JobCreatorComponentDir"] is None:
            self.args["JobCreatorComponentDir"] = self.directory
        self.jobCreatorDir = \
                os.path.expandvars(self.args["JobCreatorComponentDir"])
        self.verbose = (self.args["verbose"] == 1)

        # initialize job handling
        self.jobHandling = None

        # compute delay for get output operations
        sleepTime = int(self.args['QueryInterval'])
        if sleepTime < 15:
            sleepTime = 15 # a minimum value

        # create pool thread
        params = {}
        params['delay'] = sleepTime
        params['jobsToPoll'] = int(self.args['jobsToPoll'])
        JobStatus.setParameters(params)
        pool = \
             WorkQueue([JobStatus.doWork] * int(self.args["PoolThreadsSize"]))

        # create pool scheduler
        params = {}
        params['delay'] = delay
        params['jobsToPoll'] = int( self.args['jobsToPoll'] )
        PoolScheduler(pool, params) 

        # initialize members
        self.ms = None
        self.maxGetOutputAttempts = 3
        self.database = deepcopy(dbConfig)

        # initialize Session
        self.bossLiteSession = BossLiteAPI('MySQL', self.database)

        # set parameters for getoutput operations
        params = {}
        params['componentDir'] = self.componentDir
        params['dbConfig'] = self.database
        JobOutput.setParameters(params)

        # check for dashboard usage
        self.usingDashboard = self.args['dashboardInfo']
        logging.debug("DashboardInfo = %s" % str(self.usingDashboard))

        # some initializations
        self.newJobs = []
        self.failedJobs = []
        self.finishedJobs = []
        self.counters = ['pending', 'submitted', 'waiting', 'ready', \
                         'scheduled', 'running', 'cleared', 'created', 'other']

        # component running, display info
        logging.info("JobTracking Component Started...")


    def __call__(self, event, payload):
        """
        __operator()__

        Respond to events to control debug level for this component

        """
        if event == "TrackingComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "TrackingComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        elif event == "TrackingComponent:pollDB":
            self.checkJobs()
            return

        # wrong event
        logging.info("Unexpected event %s(%), ignored" % \
                     (str(event), str(payload)))
        return


    def pollNewJobs(self):
        """
        __pollNewJobs__

        Poll the BOSS DB for new job ids and handle they registration

        Return a list of failed job ids
        """

        self.newJobs = self.bossLiteSession.loadJobsByRunningAttr(
            { 'processStatus' : 'not_handled' }
            )
        logging.info("new jobs : " + str( len(self.newJobs) ) )

        # for job in self.newJobs :
        while self.newJobs != [] :

            job = self.newJobs.pop()
            # FIXME: temp hack, to be removed as soon as BossLite is updated
            if job.runningJob['status'] == 'C' or \
                   job.runningJob['status'] == 'S' :
                del( job )
                continue
            
            ### TODO here was good the compound update... to be reimplemented
            job.runningJob['processStatus'] = 'handled'
            self.bossLiteSession.updateDB( job )
            # publish information to dashboard
            try:
                self.jobHandling.dashboardPublish( job )
            except StandardError, msg:
                logging.error("Cannot publish to dashboard:%s" % msg)
            del( job )

        del self.newJobs[:]

        # summary of the jobs in the DB
        # TODO : change with a BossLite call
        session = directDB.getDbSession()
        db = TrackingDB( session )
        result = db.getJobsStatistic()
        directDB.close( session )

        if result is not None:

            counter = {}
            for ctr in self.counters:
                counter[ctr] = 0

            for pair in result :
                status, count = pair
                if status == 'E':
                    continue
                elif status == 'R' :
                    counter['running'] = count
                elif status == 'I':
                    counter['pending'] = count
                elif status == 'SW' :
                    counter['waiting'] = count
                elif status == 'SR':
                    counter['ready'] = count
                elif status == 'SS':
                    counter['scheduled'] = count
                elif status == 'SU':
                    counter['submitted'] = count
                elif status == 'SE':
                    counter['cleared'] = count
                elif status == 'C':
                    counter['created'] = count
                else:
                    counter['other'] += count

            # display counters
            for ctr, value in counter.iteritems():
                logging.info(ctr + " jobs : " + str(value))
            logging.info("....................")

            del( result )


    def checkJobs(self):
        """
        __checkJobs__

        Poll the DB and call the appropriate handler method for each
        jobId that is returned.

        """

        # get finished and failed jobs and handle them
        self.handleFinished()
        self.handleFailed()
        
        # notify new jobs
        self.pollNewJobs()

        # generate next polling cycle
        logging.info("Waiting %s for next get output polling cycle" % \
                     self.pollDelay)
        self.ms.publish("TrackingComponent:pollDB", "", self.pollDelay)
        self.ms.commit()


    def handleFailed( self ):
        """
        __handleFailed__

        handle failed jobs

        """

        # query failed jobs
        self.failedJobs = self.bossLiteSession.loadFailed(
            { 'processStatus' : 'handled' }
            )
        logging.info("failed jobs : " + str( len(self.failedJobs) ) )

        # process all jobs
        # for job in self.failedJobs:
        while self.failedJobs != [] :

            job = self.failedJobs.pop()

            # publish information to the dashboard
            try:
                self.jobHandling.dashboardPublish(job)
            except StandardError, msg:
                logging.error("Cannot publish to dashboard:%s" % msg)


            # enqueue the get output operation
            logging.debug("Enqueing failure handling request for %s" % \
                          self.jobHandling.fullId(job))

            job.runningJob['processStatus'] = 'failed'
            self.bossLiteSession.updateDB( job.runningJob )
            del( job )
        del self.failedJobs[:]


    def handleFinished(self):
        """
        __handleFinished__

        handle finished jobs: retrieve output and notify execution
        failure or success

        """

        # query finished jobs
        self.finishedJobs = self.bossLiteSession.loadEnded(
            { 'processStatus' : 'handled' }
            )
        logging.info("finished jobs : " + str( len(self.finishedJobs) ) )

        # process all jobs
        # for job in self.finishedJobs:
        while self.finishedJobs != [] :

            job = self.finishedJobs.pop()

            # publish information to the dashboard
            try:
                self.jobHandling.dashboardPublish(job)
            except StandardError, msg:
                logging.error("Cannot publish to dashboard:%s" % msg)

            # enqueue the get output operation
            logging.debug("Enqueing getoutput request for %s" % \
                          self.jobHandling.fullId(job))

            JobOutput.requestOutput(job)
            del( job )

        del self.finishedJobs[:]


    def startComponent(self):
        """
        __startComponent__

        Start up this component, 

        """

        # create message server instances
        self.ms = MessageService()

        # register
        self.ms.registerAs("TrackingComponent")

        # subscribe to messages
        self.ms.subscribeTo("TrackingComponent:StartDebug")
        self.ms.subscribeTo("TrackingComponent:EndDebug")
        self.ms.subscribeTo("TrackingComponent:pollDB")

        # generate first polling cycle
        self.ms.remove("TrackingComponent:pollDB")
        self.ms.publish("TrackingComponent:pollDB", "")
        self.ms.commit()

        # initialize job handling object
        params = {}
        params['baseDir'] = self.componentDir
        params['jobCreatorDir'] = self.jobCreatorDir
        params['usingDashboard'] = self.usingDashboard
        params['messageServiceInstance'] = self.ms
        params['OutputLocation'] = None
        params['OutputParams'] = None
        self.jobHandling = JobHandling(params)

        # wait for messages
        while True:

            # get a message
            mtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (mtype, payload))

            # process it
            self.__call__(mtype, payload)


