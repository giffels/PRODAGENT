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

__revision__ = "$Id: TrackingComponent.py,v 1.47.2.34 2008/07/01 13:38:00 gcodispo Exp $"
__version__ = "$Revision: 1.47.2.34 $"

import os
import os.path
import logging
from copy import deepcopy

# PA configuration
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentDB.Config import defaultConfig as dbConfig
from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo

## other dependencies
from GetOutput.JobOutput import JobOutput

# to be substituted with a BossLite implementation
from JobTracking.TrackingDB import TrackingDB

# BossLite support
from ProdCommon.BossLite.API.BossLiteDB import BossLiteDB
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
        self.args.setdefault("PollInterval", 300)
        self.args.setdefault("QueryInterval", 3)
        self.args.setdefault("jobsToPoll", 300)
        self.args.setdefault("ComponentDir", "/tmp")
        self.args.setdefault("configDir", None)
        self.args.setdefault("ProdAgentWorkDir", None)
        self.args.setdefault("PoolThreadsSize", 5)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("verbose", 0)
        self.args.setdefault("JobCreatorComponentDir", None)
        self.args.setdefault("dashboardInfo", \
                             {'use' : 'True', \
                              'address' : 'cms-jobmon.cern.ch', \
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
        if self.args["JobCreatorComponentDir"] is None:
            self.args["JobCreatorComponentDir"] = self.args["ComponentDir"]
        self.args["JobCreatorComponentDir"] = \
                        os.path.expandvars(self.args["JobCreatorComponentDir"])

        # compute delay for get output operations
        sleepTime = int(self.args['QueryInterval'])
        if sleepTime < 15:
            sleepTime = 15 # a minimum value

        # initialize members
        self.ms = None
        self.database = deepcopy(dbConfig)

        # create pool thread
        params = {}
        params['delay'] = sleepTime
        params['jobsToPoll'] = int(self.args['jobsToPoll'])
        params['dbConfig'] = self.database
        JobStatus.setParameters(params)
        pool = \
             WorkQueue([JobStatus.doWork] * int(self.args["PoolThreadsSize"]))

        # create pool scheduler
        params = {}
        params['delay'] = delay
        params['jobsToPoll'] = int( self.args['jobsToPoll'] )
        params['dbConfig'] = self.database
        PoolScheduler(pool, params)

        # initialize Session
        self.bossLiteSession = BossLiteAPI('MySQL', self.database)

        # set parameters for getoutput operations
        params = {}
        params['componentDir'] = self.args['ComponentDir']
        params['dbConfig'] = self.database
        JobOutput.setParameters(params)

        # check for dashboard usage
        self.usingDashboard = self.args['dashboardInfo']
        logging.debug("DashboardInfo = %s" % str(self.usingDashboard))

        # some initializations
        self.jobLimit = int(self.args['jobsToPoll'])
        self.newJobs = []
        self.failedJobs = []
        self.finishedJobs = []
        self.counters = ['pending', 'submitted', 'waiting', 'ready', \
                         'scheduled', 'running', 'cleared', 'created', 'other']

        # ended/failed attributes
        self.runningAttrs = { 'processStatus' : 'handled',
                              'closed' : 'N' }
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


    def checkJobs(self):
        """
        __checkJobs__

        Poll the DB and call the appropriate handler method for each
        jobId that is returned.

        """

        # log the status of the jobs
        self.getStatistic()

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


    def getStatistic(self):
        """
        __getStatistics__

        Poll the BOSS DB for a summary of the job status

        """

        # summary of the jobs in the DB
        db = TrackingDB( self.bossLiteSession.bossLiteDB )
        result = db.getJobsStatistic()

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


    def pollNewJobs(self):
        """
        __pollNewJobs__

        Poll the BOSS DB for new job ids and handle they registration

        """

        # loading attributes
        runningAttrs = { 'processStatus' : 'not_handled',
                         'closed' : 'N' }
        offset = 0
        loop = True

        while loop :

            logging.debug("Max new jobs to be loaded %s:%s " % \
                         (str( offset ), str( offset + self.jobLimit) ) )

            # self.newJobs = self.bossLiteSession.loadJobsByRunningAttr(
            #     { 'processStatus' : 'not_handled' }, \
            #     limit=self.jobLimit, offset=offset
            #     )

            self.newJobs = self.bossLiteSession.loadJobsByRunningAttr(
                runningAttrs=runningAttrs, \
                limit=self.jobLimit, offset=offset
                )

            logging.info("new jobs : " + str( len(self.newJobs) ) )

            # exit if no more jobs to query
            if self.newJobs == [] :
                loop = False
                break
            else :
                offset += self.jobLimit

            while self.newJobs != [] :

                job = self.newJobs.pop()

                # FIXME: temp hack
                if job.runningJob['status'] == 'C' or \
                       job.runningJob['status'] == 'S' :
                    del( job )
                    continue

                job.runningJob['processStatus'] = 'handled'
                self.bossLiteSession.updateDB( job )

                # publish information to dashboard
                try:
                    self.dashboardPublish( job )
                except StandardError, msg:
                    logging.error("Cannot publish to dashboard:%s" % msg)

                del( job )

            del self.newJobs[:]

        del self.newJobs[:]


    def handleFailed( self ):
        """
        __handleFailed__

        handle failed jobs

        """

        # loading attributes
        offset = 0
        loop = True

        while loop :

            logging.debug("Max failed jobs to be loaded %s:%s " % \
                         (str( offset ), str( offset + self.jobLimit) ) )

            # query failed jobs
            self.failedJobs = self.bossLiteSession.loadFailed(
                attributes=self.runningAttrs, \
                limit=self.jobLimit, offset=offset
                )
            logging.info("failed jobs : " + str( len(self.failedJobs) ) )

            # exit if no more jobs to query
            if self.failedJobs == [] :
                loop = False
                break
            else :
                offset += self.jobLimit

                # process all jobs
            while self.failedJobs != [] :

                job = self.failedJobs.pop()

                # publish information to the dashboard
                try:
                    self.dashboardPublish(job)
                except StandardError, msg:
                    logging.error("Cannot publish to dashboard:%s" % msg)


                # enqueue the get output operation
                logging.debug("Enqueing failure handling request for %s" % \
                              self.fullId(job))

                job.runningJob['processStatus'] = 'failed'
                self.bossLiteSession.updateDB( job.runningJob )
                del( job )

            del self.failedJobs[:]

        del self.failedJobs[:]


    def handleFinished(self):
        """
        __handleFinished__

        handle finished jobs: retrieve output and notify execution
        failure or success

        """

        # loading attributes
        offset = 0
        loop = True

        while loop :

            logging.debug("Max finished jobs to be loaded %s:%s " % \
                         (str( offset ), str( offset + self.jobLimit) ) )

            # query finished jobs
            self.finishedJobs = self.bossLiteSession.loadEnded(
                attributes=self.runningAttrs, \
                limit=self.jobLimit, offset=offset
                )
            logging.info("finished jobs : " + str( len(self.finishedJobs) ) )

            # exit if no more jobs to query
            if self.finishedJobs == [] :
                loop = False
                break
            else :
                offset += self.jobLimit

            # process all jobs
            # for job in self.finishedJobs:
            while self.finishedJobs != [] :

                job = self.finishedJobs.pop()

                # publish information to the dashboard
                try:
                    self.dashboardPublish(job)
                except StandardError, msg:
                    logging.error("Cannot publish to dashboard:%s" % msg)

                # enqueue the get output operation
                logging.debug("Enqueing getoutput request for %s" % \
                              self.fullId(job))

                JobOutput.requestOutput(job)
                del( job )

            del self.finishedJobs[:]

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

        # wait for messages
        while True:

            # get a message
            mtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (mtype, payload))

            # process it
            self.__call__(mtype, payload)


    def dashboardPublish(self, job):
        """
        __dashboardPublish__

        publishes dashboard info
        """

        # using Dashboard?
        if self.usingDashboard['use'] == "False" :
            return

        # initialize
        dashboardInfo = DashboardInfo()
        dashboardInfoFile = None

        # looking for dashboardInfoFile
        try :
            dashboardInfoFile = \
                              os.path.join(job.runningJob['outputDirectory'], \
                                           "DashboardInfo.xml" )
        except :
            pass

        # if the dashboardInfoFile is not there, this is a crab job
        if dashboardInfoFile is None or not os.path.exists(dashboardInfoFile):
            task = self.bossLiteSession.loadTask(job['taskId'], deep=False)
            dashboardInfo.task = task['name'][: task['name'].rfind('_')]
            dashboardInfo.job = str(job['jobId']) + '_' + \
                                job.runningJob['schedulerId']
            dashboardInfo['JSTool'] = 'crab'
            dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
            dashboardInfo['User'] = task['name'].split('_')[0]
            dashboardInfo['TaskType'] =  'analysis'

        # otherwise, ProdAgent job: everything is stored in the file
        else:
            try:
                # it exists, get dashboard information
                dashboardInfo.read(dashboardInfoFile)

            except StandardError, msg:
                # it does not work, abandon
                logging.error("Reading dashboardInfoFile " + \
                              dashboardInfoFile + " failed (jobId=" \
                              + str(job['jobId']) + ")\n" + str(msg))
                return

        # write dashboard information
        dashboardInfo['GridJobID'] = job.runningJob['schedulerId']

        try :
            dashboardInfo['StatusEnterTime'] = \
                                             str(job.runningJob['lbTimestamp'])
        except StandardError:
            pass

        try :
            dashboardInfo['StatusValue'] = job.runningJob['statusScheduler']
        except KeyError:
            pass

        try :
            dashboardInfo['StatusValueReason'] = job.runningJob['statusReason']
        except KeyError:
            pass

        try :
            dashboardInfo['StatusDestination'] = job.runningJob['destination']
        except KeyError:
            pass

        try :
            dashboardInfo['RBname'] = job.runningJob['service']
        except KeyError:
            pass

        ### # create/update info file
        ### logging.info("Creating dashboardInfoFile " + dashboardInfoFile )
        ### dashboardInfo.write( dashboardInfoFile )

        # set dashboard destination
        dashboardInfo.addDestination(
            self.usingDashboard['address'], self.usingDashboard['port']
            )

        # publish it
        try:
            # logging.debug("dashboardinfo: %s" % dashboardInfo.__str__())
            dashboardInfo.publish(5)
            logging.info("dashboard info sent for job %s" % self.fullId(job) )

        # error, cannot publish it
        except StandardError, msg:
            logging.error("Cannot publish dashboard information: " + \
                          dashboardInfo.__str__() + "\n" + str(msg))

        return


    def fullId( self, job ):
        """
        __fullId__

        compose job primary keys in a string
        """

        return str( job['taskId'] ) + '.' \
               + str( job['jobId'] ) + '.' \
               + str( job['submissionNumber'] )





