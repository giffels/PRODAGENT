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

__revision__ = "$Id: TrackingComponent.py,v 1.47.2.13 2008/03/28 15:35:25 gcodispo Exp $"
__version__ = "$Revision: 1.47.2.13 $"

import time
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
from ProdAgentBOSS import BOSSCommands
from ProdAgentBOSS.BOSSCommands import directDB

# BossLite support 
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.Scheduler import Scheduler

# Framework Job Report handling
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport

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
        params['jobsToPoll'] = self.args['jobsToPoll']
        JobStatus.setParameters(params)
        pool = \
             WorkQueue([JobStatus.doWork] * int(self.args["PoolThreadsSize"]))

        # create pool scheduler
        params = {}
        params['delay'] = delay
        params['jobToPoll'] = self.args['jobsToPoll']
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

        # build submitted jobs structure
        # self.submittedJobs = self.loadDict()

        # check for dashboard usage
        self.usingDashboard = self.args['dashboardInfo']
        logging.debug("DashboardInfo = %s" % str(self.usingDashboard))

        # component running, display info
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("JobTracking Component Started...")

    def __call__(self, event, payload):
        """
        _operator()_

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
        _pollNewJobsd_

        Poll the BOSS DB for new job ids and handle they registration

        Return a list of failed job ids
        """

        newJobs = self.bossLiteSession.loadJobsByRunningAttr(
            { 'processStatus' : 'not_handled' }
            )
        logging.info("new jobs : " + str( len(newJobs) ) )

        for job in newJobs :
            ### here was good the compund update... to be reimplemented
            job.runningJob['processStatus'] = 'handled'
            self.bossLiteSession.updateDB( job )
            # publish information to dashboard
            try:
                self.dashboardPublish( job )
                pass
            except StandardError, msg:
                logging.error("Cannot publish to dashboard:%s" % msg)    

        # build query 
        query = """
        select status, count( status ) from bl_runningjob
        where closed='N' group by  status
        """

        # summary of the jobs in the DB
        session = directDB.getDbSession()
        result = directDB.select( session, query )
        directDB.close( session )

        if result is not None:
            
            # initialize counters for statistic
            counters = ['pending', 'submitted', 'waiting', 'ready', \
                        'scheduled', 'running', 'cleared', 'created', 'other']

            counter = {}
            for ctr in counters:
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

    def checkJobs(self):
        """
        _checkJobs_

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
        _handleFailed_

        handle failed jobs

        """

        failedJobs = self.bossLiteSession.loadFailed()
        logging.info("failed jobs : " + str( len(failedJobs) ) )

        # process all jobs
        for job in failedJobs:

            # get framework jod report file name
            directory = self.componentDir + '/BossJob_%s_%s/Submission_%s' % \
                        (job['taskId'], job['jobId'], job['submissionNumber'])
            reportfilename = directory + "/FrameworkJobReport.xml"

            # publish information to the dashboard
            try:
                self.dashboardPublish(job)
                pass
            except StandardError, msg:
                logging.error("Cannot publish to dashboard:%s" % msg)

            # create directory
            logging.debug("Creating directory: " + directory)

            try:
                os.makedirs( directory )
            except OSError, err:
                if  err.errno == 17:
                    # existing dir
                    pass
                else :
                    # cannot create directory, go to next job
                    logging.error("Cannot create directory : " + str(msg))
                    continue

            # create Framework Job Report
            logging.debug("Creating report %s" % reportfilename)

            # create failure Framework Job Report
            fwjr = FwkJobReport()
            fwjr.jobSpecId = job['name']
            fwjr.exitCode = -1
            fwjr.status = "Failed"
            fwjr.write(reportfilename)

            # loggingInfo
            task = self.bossLiteSession.loadTask(job['taskId'])
            if task['user_proxy'] is None:
                task['user_proxy'] = ''

            schedulerConfig = {'name' : job.runningJob['scheduler'],
                               'user_proxy' : task['user_proxy'] ,
                               'service' : job.runningJob['service'] }
            scheduler = Scheduler.Scheduler(
                job.runningJob['scheduler'], schedulerConfig )

            scheduler.postMortem( job, directory + '/loggingInfo.log' )

            # perform a BOSS archive operation
            self.bossLiteSession.archive( job )

            # generate a failure message
            self.jobHandling.publishJobFailed(job, reportfilename)

    def handleFinished(self):
        """
        _handleFinished_

        handle finished jobs: retrieve output and notify execution
        failure or success

        """

        finishedJobs = self.bossLiteSession.loadEnded()
        logging.info("finished jobs : " + str( len(finishedJobs) ) )

        # process all jobs
        for job in finishedJobs:

            # publish information to the dashboard
            try:
                self.dashboardPublish(job)
                pass
            except StandardError, msg:
                logging.error("Cannot publish to dashboard:%s" % msg)

            # enqueue the get output operation
            logging.debug("Enqueing getoutput request for %s" % \
                          BOSSCommands.fullId(job))

            JobOutput.requestOutput(job)
                
        return

    def dashboardPublish(self, job):
        """
        _dashboardPublish_
        
        publishes dashboard info
        """

        # dashboard information
        ( dashboardInfo, dashboardInfoFile )= BOSSCommands.guessDashboardInfo(
            job['jobId'], self.bossLiteSession
            )
        if dashboardInfo.task == '' or dashboardInfo.task == None :
            logging.error( "unable to retrieve DashboardId" )
            return

        # set dashboard destination
        dashboardInfo.addDestination(
            self.usingDashboard['address'], self.usingDashboard['port']
            )
        
        # if the dashboardInfo.job is not set,
        # this is a crab job detected for the first time
        # set it and write the info file 
        if dashboardInfo.job == '' or dashboardInfo.job == None :
            dashboardInfo.job = job['jobId'] + '_' + \
                                job.runningJob['schedulerId']
#            # create/update info file
#            logging.info("Creating dashboardInfoFile " + dashboardInfoFile )
#            dashboardInfo.write( dashboardInfoFile )
    
        # write dashboard information
        dashboardInfo['GridJobID'] = job.runningJob['schedulerId']
        
        try :
            dashboardInfo['StatusEnterTime'] = time.strftime( \
                             '%Y-%m-%d %H:%M:%S', \
                             time.gmtime(float(job.runningJob['lbTimestamp'])))
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

#        dashboardInfo['SubTimeStamp'] = time.strftime( \
#                             '%Y-%m-%d %H:%M:%S', \
#                             time.gmtime(float(schedulerI['LAST_T'])))

        # create/update info file
        logging.info("Creating dashboardInfoFile " + dashboardInfoFile )
        dashboardInfo.write( dashboardInfoFile )
        
        # publish it
        try:
            logging.debug("dashboardinfo: %s" % dashboardInfo.__str__())
            dashboardInfo.publish(5)

        # error, cannot publish it
        except StandardError, msg:
            logging.error("Cannot publish dashboard information: " + \
                          dashboardInfo.__str__() + "\n" + str(msg))

        return

    def startComponent(self):
        """
        _startComponent_

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
#        params['bossCfgDir'] = self.bossCfgDir
        params['baseDir'] = self.componentDir
        params['jobCreatorDir'] = self.jobCreatorDir
        params['usingDashboard'] = self.usingDashboard
        params['messageServiceInstance'] = self.ms
        self.jobHandling = JobHandling(params)

        # wait for messages
        while True:

            # get a message
            mtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (mtype, payload))

            # process it
            self.__call__(mtype, payload)


