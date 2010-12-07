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

__revision__ = "$Id: TrackingComponent.py,v 1.68 2010/06/01 12:02:33 mcinquil Exp $"
__version__ = "$Revision: 1.68 $"

import os
import os.path
import logging
import re
from datetime import datetime, timedelta


# PA configuration
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentDB.Config import defaultConfig as dbConfig
from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo

# to be substituted with a BossLite implementation
from JobTracking.TrackingDB import TrackingDB

# BossLite support
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import BossLiteError

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
        self.args.setdefault("jobsToPoll", 3000)
        self.args.setdefault("PoolThreadsSize", 5)
        self.args.setdefault("ComponentDir", "/tmp")
        self.args.setdefault("ProdAgentWorkDir", None)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("verbose", 0)
        self.args.setdefault("JobCreatorComponentDir", None)
        self.args.setdefault("dashboardInfo", \
                             {'use' : 'True', \
                              'address' : 'cms-jobmon.cern.ch', \
                              'port' : 8884})
        self.args.setdefault("TimeOutEvent", None)
        self.args.setdefault("TimeOut", 12) # In hours
        self.args.setdefault("DoneFailedTimeOut", None) # In hours
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
        self.database = dbConfig

        # initialize Session
        self.bossLiteSession = \
                             BossLiteAPI('MySQL', self.database, makePool=True)
        self.sessionPool = self.bossLiteSession.bossLiteDB.getPool()
        self.db = TrackingDB( self.bossLiteSession.bossLiteDB )

        # create pool thread
        params = {}
        params['delay'] = sleepTime
        params['jobsToPoll'] = int(self.args['jobsToPoll'])
        params['sessionPool'] = self.sessionPool
        JobStatus.setParameters(params)
        pool = \
             WorkQueue([JobStatus.doWork] * int(self.args["PoolThreadsSize"]))

        # create pool scheduler
        params = {}
        params['delay'] = delay
        params['jobsToPoll'] = int( self.args['jobsToPoll'] )
        params['sessionPool'] = self.sessionPool
        PoolScheduler(pool, params)

        # set parameters for getoutput operations
        params = {}
        params['componentDir'] = self.args['ComponentDir']
        params['sessionPool'] = self.sessionPool

        # check for dashboard usage
        self.usingDashboard = self.args['dashboardInfo']
        logging.debug("DashboardInfo = %s" % str(self.usingDashboard))

        # some initializations
        self.jobLimit = int(self.args['jobsToPoll'])
        self.newJobs = []
        self.counters = ['pending', 'submitted', 'waiting', 'ready', \
                         'scheduled', 'running', 'cleared', 'created', 'other']

        # time out for jobs
        self.timeoutEvent = self.args['TimeOutEvent']
        self.timeout = int(self.args['TimeOut'])
        if self.timeout < 1:
            self.timeout = 1 # a minimum value
        self.toDelay = str(self.timeout).zfill(2) + ':00:00'
        self.timeout = self.timeout * 3600

        # time out for Done(failed) jobs
        if self.args['DoneFailedTimeOut'] is not None:
            self.doneFailedTimeout = timedelta(
                hours=int(self.args['DoneFailedTimeOut']))
        else:
            self.doneFailedTimeout = None

        # ended/failed attributes
        self.newAttrs = { 'processStatus' : 'not_handled',
                          'closed' : 'N' }
        self.failedAttrs = { 'processStatus' : 'handled',
                             'status' : 'A', 'closed' : 'N' }
        self.killedAttrs = { 'processStatus' : 'handled',
                             'status' : 'K', 'closed' : 'N' }
        self.finishedAttrs = { 'processStatus' : 'handled',
                               'status' : 'SD', 'closed' : 'N' }
        self.doneFailedAttrs = { 'processStatus' : 'handled',
                               'status' : 'DA', 'closed' : 'N' }

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
        elif event == "TrackingComponent:CheckSubmitted":
            self.checkSubmitted()
            return

        # wrong event
        logging.info("Unexpected event %s(%s), ignored" % \
                     (str(event), str(payload)))
        return


    def checkSubmitted(self):
        """
        __checkSubmitted__

        check for submitted jobs and kill jobs stuck
        """

        logging.info(
            "Apply timeout: perform %s for jobs stuck since %s hours" % \
            (self.timeoutEvent, self.toDelay)
            )

        result = self.db.getStuckJobs( ['SU'], self.timeout, \
                                       begin='submission_time', \
                                       end='CURRENT_TIMESTAMP' )

        for jobSpec in result :
            logging.info( "perform %s for job %s" % \
                          ( self.timeoutEvent, jobSpec ) )
            self.ms.publish(self.timeoutEvent, jobSpec)

        # generate next polling cycle
        logging.info("Waiting %s for next polling cycle" % self.toDelay)
        self.ms.publish("TrackingComponent:CheckSubmitted", "", self.toDelay)
        self.ms.commit()


    def pollJobs(self, runningAttrs, processStatus, skipStatus=None, skipDelay=None):
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
            if self.newJobs == []: 
                loop = False
                break
            else :
                offset += self.jobLimit

            jobstoup = self.newJobs
            # Filter out jobs with status in skipStatus list
            if skipStatus is not None:
                skipStatusFilter = \
                    lambda j: j.runningJob['status'] not in skipStatus
                jobstoup = filter(skipStatusFilter, jobstoup)

            # Filter out jobs with lb_timestamp not older than skipDelay
            if skipDelay is not None:
                now = datetime.utcnow()
                timeoutFilter = \
                    lambda j: now - j.runningJob['lbTimestamp'] > skipDelay
                jobstoup = filter(timeoutFilter, jobstoup)

            queryjobstoup = \
                ",".join([str(j.runningJob['id']) for j in jobstoup])

            logging.debug('All new jobs: [%s]'%str(len(self.newJobs)))
            logging.debug('New jobs to send info: [%s]'%str(len(jobstoup))) 
            logging.debug('New job in query format: [%s]'%str(queryjobstoup))
            if jobstoup == []:
                continue
                
            try:
                #self.db.processBulkUpdate( self.newJobs, processStatus, \
                #                           skipStatus )
                self.db.processBulkUpdate( queryjobstoup, processStatus, \
                                           skipStatus )
                logging.info( "Changed status to %s for %s of %s loaded jobs" \
                              % ( processStatus, str( len(jobstoup) ), str ( len(self.newJobs) ) ) )
                            #  % ( processStatus, str( len(self.newJobs) ) ) )

            except BossLiteError, err:
                logging.error(
                    "Failed handling %s loaded jobs, waiting next round: %s" \
                    % ( processStatus, str( err ) ) )
                continue

            #while self.newJobs != [] :
            while jobstoup != [] :

                #job = self.newJobs.pop()
                job = jobstoup.pop()

                # publish information to dashboard
                try:
                    self.dashboardPublish( job )
                except Exception, msg:
                    logging.error("Cannot publish to dashboard:%s" % msg)
                    import traceback
                    logging.error(str(msg.format_exc()))

                del( job )

            del jobstoup[:]
            del self.newJobs[:]

        del self.newJobs[:]


    def checkJobs(self):
        """
        __checkJobs__

        Poll the DB and call the appropriate handler method for each
        jobId that is returned.

        """

        # log the status of the jobs
        self.getStatistic()

        # get finished jobs and handle them
        logging.info( 'Load Finished Jobs' )
        self.pollJobs( self.finishedAttrs, 'output_requested' )

        # get jobs and handle them
        logging.info( 'Load Failed Jobs' )
        self.pollJobs( self.failedAttrs, 'failed' )
        logging.info( 'Load Killed Jobs' )
        self.pollJobs( self.killedAttrs, 'failed' )

        # get timed out Done(failed) jobs
        if self.doneFailedTimeout is not None:
            logging.info( 'Load Done(failed) Jobs' ) 
            self.pollJobs( self.doneFailedAttrs,
                           'failed',
                           skipDelay=self.doneFailedTimeout )

        # notify new jobs (do not notify jobs if they haven't been assigned a
        # destiation to run)
        logging.info( 'Load New Jobs' )
        self.pollJobs( self.newAttrs, 'handled' , ['C', 'S', 'SU', 'SW'] )

        # generate next polling cycle
        logging.info( "Waiting %s for next get output polling cycle" % \
                      self.pollDelay )
        self.ms.publish( "TrackingComponent:pollDB", "", self.pollDelay )
        self.ms.commit()


    def getStatistic(self):
        """
        __getStatistics__

        Poll the BOSS DB for a summary of the job status

        """

        # summary of the jobs in the DB
        result = self.db.getJobsStatistic()

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
        if self.timeoutEvent is not None:
            self.ms.subscribeTo("TrackingComponent:CheckSubmitted")
            self.ms.publish("TrackingComponent:CheckSubmitted", "")

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
        if self.usingDashboard['use'] == "False" or \
               job.runningJob['schedulerId'] is None:
            return

        # initialize
        dashboardInfo = DashboardInfo()
        dashboardInfoFile = None

        # looking for dashboardInfoFile
        try :
            baseDir = os.path.dirname(job.runningJob['outputDirectory'])
            dashboardInfoFile = \
                             os.path.join(baseDir, \
                                          "DashboardInfo.xml" )
        except :
            pass

        useFile = False
        # if the dashboardInfoFile is not there, this is a crab job
        if dashboardInfoFile is None or not os.path.exists(dashboardInfoFile):
            task = self.bossLiteSession.loadTask(job['taskId'], deep=False)
            match = str(job.runningJob['schedulerId'])
            m = re.search("glidein", match)

            dashboardInfo.task = task['name']
            if m:
               dashboardInfo.job = str(job['jobId']) + '_https://' + \
                                job.runningJob['schedulerId']
            else:
               dashboardInfo.job = str(job['jobId']) + '_' + \
                                job.runningJob['schedulerId']
            dashboardInfo['JSTool'] = 'crab'
            dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
            dashboardInfo['User'] = task['name'].split('_')[0]
            dashboardInfo['TaskType'] =  'analysis'
            dashboardInfo.addDestination( self.usingDashboard['address'],
                                          int(self.usingDashboard['port']) )

        # otherwise, ProdAgent job: everything is stored in the file
        else:
            useFile = True
            try:
                # it exists, get dashboard information
                dashboardInfo.read(dashboardInfoFile)

            except Exception, msg:
                # it does not work, abandon
                logging.error("Reading dashboardInfoFile " + \
                              dashboardInfoFile + " failed (jobId=" \
                              + str(job['jobId']) + ")\n" + str(msg))
                return
            if not dashboardInfo.destinations.has_key('cms-pamon.cern.ch'):
                dashboardInfo.addDestination('cms-pamon.cern.ch', 8884)

        # write dashboard information
        dashboardInfo['GridJobID'] = job.runningJob['schedulerId']

        try :
            dashboardInfo['StatusEnterTime'] = \
                                             str(job.runningJob['lbTimestamp'])
        except Exception:
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

        # publish it
        try:
            # logging.debug("dashboardinfo: %s" % dashboardInfo.__str__())
            dashboardInfo.publish(1)
            logging.debug("dashboard info sent for job %s" % self.fullId(job) )

        # error, cannot publish it
        except Exception, msg:
            logging.error("Cannot publish dashboard information: " + \
                          dashboardInfo.__str__() + "\n" + str(msg))

        ### # create/update info file
        if useFile:
            logging.info("Creating dashboardInfoFile " + dashboardInfoFile )
            dashboardInfo.write( dashboardInfoFile )

        return


    def fullId( self, job ):
        """
        __fullId__

        compose job primary keys in a string
        """

        return str( job['taskId'] ) + '.' \
               + str( job['jobId'] ) + '.' \
               + str( job['submissionNumber'] )





