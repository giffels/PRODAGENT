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

__revision__ = "$Id: TrackingComponent.py,v 1.47.2.12 2008/02/18 13:25:31 afanfani Exp $"
__version__ = "$Revision: 1.47.2.12 $"

import time
import os
import os.path
import logging
from copy import deepcopy

# PA configuration
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils

from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig
from GetOutput.TrackingDB import TrackingDB
from ProdCommon.Database.MysqlInstance import MysqlInstance
from ProdCommon.Database.SafeSession import SafeSession

##safe pool
from ProdCommon.Database.SafePool import SafePool

from GetOutput.JobOutput import JobOutput
from JobTracking.JobHandling import JobHandling

# BossLite support 
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

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
#        workingDir = self.args['ProdAgentWorkDir'] 
#        workingDir = os.path.expandvars(workingDir)
        self.componentDir = self.args['ComponentDir']

        ## get BOSS configuration, set directory and verbose mode
        # NOTE : probably not needed for BossLite # Fabio
        #self.bossCfgDir = self.args['configDir'] 
        #logging.info("Using BOSS configuration from " + self.bossCfgDir)

        self.directory = self.args["ComponentDir"]
        if self.args["JobCreatorComponentDir"] is None:
            self.args["JobCreatorComponentDir"] = self.directory
        self.jobCreatorDir = \
                os.path.expandvars(self.args["JobCreatorComponentDir"])
        self.verbose = (self.args["verbose"] == 1)

        # set BOSS path
        #BOSS.setBossCfgDir(self.bossCfgDir)

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
        #self.database = dbConfig
        #self.bossDatabase = deepcopy(dbConfig)
        #self.bossDatabase['dbName'] += "_BOSS"
        #self.bossDatabase['dbType'] = 'mysql'
        #self.activeJobs = self.bossDatabase['dbName'] + ".jt_activejobs"
        #self.dbInstance = MysqlInstance(self.bossDatabase)

        self.database = deepcopy(dbConfig)
        self.msqlDBInstance = MysqlInstance(self.database)
        self.activeJobs = str(self.database['dbName']) + ".jt_activejobs"

        # initialize Session
        Session.set_database(self.database)
        Session.connect()

        # initialize BossLite API and the session to interact with bl Database
        # TODO: Fix with the correct configurations for the DB
        self.blDBinstance = BossLiteAPI('MySQL', self.database)
        self.blAdminSession = SafeSession( dbInstance = self.msqlDBInstance )

        # set parameters for getoutput operations
        params = {}
        #params['bossCfgDir'] = self.bossCfgDir
        params['dbInstance'] = self.msqlDBInstance
        JobOutput.setParameters(params)

        # build submitted jobs structure
        self.submittedJobs = self.loadDict()

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


    def pollFinished(self):
        """
        _pollFinished_

        Poll the BOSS DB for completed job ids, making sure that
        only newly completed jobs are retrieved

        Return a list of finished job ids
        """

        # build query 
        #query = """
        #"""
        #select JOB.TASK_ID, JOB.CHAIN_ID, JOB.ID, JOB.STATUS
        #  from JOB LEFT JOIN jt_activejobs
        #                 on jt_activejobs.job_id=
        #                    concat(JOB.TASK_ID, '.', JOB.CHAIN_ID, '.', JOB.ID)
        # where JOB.STATUS in ('OR', 'SD')
        #       and (jt_activejobs.status="output_not_requested"
        #            or jt_activejobs.status is NULL)
        #"""

        # BossLite query
        query = """
        select bl_runningjob.task_id, bl_runningjob.job_id, bl_runningjob.id, bl_runningjob.status
          from bl_runningjob LEFT JOIN jt_activejobs
                         on jt_activejobs.job_id=
                            concat(bl_runningjob.task_id, '.', bl_runningjob.job_id, '.', bl_runningjob.id)
         where bl_runningjob.STATUS in ('OR', 'SD')
               and (jt_activejobs.status="output_not_requested"
                    or jt_activejobs.status is NULL)
        """
        return self.pollBossDB( query )

    def pollFailed(self):
        """
        _pollFailed_

        Poll the BOSS DB for completed job ids, making sure that
        only newly completed jobs are retrieved

        Return a list of failed job ids
        """

        # build query 
        #query = """
        #select TASK_ID,CHAIN_ID,ID,STATUS from JOB
        #WHERE STATUS in ('A', 'SA', 'K', 'SK')
        #order by TASK_ID,CHAIN_ID
        #"""
        
        # BossLite query   
        query = """
        select task_id,job_id,id,status from bl_runningjob
        WHERE status in ('A', 'SA', 'K', 'SK')
        order by task_id,job_id
        """
        return self.pollBossDB( query )

    def pollNewJobs(self):
        """
        _pollNewJobsd_

        Poll the BOSS DB for new job ids and handle they registration

        Return a list of failed job ids
        """

        # initialize counters for statistic
        counters = ['pending', 'submitted', 'waiting', 'ready', 'scheduled', \
                    'running', 'cleared', 'created', 'other']
        counter = {}
        for ctr in counters:
            counter[ctr] = 0

        # list of jobs
        submittedJobs = {}

        # build query 
        #query = """
        #select TASK_ID,CHAIN_ID,ID,STATUS from JOB
        #WHERE (STATUS not in ('A', 'SA', 'K', 'SK', 'OR', 'SD'))
        #order by TASK_ID,CHAIN_ID
        #"""
        
        # BossLite query
        query = """
        select task_id,job_id,id,status from bl_runningjob
        WHERE (status not in ('A', 'SA', 'K', 'SK', 'OR', 'SD'))
        order by task_id,job_id
        """
        jobs = self.pollBossDB( query )
        index = len ( jobs )

        while index > 0 :
            index -= 1
            jid    = jobs[ index ][0]
            status = jobs[ index ][1]
            jobs.pop( index )

            # publish information to dashboard if not submitted before
            if not jid in self.submittedJobs.keys() \
                   and status not in [ 'S', 'W', 'SU', 'SW' ] :

                # no, publish information to dashboard
                try:
                    # TODO commented waiting for BossLite information 
                    # self.dashboardPublish( jid, 
                         # What counterpart in BLite? BOSSCommands.jobSpecId(jid, self.bossCfgDir)
                    #    )
                    pass
                except Exception, msg:
                    logging.error("Cannot publish to dashboard:%s" % msg)

            # include job in current structure
            submittedJobs[jid] = 0

            # process status
            if status == 'E':
                continue
            elif status == 'R' :
                counter['running'] += 1
            elif status == 'I':
                counter['pending'] += 1
            elif status == 'SW' :
                counter['waiting'] += 1
            elif status == 'SR':
                counter['ready'] += 1
            elif status == 'SS':
                counter['scheduled'] += 1
            elif status == 'SU':
                counter['submitted'] += 1
            elif status == 'SE':
                counter['cleared'] += 1
            elif status == 'C':
                counter['created'] += 1
            else:
                counter['other'] += 1

        # display counters
        for ctr, value in counter.items():
            logging.info(ctr + " jobs : " + str(value))

        # save submitted jobs and update dictionary
        self.updateDict(self.submittedJobs, submittedJobs)
        self.submittedJobs = submittedJobs
        return 


    def pollBossDB(self, query):
        """
        _pollFailed_

        Poll the BOSS DB for job ids

        Return a list of job ids
        """
        
        # list of jobs
        jobs = []

        # fix for BossLite # Fabio
        row = self.blAdminSession.execute(query) 
        out = []

        if (row > 0):
            out = self.blAdminSession.fetchall()

        # process all jobs # with BossLite
        for j in out:
            # get job information 
            # the selected fields are the same of the tuple, with the same order too for every query # Fabio
            try:
                (taskId, chainId, ident, status) = j

            # line does not contain job information, ignore
            except StandardError:
                continue

            # ignore non positive task ids
            if int(taskId) <= 0:
                logging.error("Incorrect job information from BOSS DB: " + \
                              str(j))
                continue
 
            # build job id 
            jid = str(taskId) + "." + str(chainId) + "." + str(ident)
            jobs.append( [jid, status])

        # return finished and failed jobs
        return jobs


    def checkJobs(self):
        """
        _checkJobs_

        Poll the DB and call the appropriate handler method for each
        jobId that is returned.

        """

        # get finished and failed jobs and handle them
        # NOTE: modified to work with the new BossLite structures

        finishedJobs = self.pollFinished()
        logging.debug("FINISHED JOBS: %s" % str(finishedJobs))
        self.handleFinished(finishedJobs)
        failedJobs = self.pollFailed()
        logging.debug("FAILED JOBS: %s" % str(failedJobs))
        self.handleFailed(failedJobs)
        self.pollNewJobs()

        # generate next polling cycle
        logging.info("Waiting %s for next get output polling cycle" % \
                     self.pollDelay)
        self.ms.publish("TrackingComponent:pollDB", "", self.pollDelay)
        self.ms.commit()

    def handleFailed(self, failedJobs):
        """
        _handleFailed_

        handle failed jobs

        """

        logging.info("failed jobs : " + str( len(failedJobs) ) )

        # process all jobs
        for jobIdde in failedJobs:
            jobId = jobIdde[0].split('.')[1]
            jobObj = None
             
            # get job specification id
            try:
                # load job data from BossLite #Fabio
                jobObj = self.blDBinstance.loadJobsByAttr( {'id': jobId} )[0]
                jobSpecId = jobObj['jobId'] # TODO check
                # BOSSCommands.jobSpecId(jobId[0], self.bossCfgDir)

            # error
            except StandardError, msg:
                logging.error("Failed job with wrong job id %s. Error:\n%s" \
                              % (jobId, str(msg)))
                import traceback
                logging.info (str(traceback.format_exc()))
                continue

            # get framework jod report file name
            reportfilename = "" # TODO check
                 # BOSSCommands.reportfilename(jobId[0], self.directory)

            # publish information to the dashboard
            try:
                # TODO to be fixed with the BossLite information
                # self.dashboardPublish(jobId, jobSpecId) # TODO check
                pass 
            except Exception, msg:
                logging.error("Cannot publish to dashboard:%s" % msg)

            # create directory
            directory = os.path.dirname(reportfilename)
            logging.debug("Creating directory: " + directory)

            try:
                os.makedirs(os.path.dirname(reportfilename))

            except StandardError, msg:
                # cannot create directory, go to next job
                logging.error("Cannot create directory : " + str(msg))
                continue

            # create Framework Job Report
            logging.debug("Creating report %s" % reportfilename)

            # create failure Framework Job Report
            fwjr = FwkJobReport()
            fwjr.jobSpecId = jobSpecId
            fwjr.exitCode = -1
            fwjr.status = "Failed"
            fwjr.write(reportfilename)

            # get grid log file
            outdir = os.path.dirname(reportfilename)
#            BOSSCommands.loggingInfo( jobId[0], outdir, self.bossCfgDir ) # TODO: BLite counterpart?

            # perform a BOSS archive operation
            #BOSSCommands.archive(jobId[0], self.bossCfgDir)
            self.blDBsession.archive(jobObj['taskId'], jobId) # TODO check 

            # generate a failure message
            self.jobHandling.publishJobFailed(jobId, reportfilename)

    def handleFinished(self, finishedJobs):
        """
        _handleFinished_

        handle finished jobs: retrieve output and notify execution
        failure or success

        """

        logging.info("finished jobs : " + str( len(finishedJobs) ) )

        # process all jobs
        for jobId in finishedJobs:
  
            jobObj = None
  
            # get job specification id
            try:
                # load job data from BossLite #Fabio
                jobObj = self.blDBinstance.loadJobsByAttr( {'id': jobId} )[0]
                jobSpecId = jobObj['jobId'] # TODO check
                #BOSSCommands.jobSpecId(jobId[0], self.bossCfgDir)
            # error
            except StandardError, msg:
                logging.error("Finished job with wrong job id %s. Error:\n%s" \
                              % (job.__str__(), str(msg)))
                continue

            # publish information to the dashboard
            try:
                # TODO to be fixed with BossLite information
                # self.dashboardPublish(jobId, jobSpecId) # TODO check
                pass
            except Exception, msg:
                logging.error("Cannot publish to dashboard:%s" % msg)


            # perform the get output operation 
            jobInfo = {'jobId' : jobId[0],
                       'jobSpecId' : jobSpecId,
                       'directory' : self.directory,
                       'bossStatus' : jobId[1],
                       'output' : None}

            # enqueue the get output operation
            logging.debug("Enqueing getoutput request for %s" % str(jobId))

            JobOutput.requestOutput(jobInfo)
                
        return
    '''
    def dashboardPublish(self, jobId, jobSpecId):
        #TODO BLite counterparts?

        # many bossCommands here have not a BossLite couterpart. How could we get information on the
        # the dashboard or on the scheduler infos so that the fields are filled? # Fabio  
        """
        _dashboardPublish_
        
        publishes dashboard info
        """

        # dashboard information #TODO check
        ( dashboardInfo, dashboardInfoFile )= BOSSCommands.guessDashboardInfo( jobId, jobSpecId, self.bossCfgDir )
        if dashboardInfo.task == '' or dashboardInfo.task == None :
            logging.error( "unable to retrieve DashboardId" )
            return

        # set dashboard destination
        dashboardInfo.addDestination(
            self.usingDashboard['address'], self.usingDashboard['port']
            )

        # get scheduler info
        schedulerI = BOSSCommands.schedulerInfo(self.bossCfgDir, jobId)
        
        if len( schedulerI ) == 0 or not schedulerI.has_key('SCHED_ID') :
            logging.error("schedulerinfo: %s" % schedulerI.__str__())
            return
        
        logging.debug("schedulerinfo: %s" % schedulerI.__str__())
        
        # if the dashboardInfo.job is not set,
        # this is a crab job detected for the first time
        # set it and write the info file 
        if dashboardInfo.job == '' or dashboardInfo.job == None :
            dashboardInfo.job = jobId.split('.')[1] + '_' + \
                                schedulerI['SCHED_ID']
#            # create/update info file
#            logging.info("Creating dashboardInfoFile " + dashboardInfoFile )
#            dashboardInfo.write( dashboardInfoFile )
    
        # write dashboard information
        dashboardInfo['GridJobID'] = schedulerI['SCHED_ID']

        
        try :
            dashboardInfo['StatusEnterTime'] = time.strftime( \
                             '%Y-%m-%d %H:%M:%S', \
                             time.gmtime(float(schedulerI['LB_TIMESTAMP'])))
        except StandardError:
            pass

        try :
            dashboardInfo['StatusValue'] = schedulerI['SCHED_STATUS']
        except KeyError:
            pass

        try :
            dashboardInfo['StatusValueReason'] = \
                                   schedulerI['STATUS_REASON'].replace('-',' ')
        except KeyError:
            pass

        try :
            dashboardInfo['StatusDestination'] = schedulerI['DEST_CE'] + \
                                                 "/" + schedulerI['DEST_QUEUE']
        except KeyError:
            pass
        
        try :
            dashboardInfo['RBname'] = schedulerI['RB']
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
    '''

    def updateDict(self, old, new):
        """
        _updateDict_

        save information about active jobs in case of component restart, so
        status is not sent twice to the dashboard.

        all updates are done in single queries. Length should not be a problem
        since default for mysql 4.0 is 1GB...

        """

        # get new and old set of jobs id
        newJobs = set(new.keys())
        oldJobs = set(old.keys())

        # get newly submitted jobs 
        submittedJobs = newJobs.difference(oldJobs)
        numberOfJobs = len(submittedJobs)

        # no new jos, nothing to do
        if numberOfJobs == 0:
            return

        # build list
        jobs = [x for x in submittedJobs]

        # create a session
        
        session = SafeSession(dbInstance = self.msqlDBInstance)
        db = TrackingDB(session)

        # add jobs
        added = db.addJobs(jobs)

        # commit changes
        session.commit()
        session.close()

        if added != numberOfJobs:
            logging.warning("Only %s of %s jobs added" % \
                                (added, numberOfJobs))

        return new

    def loadDict(self):
        """
        _loadDict_

        load information about jobs from database
        """

        # create a session
        session = SafeSession(dbInstance = self.msqlDBInstance)
        db = TrackingDB(session)

        # query database for active jobs information
        jobs = db.getJobs("output_not_requested")

        # close session
        session.close()
 
        # add jobs to dictionary
        activeJobs = {}
        for jobId in jobs:
            activeJobs[jobId] = 0

        return activeJobs

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
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (type, payload))

            # process it
            self.__call__(type, payload)


