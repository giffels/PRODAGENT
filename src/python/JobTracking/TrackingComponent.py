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

__revision__ = "$Id: TrackingComponent.py,v 1.47.2.8 2007/10/26 10:08:34 gcodispo Exp $"
__version__ = "$Revision: 1.47.2.8 $"

import time
import os
from shutil import copy
from shutil import rmtree
import logging
from copy import deepcopy

# PA configuration
from MessageService.MessageService import MessageService
from ProdCommon.Database import Session
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as WEJob
from ProdAgentDB.Config import defaultConfig as dbConfig
from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo
from ProdAgentBOSS import BOSSCommands
from ProdAgentBOSS.BOSSCommands import BOSS
import ProdAgentCore.LoggingUtils as LoggingUtils

# Framework Job Report handling
from FwkJobRep.ReportState import checkSuccess
from FwkJobRep.FwkJobReport import FwkJobReport
from FwkJobRep.ReportParser import readJobReport

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
        workingDir = self.args['ProdAgentWorkDir'] 
        workingDir = os.path.expandvars(workingDir)

        # get BOSS configuration, set directory and verbose mode
        self.bossCfgDir = self.args['configDir'] 
        logging.info("Using BOSS configuration from " + self.bossCfgDir)

        self.directory = self.args["ComponentDir"]
        self.jobCreatorDir = \
                os.path.expandvars(self.args["JobCreatorComponentDir"])
        self.verbose = (self.args["verbose"] == 1)

        # set BOSS path
        BOSS.setBossCfgDir(self.bossCfgDir)

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
        self.database = dbConfig
        self.bossDatabase = deepcopy(dbConfig)
        self.bossDatabase['dbName'] += "_BOSS"

        # initialize Session
        Session.set_database(dbConfig)

        # build submitted jobs structure
        self.submittedJobs = self.loadDict()

        # check for dashboard usage
        self.usingDashboard = self.args['dashboardInfo']
        logging.debug("DashboardInfo = %s" % str(self.usingDashboard))

        # component running, display info
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("JobTracking Component Started...")
        logging.info("BOSS_ROOT = %s" % os.environ["BOSS_ROOT"])
        logging.info("BOSS_VERSION = v4\n")

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
        query = """
        select TASK_ID,CHAIN_ID,ID,STATUS from JOB
        WHERE STATUS in ('OR', 'SD')
        order by TASK_ID,CHAIN_ID
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
        query = """
        select TASK_ID,CHAIN_ID,ID,STATUS from JOB
        WHERE STATUS in ('A', 'SA', 'K', 'SK')
        order by TASK_ID,CHAIN_ID
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
                    'running', 'cleared', 'other']
        counter = {}
        for ctr in counters:
            counter[ctr] = 0

        # list of jobs
        submittedJobs = {}

        # build query 
        query = """
        select TASK_ID,CHAIN_ID,ID,STATUS from JOB
        WHERE STATUS not in ('A', 'SA', 'K', 'SK', 'OR', 'SD')
        order by TASK_ID,CHAIN_ID
        """
        jobs = self.pollBossDB( query )
        
        # for jobId in jobs:
        #    jid = jobId[0]
        #    status = jobId[1]
        
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
                self.dashboardPublish(
                    jid, BOSSCommands.jobSpecId(jid, self.bossCfgDir)
                    )

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
            else:
                counter['other'] += 1

        # display counters
        logging.info("--------------------")
        for ctr, value in counter.items():
            logging.info(ctr + " jobs : " + str(value))
        logging.info("--------------------")

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

        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()

        # execute query
        (adminSession, out) = BOSS.performBossQuery(adminSession, query)
        
        lines = out.split('\n')

        # process all jobs
        for j in lines[1:]:

            # get job information
            try:
                job = j.strip().split()
                (taskId, chainId, ident, status) = job

            # line does not contain job information, ignore
            except StandardError:
                continue

            # ignore non positive task ids
            if int(taskId) <= 0:
                logging.error("Incorrect job information from BOSS DB: " + \
                              str(j))

                continue
 
            # build job id 
            jid = taskId + "." + chainId + "." + ident

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
        # finishedJobs, failedJobs = self.pollBossDb()
        finishedJobs = self.pollFinished()
        self.handleFinished(finishedJobs)
        failedJobs = self.pollFailed()
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

        logging.info("--------------------")
        logging.info("failed jobs : " + str( len(failedJobs) ) )
        logging.info("--------------------")

        # process all jobs
        for jobId in failedJobs:

            # get job specification id
            try:
                jobSpecId = BOSSCommands.jobSpecId(jobId[0], self.bossCfgDir)

            # error
            except StandardError, msg:
                logging.error("Failed job with wrong job id %s. Error:\n%s" \
                              % (jobId.__str__(), str(msg)))
                continue

            # get framework jod report file name
            reportfilename = BOSSCommands.reportfilename(jobId[0], \
                                                         self.directory)

            # publish information to the dashboard
            self.dashboardPublish(jobId[0], jobSpecId)

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
            BOSSCommands.loggingInfo( jobId[0], outdir, self.bossCfgDir )

            # perform a BOSS archive operation
            BOSSCommands.archive(jobId[0], self.bossCfgDir)

            # generate a failure message
            self.jobFailed(jobId, reportfilename)

    def handleFinished(self, finishedJobs):
        """
        _handleFinished_

        handle finished jobs: retrieve output and notify execution
        failure or success

        """

        logging.info("--------------------")
        logging.info("finished jobs : " + str( len(finishedJobs) ) )
        logging.info("--------------------")

        # process all jobs
        for jobId in finishedJobs:

            # get job specification id
            try:
                jobSpecId = BOSSCommands.jobSpecId(jobId[0], self.bossCfgDir)

            # error
            except StandardError, msg:
                logging.error("Finished job with wrong job id %s. Error:\n%s" \
                              % (jobId.__str__(), str(msg)))
                continue

            # publish information to the dashboard
            self.dashboardPublish(jobId[0], jobSpecId)

            # perform the get output operation
            jobInfo = {'jobId' : jobId,
                       'jobSpecId' : jobSpecId,
                       'directory' : self.directory,
                       'configDir' : self.bossCfgDir }

            jobInfo['output'] = self.getOutput(jobInfo)

            # process the output
            self.processOutput(jobInfo)
                
        return

    def getOutput(self, jobInfo):
        """
        _getOutput_

        perform the real get output operation

        """

        # get job information
        jobId = jobInfo['jobId']
        directory = jobInfo['directory']
        configDir = jobInfo['configDir']

        #  get output, trying at most maxGetOutputAttempts
        retry = 0
        while retry < self.maxGetOutputAttempts:

            # perform get output operation
            try:
                outp = BOSSCommands.getoutput( jobId[0], directory, configDir )
                break

            # error
            except StandardError, msg:
                logging.error(str(msg))
                outp = "error"
                retry += 1

        logging.debug("BOSS Getoutput: " + outp)

        # build return value:
        return outp


    def processOutput(self, jobInfo):
        """
        _processOutput_

        process the output of a job and notify execution failure or success

        """

        # get job information
        jobId = jobInfo['jobId']
        jobSpecId = jobInfo['jobSpecId']
        outp = jobInfo['output']

        # successful output retrieval?
        if outp.find("-force") < 0 and \
           outp.find("error") < 0 and \
           outp.find("already been retrieved") < 0:

            # yes, get report file name
            reportfilename = BOSSCommands.reportfilename(jobId[0], \
                                                         self.directory)
            logging.debug("report file name %s exists: %s" % \
                (reportfilename, os.path.exists(reportfilename)))

            # job status
            success = False

            # is the FwkJobReport there?
            if os.path.exists(reportfilename):

                # check success
                success = checkSuccess(reportfilename)
                logging.debug("check Job Success: %s" % str(success))

            # FwkJobReport not there: create one based on BOSS DB
            else:

                # check success
                success = BOSSCommands.checkSuccess(jobId[0], \
                                                    self.bossCfgDir)
                logging.debug("check Job Success: %s" % str(success))

                # create BOSS based Framework Job Report
                fwjr = FwkJobReport()
                fwjr.jobSpecId = jobSpecId
                reportfilename = BOSSCommands.reportfilename(jobId[0], \
                                                         self.directory)

                # job successful even if job report is not there
                if success:

                    # set success status
                    logging.info("Created successful report for %s" % \
                                    jobId.__str__())
                    fwjr.status = "Success"
                    fwjr.exitCode = 0

                # job failed
                else:

                    # set failed status
                    fwjr.status = "Failed"
                    fwjr.exitCode = -1

                try:
                    os.makedirs( os.path.dirname(reportfilename) )
                except OSError:
                    pass

                # store job report
                fwjr.write(reportfilename)
                
            # in both cases: is the job successful?
            if success:

                # yes, generate a job successful message and change status
                self.jobSuccess(jobId, reportfilename)
                self.notifyJobState(jobSpecId)

            else:

                # no, generate a job failure message
                self.jobFailed(jobId, reportfilename)

        # else if output retrieval failed
        elif outp.find("Unable to find output sandbox file:") >= 0 \
                 or outp.find("Error retrieving Output") >= 0 \
                 or outp.find("Error extracting files ") >= 0 :

            logging.debug("Job " + jobId.__str__() + \
                          " has no FrameworkReport : creating a dummy one")

            # create job report
            fwjr = FwkJobReport()
            fwjr.jobSpecId = jobSpecId
            reportfilename = BOSSCommands.reportfilename(jobId[0], \
                                                            self.directory)
            fwjr.exitCode = -1
            fwjr.status = "Failed"

            try:
                os.makedirs( os.path.dirname(reportfilename) )
            except OSError:
                pass

            # store job report
            fwjr.write(reportfilename)

            # archive job, forcing a deleted status in the BOSS DB
            BOSSCommands.Delete(jobId[0], self.bossCfgDir)

            # generate a failure message
            self.jobFailed(jobId, reportfilename)
            
        # other problem... just display error                            
        else:
            logging.error(outp)

        return

    def dashboardPublish(self, jobId, jobSpecId):
        """
        _dashboardPublish_
        
        publishes dashboard info
        """

        # dashboard information
        ( dashboardInfo, dashboardInfoFile )= BOSSCommands.guessDashboardInfo(
            jobId, jobSpecId, self.bossCfgDir
            )
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


    def notifyJobState(self, jobId):
        """
        _notifyJobState_

        Notify the JobState DB of finished jobs
        """

        # set finished job state
        try:
            JobState.finished(jobId)
            Session.commit()

        # error
        except StandardError, ex:
            msg = "Error setting job state to finished for job: %s\n" \
                  % jobId 
            msg += str(ex)
            logging.error(msg)

        return

    def jobSuccess(self, jobId, reportfilename):
        """
        _jobSuccess_
        
        Set success status for the job on BOSS DB and publish a JobSuccess
        event to the prodAgent
        """

        # set success job status
        reportfilename = self.archiveJob("Success", jobId, reportfilename)

        # publish success event
        self.ms.publish("JobSuccess", reportfilename)
        self.ms.commit()
        
        logging.info("Published JobSuccess with payload :%s" % \
                     reportfilename)
        return

    def jobFailed(self, jobId, reportfilename):
        """
        _jobFailed_
        
        Set failure status for the job on BOSS DB and publish a JobFailed
        event to the prodAgent
        """
        
        # archive the job in BOSS DB
        reportfilename = self.archiveJob("Failed", jobId, reportfilename)

        # publish job failed event
        self.ms.publish("JobFailed", reportfilename)
        self.ms.commit()

        logging.info("published JobFailed with payload: %s" % \
                     reportfilename)
           
        return


    def archiveJob(self, success, jobId, reportfilename):
        """
        _archiveJob_

        Moves output file to archdir
        """

        # get resubmission count
        try:
            resub = jobId[0].split('.')[2]

        except StandardError, msg:
            logging.error("archiveJob for job %s failed: " % \
                          (jobId[0], str(msg)))
            return reportfilename
        
        # get directory information
        lastdir = os.path.dirname(reportfilename).split('/').pop()
        baseDir = os.path.dirname(reportfilename) + "/"

        # get job report
        fjr = readJobReport(reportfilename)

        # fallback directory in JobTracking.
        fallbackCacheDir = self.args['ComponentDir'] + "/%s" % fjr[0].jobSpecId

        # try to get cache from JobState
        try:
            jobCacheDir = \
                        JobState.general(fjr[0].jobSpecId)['CacheDirLocation']

        # error, cannot get cache location
        except StandardError, ex:
            msg = "Cannot get JobCache from JobState.general for %s\n" % \
                  fjr[0].jobSpecId
            msg += str(ex)
            logging.warning(msg)

            # try to get cache from WEJob
            try: 
                WEjobState = WEJob.get(fjr[0].jobSpecId)
                jobCacheDir = WEjobState['cache_dir']

            # error, cannot get cache location
            except StandardError, ex:
                msg = "Cant get JobCache from Job.get['cache_dir'] for %s" % \
                      fjr[0].jobSpecId
                msg += str(ex)
                logging.warning(msg)

                # try guessing the JobCache area based on jobspecId name
                try:

                    # split the jobspecid=workflow-run into workflow/run
                    spec = fjr[0].jobSpecId
                    end = spec.rfind('-')
                    workflow = spec[:end]
                    run = spec[end+1:]

                    # additional split for PM jobspecid that are in the form
                    # jobcut-workflow-run
                    pmspec = workflow.find('jobcut-')
                    if pmspec > 0:
                        workflow = workflow[pmspec+7:]

                    # build cache directory on JobCreator area
                    jobCacheDir = "%s/%s/%s" % \
                                  (self.jobCreatorDir, workflow, run)

                    # if it does not exist, use fallback
                    if not os.path.exists(jobCacheDir):
                        jobCacheDir = fallbackCacheDir

                # error, cannot get cache location
                except StandardError, ex:

                    # use fallback
                    msg = "Cant guess JobCache in JobCreator dir" 
                    msg += str(ex)
                    logging.warning(msg)
                    jobCacheDir = fallbackCacheDir

        logging.debug("jobCacheDir = %s" % jobCacheDir)

        # build path and report file name
        newPath = jobCacheDir + "/JobTracking/" + success + "/" + lastdir + "/"

        # create directory if not there
        try:
            os.makedirs(newPath)

        except StandardError, msg:
            logging.debug("cannot create directory %s: %s" % \
                          (newPath, str(msg)))

        # move report file
        try:
            copy(reportfilename, newPath)
            os.unlink(reportfilename)

        except StandardError, msg:
            logging.error("failed to move %s to %s: %s" % \
                          (reportfilename, newPath, str(msg)))

        # using new report path
        reportfilename = newPath + os.path.basename(reportfilename)
        
        # get other files
        files = os.listdir(baseDir)
        
        # move all them
        for f in files:

            # get extension
            try:
                ext = os.path.splitext(f)[1]
                ext = ext.split('.')[1]
 
            except StandardError:
                ext = ""

            # create directory
            try:
                os.makedirs(newPath + ext)

            except StandardError, msg:
                pass
#                logging.error("failed to create directory %s: %s" % \
#                              (newPath + ext, str(msg)))

            # move file
            try:
                copy(baseDir+f, newPath+ext)
                os.unlink(baseDir+f)

            except StandardError, msg:
                logging.error("failed to move %s to %s: %s" % \
                              (baseDir + f, newPath + ext, str(msg)))

        # remove original files
        try:
            os.rmdir(baseDir)
            logging.debug("removing baseDir %s" % baseDir)

        except StandardError, msg:
            logging.error("error removing baseDir %s: %s" % \
                          (baseDir, str(msg)))
            
        try:
            chainDir = baseDir.split('/')
            chainDir.pop()
            chainDir.pop()
            chainDir = "/".join(chainDir)
            logging.debug("removing chainDir %s" % chainDir)
            os.rmdir(chainDir)

        except StandardError, msg:
            logging.error("error removing chainDir %s: %s" % \
                          (chainDir, str(msg)))

        # get max retries from JobState
        try:
            jobMaxRetries = JobState.general(fjr[0].jobSpecId)['MaxRetries']

        # does not work, try from Workflow entities
        except StandardError:

            try: 
                jobMaxRetries = WEjobState['max_retries']

            # assume a default value
            except StandardError:
                jobMaxRetries = 10

        logging.debug("maxretries = %s and resub = %s\n" % \
                      (jobMaxRetries,resub))

        # remove directory tree for finished jobs or when there will not
        # be resubmitted
        if success == "Success" or int(jobMaxRetries) <= int(resub):

            # get submission path
            try:
                subPath = BOSSCommands.subdir(jobId[0], self.bossCfgDir)

            except StandardError:
                subPath = ""

            logging.debug("SubmissionPath: %s" % subPath)

            # set status to ended
            if BOSSCommands.taskEnded(jobId[0], self.bossCfgDir):

                # remove directory tree
                try:
                    rmtree(subPath)
 
                # error, cannot remove files
                except StandardError, msg:
                    logging.error("Failed to remove files for job %s: %s" % \
                                  (jobId, str(msg)))

                    # remove ..id file,
                    # so that re-declaration is possible if needed
                try:
                    os.remove(
                        "%s/%sid" % (jobCacheDir,fjr[0].jobSpecId)
                        )
                except: 
                    logging.info( "not removed file %s/%sid" \
                                  % (jobCacheDir,fjr[0].jobSpecId)
                                  )
                    pass

                # archive job
                try:
                    BOSSCommands.archive(jobId[0], self.bossCfgDir)

                # error, cannot archive job
                except StandardError, msg:
                    logging.error("Failed to archive job %s: %s" % \
                                  (jobId, str(msg)))

        return reportfilename

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

        # get submitted jobs and lost jobs
        submittedJobs = newJobs.difference(oldJobs)
        lostJobs = oldJobs.difference(newJobs)

        # only update if there are changes
        if len(submittedJobs) == 0 and len(lostJobs) == 0:
            return oldJobs

        # open database
        Session.connect("bossdb")
        Session.start_transaction("bossdb")

        Session.execute("use " + self.bossDatabase['dbName'], "bossdb")

        # remove lost jobs
        numberOfJobs = len(lostJobs)

        if numberOfJobs != 0:

            # update DB
            jobs = ",".join(["'" + str(x) + "'" for x in lostJobs])
            query = "delete from jt_activejobs where job_id in (" + jobs + ")" 

            rowsModified = Session.execute(query, "bossdb")

            if rowsModified != numberOfJobs:
                logging.warning("Only %s of %s jobs removed from activejobs" % \
                                (rowsModified, numberOfJobs))

        # add new jobs
        numberOfJobs = len(submittedJobs)

        if numberOfJobs != 0:

            # update DB
            jobs = ",".join(["('" + str(x) + "')" for x in submittedJobs])
            query = "replace into jt_activejobs(job_id) values " + jobs

            rowsModified = Session.execute(query, "bossdb")

            if rowsModified != numberOfJobs:
                logging.warning("Only %s of %s jobs updated activejobs" % \
                                (rowsModified, numberOfJobs))

        # commit changes and set back default DB

        Session.commit("bossdb")
        Session.close("bossdb")

        return new

    def loadDict(self):
        """
        _loadDict_

        load information about active jobs from database
        """

        # build dictionary
        activeJobs = {}

        # query database for active jobs information
        Session.connect("bossdb")

        Session.execute("use " + self.bossDatabase['dbName'], "bossdb")
        Session.execute("select job_id from jt_activejobs", "bossdb")
        results = Session.fetchall("bossdb")
        Session.execute("use " + self.database['dbName'], "bossdb")

        Session.close("bossdb")

        # add jobs to dictionary
        for jobId in results:
            activeJobs[jobId[0]] = 0

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

        # wait for messages
        while True:

            # create session
            Session.connect("messages")
            Session.start_transaction("messages")

            # get a message
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (type, payload))

            # process it
            self.__call__(type, payload)

            # close session
            Session.commit("messages")
            Session.close("messages")


