#!/usr/bin/env python
"""
_JobHandling_

"""

__revision__ = "$Id"
__version__ = "$Revision"

import os
import re
import time
import logging
from shutil import copy
#from shutil import rmtree

# PA configuration
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as WEJob
from ProdCommon.Database import Session
from ProdAgentCore.ProdAgentException import ProdAgentException

# Blite API import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Scheduler import Scheduler
from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdAgentBOSS import BOSSCommands

# Framework Job Report handling
from ProdCommon.FwkJobRep.ReportState import checkSuccess
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.ReportParser import readJobReport


__version__ = "$Id: JobHandling.py,v 1.1.2.13 2008/04/08 15:00:33 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.13 $"

class JobHandling:
    """
    _JobHandling_
    """

    def __init__(self, params):
        """
        __init__
        """

        # store parameters and open a connection
        self.baseDir = params['baseDir']
        self.jobCreatorDir = params['jobCreatorDir']
        self.usingDashboard = params['usingDashboard']
        self.ms = params['messageServiceInstance']
        self.bossLiteSession = BossLiteAPI('MySQL', dbConfig)
        self.ft = re.compile( 'gsiftp://[\w.]+:\d+/*' )
        try:
            from ProdAgentCore.Configuration import loadProdAgentConfiguration
            config = loadProdAgentConfiguration()
            compCfg = config.getConfig("CrabServerConfigurations")
            self.tasksDir = compCfg["dropBoxPath"]
        except StandardError, ex:
            self.tasksDir = ''

    def performOutputProcessing(self, job):
        """
        __performOutputProcessing__
        """

        # get job information
        taskId = job['taskId']
        jobId = job['jobId']
        jobSpecId = job['name']

        # get outdir and report file name
        outdir = job.runningJob['outputDirectory']
        if outdir is None :
            outdir = self.buildOutdir( job )
        reportfilename = outdir + '/FrameworkJobReport.xml'
        
        # FIXME: temporary to emulate SE
        task = self.bossLiteSession.loadTask(job['taskId'], {'name' : ''})
        if task['outputDirectory'] is None \
               or self.ft.match( task['outputDirectory'] ) is not None or \
               not os.access( task['outputDirectory'], os.W_OK):
            toWrite = False
        else:
            toWrite = True

        # retrieve output message
        try :
            # FIXME: take the last action 
            outp = job.runningJob['statusHistory'][-1]
        except IndexError:
            logging.info( "##### No output information... try to continue..." )
            outp = ''

        # process!
        #  if output retrieval failed
        if outp.find('it has been purged') != -1 :
            logging.error( "Missing output file for job %s.%s: %s" % \
                           (jobId, taskId, outp) )

            logging.debug("Job " + jobSpecId + \
                          " has no FrameworkReport : creating a dummy one")

            # create job report
            self.writeFwkJobReport( jobSpecId, -1, reportfilename )

            # archive job, forcing a deleted status in the BOSS DB
            try:
                self.bossLiteSession.archive( job )
            except TaskError, msg:
                logging.error("Failed to archive job %s: %s" % \
                              (job['jobId'], str(msg)))

            # generate a failure message
            self.publishJobFailed(job, reportfilename, toWrite)
            return

        # proxy expire... nothing to do!
        elif outp.find("Proxy Expired") != -1 :
            logging.error( "Proxy expired for task %s: skipping" % taskId )
            return

        # FIXME: how to handle mismatching proxy?
        elif outp.find('Error with credential') != -1 :
            logging.error( "Proxy error for task %s: %s" % \
                           (taskId, outp) )
            return

        # successful output retrieval?
        elif outp.lower().find("error") < 0 and \
           outp.find("already been retrieved") < 0 :

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

                if job.runningJob['processStatus'] != 'failed' :
                    try:
                        exeCode = job.runningJob['wrapperReturnCode']
                        jobCode = job.runningJob['applicationReturnCode']
                        success = (exeCode == "0" or exeCode == "" or exeCode == None or exeCode == 'NULL') and (jobCode == "0")
                    except :
                    ## FIXME what to do?
                        pass

                logging.debug("check Job Success: %s" % str(success))

                # create db based Framework Job Report
                if success:
                    exitCode = 0
                else :
                    exitCode = -1

                self.writeFwkJobReport( jobSpecId, exitCode, reportfilename )

            # in both cases: is the job successful?
            if success:

                # yes, generate a job successful message and change status
                self.publishJobSuccess(job, reportfilename, toWrite)
                self.notifyJobState(jobSpecId)

            else:

                # no, generate a job failure message
                self.publishJobFailed(job, reportfilename, toWrite)

        # other problem... just display error
        else:
            logging.error(outp)

        return

    def buildOutdir( self, job ) :
        """
        __buildOutdir__

        compose outdir name and make the directory
        """

        # try with boss db
        task = self.bossLiteSession.loadTask(job['taskId'], {'name' : ''})
        if task['outputDirectory'] is not None \
               and task['outputDirectory'] != '' :
            outdir = task['outputDirectory']
        else :
            outdir = self.baseDir

        # FIXME: temporary to emulate SE
        # SE? 
        stdir = self.ft.match( outdir )
        if stdir is not None or not os.access( outdir, os.W_OK):
            # outdir = outdir[stdir.end()-1:]
            outdir = self.tasksDir + '/' + task['name'] + '_spec'

        # FIXME: get outdir
        outdir = "%s/BossJob_%s_%s/Submission_%s/" % \
                 (outdir, job['taskId'], job['jobId'], job['submissionNumber'])

        # make outdir
        logging.info("Creating directory: " + outdir)
        try:
            os.makedirs( outdir )
        except OSError, err:
            if  err.errno == 17:
                # existing dir
                pass
            else :
                logging.error("Cannot create directory : " + str(err))
                raise

        # return outdir
        return outdir

        
    def writeFwkJobReport( self, jobSpecId, exitCode, reportfilename ):
        """
        __writeFwkJobReport__

        write a fajke reportfilename based on the statu reported
        """

        # create job report
        logging.debug("Creating report %s" % reportfilename)
        fwjr = FwkJobReport()
        fwjr.jobSpecId = jobSpecId
        if exitCode == 0 :
            fwjr.status = "Success"
            fwjr.exitCode = 0
        else :
            fwjr.exitCode = exitCode
            fwjr.status = "Failed"

        # store job report
        fwjr.write(reportfilename)


    def publishJobSuccess(self, job, reportfilename, local=True):
        """
        __publishJobSuccess__
        """

        # set success job status
        if local :
            reportfilename = self.archiveJob("Success", job, reportfilename)
        else :
            # archive job
            self.bossLiteSession.archive( job )

        # publish success event
        self.ms.publish("JobSuccess", reportfilename)
        self.ms.commit()

        logging.info("Published JobSuccess with payload :%s" % \
                     reportfilename)
        return


    def publishJobFailed(self, job, reportfilename, local=True):
        """
        __publishJobFailed__
        """

        # set failed job status
        if local :
            reportfilename = self.archiveJob("Failed", job, reportfilename)
        else :
            # archive job
            self.bossLiteSession.archive( job )

        # publish job failed event
        self.ms.publish("JobFailed", reportfilename)
        self.ms.commit()

        logging.info("published JobFailed with payload: %s" % \
                     reportfilename)

        return


    def resolveOutdir(self, job, fjr):
        """
        __resolveOutdir__
        """

        # fallback directory in JobTracking.
        fallbackCacheDir = self.baseDir + "/%s" % fjr[0].jobSpecId

        # try to get cache from JobState
        try:
            jobCacheDir = \
                        JobState.general(fjr[0].jobSpecId)['CacheDirLocation']

        # error, cannot get cache location
        except StandardError, ex:
            msg = "Cannot get JobCache from JobState.general for %s\n" % \
                  fjr[0].jobSpecId
            msg += "Retrying with a rebuild session object\n"
            msg += str(ex)
            logging.warning(msg)

            # retrying
            try:
                # force a re connect operation
                self.recreateSession()

                jobCacheDir = \
                        JobState.general(fjr[0].jobSpecId)['CacheDirLocation']
                logging.info("Retry OK!")

            # error, cannot get cache location
            except StandardError, ex:
                msg = "No, retry does not work"
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
                    msg += 'Retrying with a rebuild session object\n'
                    msg += str(ex)
                    logging.warning(msg)

                    try:
                        # force a re connect operation
                        self.recreateSession()

                        WEjobState = WEJob.get(fjr[0].jobSpecId)
                        jobCacheDir = WEjobState['cache_dir']

                    # error, cannot get cache location
                    except StandardError, ex:
                        msg = "Cant get JobCache from Job.get['cache_dir'] for %s" % \
                              fjr[0].jobSpecId
                        msg += 'No, retry does not work\n'
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

        return jobCacheDir


    def archiveJob(self, success, job, reportfilename):
        """
        __archiveJob__

        Moves output file to archdir
        """

        # get resubmission count
        resub = job['submissionNumber']

        # get job report
        fjr = readJobReport(reportfilename)

        # get directory information
        jobCacheDir = self.resolveOutdir( job, fjr)
        baseDir = os.path.dirname(reportfilename) + "/"
        lastdir = os.path.dirname(reportfilename).split('/').pop()
        newPath = jobCacheDir + "/JobTracking/" + success + "/" + lastdir + "/"

        # create directory if not there
        try:
            os.makedirs(newPath)
        except OSError, err:
            if  err.errno == 17:
                # existing dir
                pass
            else :
                logging.error("Cannot create directory : " + str(err))

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
            except OSError, err:
                if  err.errno == 17:
                    # existing dir
                    pass
                else :
                    logging.error("Cannot create directory : " + str(err))

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
                # force a re connect operation
                self.recreateSession()
                WEjobState = WEJob.get(fjr[0].jobSpecId)
                jobMaxRetries = WEjobState['max_retries']

            # assume a default value
            except StandardError:
                jobMaxRetries = 10

        logging.debug("maxretries = %s and resub = %s\n" % \
                      (jobMaxRetries,resub))

        # remove directory tree for finished jobs or when there will not
        # be resubmitted
        if success == "Success" or int(jobMaxRetries) <= int(resub):

            # subPath = job.runningJob['submissionPath']
            # if subPath is None :
            #     subPath = ""

            # logging.debug("SubmissionPath: %s" % subPath)

            # set status to ended
            endedJobs = 0
            totJobs = 0
            try:
                taskObj = self.bossLiteSession.loadTask( job['taskId'] )
                for tjob in taskObj.jobs:
                    self.bossLiteSession.getRunningInstance(tjob)
                    totJobs += 1
                    #if tjob.runningJob['status'] in ["E", "SA", "SK", "SE"] \
                    if tjob.runningJob['closed'] == 'Y' \
                           and tjob['submissionNumber'] >= jobMaxRetries:
                        endedJobs += 1
            except TaskError, err:
                logging.error( "Unable to retrieve task information: %s" \
                               % str(err) )

            if totJobs != 0 and endedJobs == totJobs:

                # remove directory tree
                #try:
                #    rmtree(subPath)

                # error, cannot remove files
                #except StandardError, msg:
                #    logging.error("Failed to remove files for job %s: %s" % \
                #                  (str(job), str(msg)))

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

            # archive job
            try:
                self.bossLiteSession.archive( job )

            # error, cannot archive job
            except TaskError, msg:
                logging.error("Failed to archive job %s: %s" % \
                              (job['jobId'], str(msg)))

        return reportfilename


    def dashboardPublish(self, job):
        """
        __dashboardPublish__
        
        publishes dashboard info
        """

        # dashboard information
        ( dashboardInfo, dashboardInfoFile )= BOSSCommands.guessDashboardInfo(
            job, self.bossLiteSession
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


    def notifyJobState(self, job):
        """
        __notifyJobState__

        Notify the JobState DB of finished jobs
        """

        # set finished job state
        try:
            JobState.finished(job)

        # error
        except Exception, ex:
            msg = "Error setting job state to finished for job: %s\n" \
                  % job['jobId']
            msg += str(ex)
            logging.error(msg)

        return

    def recreateSession(self):
        """
        __recreateSession__
        
        fix to recreate standard default session object
        """

        # force a re connect operation
        try:
            Session.session['default']['connection'].close()
        except:
            pass
        Session.session = {}
        Session.connect()


    def fullId( self, job ):
        """
        __fullId__
        
        compose job primary keys in a string
        """

        return str( job['taskId'] ) + '.' \
               + str( job['jobId'] ) + '.' \
               + str( job['submissionNumber'] )
