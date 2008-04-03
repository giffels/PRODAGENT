#!/usr/bin/env python
"""
_JobHandling_

"""

import os
import logging
from shutil import copy
from shutil import rmtree

# PA configuration
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as WEJob
from ProdCommon.Database import Session
from ProdAgentCore.ProdAgentException import ProdAgentException

# Blite API import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import TaskError

# Framework Job Report handling
from ProdCommon.FwkJobRep.ReportState import checkSuccess
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.ReportParser import readJobReport


__version__ = "$Id: JobHandling.py,v 1.1.2.7 2008/04/02 15:27:15 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.7 $"

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

    def performOutputProcessing(self, job):
        """
        _performOutputProcessing_
        """

        # get job information
        taskId = job['taskId']
        jobId = job['jobId']
        submTimeN = job['submissionNumber']
        jobSpecId = job['name']

        # FIXME: get report file name and outdir
        outdir = "%s/BossJob_%s_%s/Submission_%s/" \
                              % (self.baseDir, taskId, jobId, submTimeN)
        reportfilename = outdir + 'FrameworkJobReport.xml'

        # make outdir
        try:
            os.makedirs( outdir )
        except OSError, err:
            if  err.errno == 17:
                # existing dir
                pass
            else :
                logging.error("Cannot create directory : " + str(err))
                return

        # retrieve output message
        try :
            # FIXME: take the last action 
            outp = job.runningJob['statusHistory'][-1]
        except:
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
            except TaskError, exc:
                pass

            # generate a failure message
            self.publishJobFailed(job, reportfilename)
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

                try:
                    exeCode = job.runningJob['wrapperReturnCode']
                    jobCode = job.runningJob['applicationReturnCode']
                    success = (exeCode == "0" or exeCode == "" or exeCode == None or exeCode == 'NULL') and (jobCode == "0")
                except Exception, exc:
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
                self.publishJobSuccess(job, reportfilename)
                self.notifyJobState(jobSpecId)

            else:

                # no, generate a job failure message
                self.publishJobFailed(job, reportfilename)

        # other problem... just display error
        else:
            logging.error(outp)

        return


    def writeFwkJobReport( self, jobSpecId, exitCode, reportfilename ):
        """
        _writeFwkJobReport_
        """

        # create job report
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


    def publishJobSuccess(self, job, reportfilename):
        """
        _jobSuccess_
        """

        # set success job status
        reportfilename = self.archiveJob("Success", job, reportfilename)

        # publish success event
        self.ms.publish("JobSuccess", reportfilename)
        self.ms.commit()

        logging.info("Published JobSuccess with payload :%s" % \
                     reportfilename)
        return

    def publishJobFailed(self, job, reportfilename):
        """
        _jobFailed_
        """

        # archive the job in BOSS DB
        reportfilename = self.archiveJob("Failed", job, reportfilename)

        # publish job failed event
        self.ms.publish("JobFailed", reportfilename)
        self.ms.commit()

        logging.info("published JobFailed with payload: %s" % \
                     reportfilename)

        return

    def archiveJob(self, success, job, reportfilename):
        """
        _archiveJob_

        Moves output file to archdir
        """

        # get resubmission count
        resub = job['submissionNumber']

        # get directory information
        lastdir = os.path.dirname(reportfilename).split('/').pop()
        baseDir = os.path.dirname(reportfilename) + "/"
        # get job report
        fjr = readJobReport(reportfilename)

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

        # build path and report file name
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
                jobMaxRetries = WEjobState['max_retries']

            # assume a default value
            except StandardError:
                jobMaxRetries = 10

        logging.debug("maxretries = %s and resub = %s\n" % \
                      (jobMaxRetries,resub))

        # remove directory tree for finished jobs or when there will not
        # be resubmitted
        if success == "Success" or int(jobMaxRetries) <= int(resub):

            subPath = job.runningJob['submissionPath']
            if subPath is None :
                subPath = ""

            logging.debug("SubmissionPath: %s" % subPath)

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
            except TaskError, te:
                taskObj = None

            if endedJobs == totJobs:

                # remove directory tree
                try:
                    rmtree(subPath)

                # error, cannot remove files
                except StandardError, msg:
                    logging.error("Failed to remove files for job %s: %s" % \
                                  (str(job), str(msg)))

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

    def notifyJobState(self, job):
        """
        _notifyJobState_

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
        fix to recreate standard default session object
        """

        # force a re connect operation
        try:
            Session.session['default']['connection'].close()
        except:
            pass
        Session.session = {}
        Session.connect()

