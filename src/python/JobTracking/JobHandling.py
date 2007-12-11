#!/usr/bin/env python
"""
_JobHandling_

"""

import os
import time
import logging
from shutil import copy
from shutil import rmtree

# PA configuration
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as WEJob
from ProdCommon.Database import Session

# BOSS
from ProdAgentBOSS import BOSSCommands

# Framework Job Report handling
from FwkJobRep.ReportState import checkSuccess
from FwkJobRep.FwkJobReport import FwkJobReport
from FwkJobRep.ReportParser import readJobReport

__version__ = "$Id$"
__revision__ = "$Revision$"

class JobHandling:
    """
    _JobHandling_
    """

    def __init__(self, params):
        """
        __init__
        """

        # store parameters and open a connection
        self.bossCfgDir = params['bossCfgDir']
        self.baseDir = params['baseDir']
        self.jobCreatorDir = params['jobCreatorDir']
        self.usingDashboard = params['usingDashboard']
        self.ms = params['messageServiceInstance']

    def performOutputProcessing(self, jobInfo):
        """
        _performOutputProcessing_
        """

        # get job information
        jobId = jobInfo['jobId']
        jobSpecId = jobInfo['jobSpecId']
        outp = jobInfo['output']
        bossStatus = jobInfo['bossStatus']
        jobData = [jobId, bossStatus]

        # successful output retrieval?
        if outp.find("-force") < 0 and \
           outp.find("error") < 0 and \
           outp.find("already been retrieved") < 0:

            # yes, get report file name
            reportfilename = BOSSCommands.reportfilename(jobId, \
                                                         self.bossCfgDir)
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
                # Marco. Trying to get also error code from Boss.
                # success = BOSSCommands.checkSuccess(jobId, \
                (success, exeCode, jobCode) = BOSSCommands.checkSuccess(jobId, \
                                                    self.bossCfgDir)
                logging.debug("check Job Success: %s" % str(success))
                logging.info("check Job Success: %s %s %s" %(str(success),str(exeCode), str(jobCode)))

                # create BOSS based Framework Job Report
                fwjr = FwkJobReport()
                fwjr.jobSpecId = jobSpecId
                reportfilename = BOSSCommands.reportfilename(jobId, \
                                                       self.baseDir)

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
                    #fwjr.addError(exeCode, "CmsRun")
                    #fwjr.addError(jobCode, "Wrapper")

                try:
                    os.makedirs(os.path.dirname(reportfilename))
                except OSError:
                    pass

                # store job report
                fwjr.write(reportfilename)

            # in both cases: is the job successful?
            if success:

                # yes, generate a job successful message and change status
                self.publishJobSuccess(jobData, reportfilename)
                self.notifyJobState(jobSpecId)

            else:

                # no, generate a job failure message
                self.publishJobFailed(jobData, reportfilename)

        # else if output retrieval failed
        elif outp.find("Unable to find output sandbox file:") >= 0 \
                 or outp.find("Error retrieving Output") >= 0 \
                 or outp.find("Error extracting files ") >= 0 :

            logging.debug("Job " + jobId.__str__() + \
                          " has no FrameworkReport : creating a dummy one")

            # create job report
            fwjr = FwkJobReport()
            fwjr.jobSpecId = jobSpecId
            reportfilename = BOSSCommands.reportfilename(jobId, \
                                                         self.baseDir)
            fwjr.exitCode = -1
            fwjr.status = "Failed"

            try:
                os.makedirs( os.path.dirname(reportfilename) )
            except OSError:
                pass

            # store job report
            fwjr.write(reportfilename)

            # archive job, forcing a deleted status in the BOSS DB
            BOSSCommands.Delete(jobId, self.bossCfgDir)

            # generate a failure message
            self.publishJobFailed(jobData, reportfilename)

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
            logging.error( "unable to retrieve DashboardId for: (%s,%s)" % \
                           (jobId,jobSpecId) )
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

    def publishJobSuccess(self, jobId, reportfilename):
        """
        _jobSuccess_
        """

        # set success job status
        reportfilename = self.archiveJob("Success", jobId, reportfilename)

        # publish success event
        self.ms.publish("JobSuccess", reportfilename)
        self.ms.commit()

        logging.info("Published JobSuccess with payload :%s" % \
                     reportfilename)
        return

    def publishJobFailed(self, jobId, reportfilename):
        """
        _jobFailed_
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
            logging.error("archiveJob for job %s failed: %s" % \
                          (jobId[0], str(msg)))
            return reportfilename

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
                logging.working("Retry OK!")

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
        except Exception, ex:
            msg = "Error setting job state to finished for job: %s\n" \
                  % jobId
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

