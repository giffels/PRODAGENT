#!/usr/bin/env python
"""
_JobHandling_

"""

__revision__ = "$Id"
__version__ = "$Revision"

import os
import logging
import re
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
from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError

# Framework Job Report handling
from ProdCommon.FwkJobRep.ReportState import checkSuccess
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface
from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo

__version__ = "$Id: JobHandling.py,v 1.1.2.34 2008/05/13 17:17:59 gcodispo Exp $"
__revision__ = "$Revision: 1.1.2.34 $"

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
        self.outputLocation = params['OutputLocation']
        self.bossLiteSession = BossLiteAPI('MySQL', dbConfig)
        self.configs = params['OutputParams']
        self.ft = re.compile( 'gsiftp://[-\w.]+[:\d]*/*' )


    def performOutputProcessing(self, job):
        """
        __performOutputProcessing__
        """

        logging.info( "Evaluate job %s and publish the job results" \
                      % self.fullId(job) )
        # get job information
        jobId = str(job['jobId'])
        jobSpecId = job['name']
                   
        # job status
        exitCode = -1
        success = False

        # get outdir and report file name
        outdir = job.runningJob['outputDirectory']
        
        # temporary to emulate SE
        # task = self.bossLiteSession.loadTask(job['taskId'], deep=False)
        #
        # if task['outputDirectory'] is None \
        #        or self.ft.match( task['outputDirectory'] ) is not None or \
        #        not os.access( task['outputDirectory'], os.W_OK):
        #     toWrite = False
        # else:
        #     toWrite = True

        if self.outputLocation == "SE" \
               and job.runningJob['processStatus'] != 'failed':
            try :
                self.reportRebounce( job )
            ## temporary workaround for OSB rebounce # Fabio
            #    self.osbRebounce( job )
            except :
                # as dirt as needed: any unknown error
                import traceback
                msg = traceback.format_exc()
                output = str(msg)
                logging.error("FAILED REBOUNCE for job %s.%s: %s" % \
                              (job['taskId'], job['jobId'], output ) )
                return
                

        # is the FwkJobReport there?
        reportfilename = outdir + '/FrameworkJobReport.xml'
        fwjrExists = os.path.exists(reportfilename)
        if not fwjrExists:
            tmp = outdir + '/crab_fjr_' + jobId + '.xml'
            fwjrExists = os.path.exists(tmp)
            if fwjrExists:
                reportfilename = tmp
        logging.debug("report file name %s exists: %s" % \
                      (reportfilename, str(fwjrExists)) )

        # is the FwkJobReport there?
        if fwjrExists:

            # check success
            success, exitCode = self.parseFinalReport(reportfilename, job)

            logging.debug("check Job Success: %s" % str(success))

        # FwkJobReport not there: create one based on db or assume failed
        else:
            # May be the job is aborted
            if job.runningJob['processStatus'] == 'failed' :
                success = False
                exitCode = -1

            # otherwise, missing just missing fwjr
            else :
                job.runningJob["applicationReturnCode"] = str(50117)
                job.runningJob["wrapperReturnCode"] = str(50117)
                success = False
                exitCode = 50117

            # write fake fwjr
            logging.debug("write fake fwjr: %s" % str(success))
            self.writeFwkJobReport( jobSpecId, exitCode, reportfilename )


        # in both cases: is the job successful?
        if success:

            # yes, generate a job successful message and change status
            self.publishJobSuccess(job, reportfilename)
            self.notifyJobState(jobSpecId)

        else:

            # no, generate a job failure message
            self.publishJobFailed(job, reportfilename)

        return

        
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


    def parseFinalReport(self, reportfilename, job):
        """
        __parseFinalReport__
        
        Parses the FWJR produced by job in order to retrieve 
        the WrapperExitCode and ExeExitCode.
        Updates the BossDB with these values.

        """

        # defaults
        success = False
        exitCode = 0
       
        if os.path.getsize(reportfilename) == 0 :
            return( success, -1 )
            ### 50117 -
            ### cmsRun did not produce a valid/readable job report at runtime
            # job.runningJob["applicationReturnCode"] = str(50117)
            # job.runningJob["wrapperReturnCode"] = str(50117)
            # self.writeFwkJobReport( job['name'], 50117, reportfilename )
            # return ( False, 50117 )

        # read standard info
        try :
            jobReport = readJobReport(reportfilename)[0]
            success = ( jobReport.status == "Success" )
            exitCode = jobReport.exitCode
        except Exception, err:
            logging.error('Invalid Framework Job Report : %s' %str(err) )
            return( success, -1 )


        # read CS specific info
        try :
            for report in jobReport.errors:
                if report['Type'] == 'WrapperExitCode':
                    job.runningJob["wrapperReturnCode"] = report['ExitStatus']
                elif report['Type'] == 'ExeExitCode':     
                    job.runningJob["applicationReturnCode"] = report['ExitStatus']
                else:
                    continue
            return( success, exitCode )
        except:
            pass

        if job.runningJob["wrapperReturnCode"] is None and \
           job.runningJob["applicationReturnCode"] is None :
            job.runningJob["wrapperReturnCode"] = exitCode
            job.runningJob["applicationReturnCode"] = exitCode

        return( success, exitCode )


    def publishJobSuccess(self, job, reportfilename):
        """
        __publishJobSuccess__
        """

        # set success job status
        if self.outputLocation != "SE" :
            reportfilename = self.archiveJob("Success", job, reportfilename)
        else :
            # archive job
            try :
                self.bossLiteSession.archive( job )
            except JobError:
                logging.error("Unable to archive job %s.%s" % \
                              (job['taskId'], job['jobId'] ) )

        # publish success event
        self.ms.publish("JobSuccess", reportfilename)
        self.ms.commit()

        logging.info("Published JobSuccess with payload :%s" % \
                     reportfilename)
        return


    def publishJobFailed(self, job, reportfilename):
        """
        __publishJobFailed__
        """

        # set failed job status
        if self.outputLocation != "SE" :
            reportfilename = self.archiveJob("Failed", job, reportfilename)
        else :
            # archive job
            try :
                self.bossLiteSession.archive( job )
            except JobError:
                logging.error("Unable to archive job %s.%s" % \
                              (job['taskId'], job['jobId'] ) )

        # publish job failed event
        self.ms.publish("JobFailed", reportfilename)
        self.ms.commit()

        logging.info("published JobFailed with payload: %s" % \
                     reportfilename)

        return


    def resolveOutdir(self, job):
        """
        __resolveOutdir__
        """

        # fallback directory in JobTracking.
        fallbackCacheDir = self.baseDir + "/%s" % job['name']

        # try to get cache from JobState
        try:
            jobCacheDir = \
                        JobState.general(job['name'])['CacheDirLocation']

        # error, cannot get cache location
        except StandardError, ex:
            msg = "Cannot get JobCache from JobState.general for %s\n" % \
                  job['name']
            msg += "Retrying with a rebuild session object\n"
            msg += str(ex)
            logging.warning(msg)

            # retrying
            try:
                # force a re connect operation
                self.recreateSession()

                jobCacheDir = \
                        JobState.general(job['name'])['CacheDirLocation']
                logging.info("Retry OK!")

            # error, cannot get cache location
            except StandardError, ex:
                msg = "No, retry does not work"
                msg += str(ex)
                logging.warning(msg)

                # try to get cache from WEJob
                try:
                    WEjobState = WEJob.get(job['name'])
                    jobCacheDir = WEjobState['cache_dir']

                # error, cannot get cache location
                except StandardError, ex:
                    msg = "Cant get JobCache from Job.get['cache_dir'] for %s" % \
                          job['name']
                    msg += 'Retrying with a rebuild session object\n'
                    msg += str(ex)
                    logging.warning(msg)

                    try:
                        # force a re connect operation
                        self.recreateSession()

                        WEjobState = WEJob.get(job['name'])
                        jobCacheDir = WEjobState['cache_dir']

                    # error, cannot get cache location
                    except StandardError, ex:
                        msg = "Cant get JobCache from Job.get['cache_dir'] for %s" % \
                              job['name']
                        msg += 'No, retry does not work\n'
                        msg += str(ex)
                        logging.warning(msg)

                        # try guessing the JobCache area based on jobspecId name
                        try:

                            # split the jobspecid=workflow-run into workflow/run
                            spec = job['name']
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
        jobSpecId = job['name']

        # get directory information
        jobCacheDir = self.resolveOutdir( job, reportfilename)
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
            jobMaxRetries = JobState.general(jobSpecId)['MaxRetries']

        # does not work, try from Workflow entities
        except StandardError:

            try:
                # force a re connect operation
                self.recreateSession()
                WEjobState = WEJob.get(jobSpecId)
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
                taskObj = self.bossLiteSession.loadTask( job['taskId'], \
                                                         deep=False )
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
                        "%s/%sid" % (jobCacheDir,jobSpecId)
                        )
                except:
                    logging.info( "not removed file %s/%sid" \
                                  % (jobCacheDir,jobSpecId)
                                  )

            # archive job
            try:
                self.bossLiteSession.archive( job )

            # error, cannot archive job
            except TaskError, msg:
                logging.error("Failed to archive job %s.%s: %s" % \
                              (job['taskId'], job['jobId'], str(msg)))

        return reportfilename


    def dashboardPublish(self, job):
        """
        __dashboardPublish__
        
        publishes dashboard info
        """
        
        dashboardInfo = DashboardInfo()
        dashboardInfoFile = None

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


    def notifyJobState(self, job):
        """
        __notifyJobState__

        Notify the JobState DB of finished jobs
        """

        # set finished job state
        try:
            self.recreateSession()
            JobState.finished(job)

        # error
        except Exception, ex:
            msg = "Error setting job state to finished for job: %s\n" \
                  % str(job['jobId'])
            msg += str(ex)
            logging.error(msg)

        return

    def recreateSession(self):
        """
        __recreateSession__
        
        fix to recreate standard default session object
        """

        # force a re connect operation
        Session.set_database(dbConfig)
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


    def reportRebounce( self, job ):
        """
        __reportRebounce__
        
        """

        logging.info("Output rebounce: %s.%s " \
                     % ( job['taskId'], job['jobId'] ) )

        # loading task
        task = self.bossLiteSession.loadTask( job['taskId'], deep=False )

        # fwjr name
        reportName = 'crab_fjr_' + str(job['jobId']) + '.xml'

        # remote source name
        outputDirectory = task['outputDirectory']
        out = self.ft.match( outputDirectory )
        if out is not None :
            outputDirectory = outputDirectory[out.end()-1:]
        source = os.path.join( outputDirectory, reportName )

        # local destination name
        dest = os.path.join( job.runningJob['outputDirectory'], reportName )
        
        # initialize tranfer protocol
        seEl = SElement( self.configs["storageName"], \
                         self.configs["Protocol"],    \
                         self.configs["storagePort"] )
        loc = SElement("localhost", "local")
        sbi = SBinterface( seEl, loc )

        # transfer fwjr
        try: 
            logging.debug( 'REBOUNCE DBG %s, %s'%(source, dest) ) 
            sbi.copy( source, dest, task['user_proxy'])
        except Exception, e:
            logging.info("Report rebounce transfer fail for %s.%s: %s " \
                         % ( job['taskId'], job['jobId'], str(e) ) )

        logging.info("Report rebounce completed for %s.%s " \
                     % ( job['taskId'], job['jobId'] ) )
        return 


    ######################
    # TODO remove this temporary workaround      # Fabio
    #  once the OSB bypass problem will be fixed # Fabio
    # This is a mess and must be removed ASAP    # Fabio
    def osbRebounce( self, job ):
        """
        __osbRebounce__
        
        """
         
        localOutDir = job.runningJob['outputDirectory']
        localOutputTgz = [ localOutDir +'/'+ f.split('/')[-1]
                           for f in job['outputFiles'] if '.tgz' in f ]
        localOutputTgz = [ f for f in localOutputTgz if os.path.exists(f) ]

        logging.info( 'REBOUNCE DBG %s, %s, %s' \
                      % (localOutDir, localOutputTgz, \
                         [ localOutDir +'/'+ f.split('/')[-1]
                           for f in job['outputFiles'] ] ) )

        if len(localOutputTgz)==0:
            return   

        task = self.bossLiteSession.loadTask( job['taskId'], deep=False )
        logging.info("Output rebounce: %s.%s " \
                     % ( job['taskId'], job['jobId'] ) )
        seEl = SElement( self.configs["storageName"], \
                         self.configs["Protocol"],    \
                         self.configs["storagePort"] )
        loc = SElement("localhost", "local")

        ## copy ISB ##
        sbi = SBinterface( loc, seEl )
        filesToClean = []
        for filetocopy in localOutputTgz:
            source = os.path.abspath(filetocopy)
            dest = os.path.join(
                task['outputDirectory'], os.path.basename(filetocopy) )
            try: 
                ## logging.info( 'REBOUNCE DBG %s, %s'%(source, dest) ) 
                sbi.copy( source, dest, task['user_proxy'])
                filesToClean.append(source)
            except Exception, e:
                logging.info("Output rebounce transfer fail for %s.%s: %s " \
                             % ( job['taskId'], job['jobId'], str(e) ) )
                continue 

        logging.info("Output rebounce completed for %s.%s " \
                     % ( job['taskId'], job['jobId'] ) )
        for filetoclean in filesToClean:
            try: 
                os.remove( filetoclean )   

            except Exception, e:
                logging.info(
                    "Output rebounce local clean fail for %s.%s: %s " \
                    % ( job['taskId'], job['jobId'], str(e) ) )
                continue
        logging.info("Output rebounce clean for %s.%s " \
                     % ( job['taskId'], job['jobId'] ) )
        return 
    ######################
