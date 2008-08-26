#!/usr/bin/env python
"""
_JobHandling_

"""


__revision__ = "$Id: JobHandling.py,v 1.3 2008/08/22 15:31:16 gcodispo Exp $"
__version__ = "$Revision: 1.3 $"

import os
import logging
import re
from shutil import copy
#from shutil import rmtree

# PA configuration
from ProdAgent.WorkflowEntities import JobState
#from ProdAgent.WorkflowEntities import Job as WEJob
from ProdCommon.Database import Session
#from ProdAgentCore.ProdAgentException import ProdAgentException

# Blite API import
#from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
#from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError

# Framework Job Report handling
#from ProdCommon.FwkJobRep.ReportState import checkSuccess
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface


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
        self.ms = params['messageServiceInstance']
        self.outputLocation = params['OutputLocation']
        self.bossLiteSession = params['bossLiteSession']
        self.database = params['database']
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

        # if SE rebounce fwjr
        if self.outputLocation == "SE" :
            try :
                if job.runningJob['processStatus'] != 'failed':
                    self.reportRebounce( job )
                else :
                    self.rebounceLoggingInfo( job )
            except :
                # as dirt as needed: any unknown error
                import traceback
                msg = traceback.format_exc()
                output = str(msg)
                logging.error("Job %s FAILED REBOUNCE : %s" % \
                              (self.fullId(job), output ) )
                return

        # if condorG rebounce full sandbox
        elif self.outputLocation == "SEcopy" :
            try :
                if job.runningJob['processStatus'] != 'failed':
                    self.rebounceOSB( job )
                else :
                    self.rebounceLoggingInfo( job )
            except :
                # as dirt as needed: any unknown error
                import traceback
                msg = traceback.format_exc()
                output = str(msg)
                logging.error("Job %s FAILED REBOUNCE : %s" % \
                              (self.fullId(job), output ) )
                return
            

        # is the FwkJobReport there?
        reportfilename = outdir + '/FrameworkJobReport.xml'
        fwjrExists = os.path.exists(reportfilename)
        if not fwjrExists:
            tmp = outdir + '/crab_fjr_' + jobId + '.xml'
            fwjrExists = os.path.exists(tmp)
            if fwjrExists:
                reportfilename = tmp
        logging.debug("Job %s : report file name %s exists: %s" % \
                      (self.fullId(job), reportfilename, str(fwjrExists)) )

        # is the FwkJobReport there?
        if fwjrExists:

            # check success
            success, exitCode = self.parseFinalReport(reportfilename, job)

            logging.debug("Job %s check Job Success: %s" % \
                          (self.fullId(job), str(success)) )

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
            logging.debug("Job %s : write fake fwjr: %s" % \
                          (self.fullId(job), str(success)) )
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
                logging.error("Job %s : Unable to archive" % self.fullId(job) )

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
                logging.error("Job %s : Unable to archive" % self.fullId(job) )

        # publish job failed event
        self.ms.publish("JobFailed", reportfilename)
        self.ms.commit()

        logging.info("Job %s : published JobFailed with payload: %s" % \
                     (self.fullId(job), reportfilename) )

        return


    def archiveJob(self, success, job, reportfilename):
        """
        __archiveJob__

        Moves output file to archdir
        """

        # get directories
        jobOutDir = job.runningJob['outputDirectory']

        # after a component recovery, the output can be already at destination
        if jobOutDir.find( 'JobTracking' ) != -1 :
            return reportfilename

        baseDir = os.path.dirname(jobOutDir)
        newPath = baseDir + "/JobTracking/" + success + "/" \
                  + os.path.basename(jobOutDir)

        # create directory if not there
        try:
            os.makedirs(newPath)
        except OSError, err:
            if  err.errno == 17:
                # existing dir
                pass
            else :
                logging.error("Job %s : Cannot create directory %s" % \
                              (self.fullId(job), str(err)) )

        # move report file
        try:
            copy(reportfilename, newPath)
            os.unlink(reportfilename)

        except StandardError, msg:
            logging.error(
                "Job %s : failed to move %s to %s: %s" % \
                (self.fullId(job), reportfilename, newPath, str(msg)))

        # using new report path
        reportfilename = os.path.join( newPath,
                                       os.path.basename(reportfilename) )

        # get other files
        files = os.listdir(jobOutDir)

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
                os.makedirs( os.path.join( newPath, ext) )
            except OSError, err:
                if  err.errno == 17:
                    # existing dir
                    pass
                else :
                    logging.error("Job %s : Cannot create directory %s" % \
                                  (self.fullId(job), str(err)) )

            except StandardError, msg:
                pass
#                logging.error("failed to create directory %s: %s" % \
#                              (newPath + ext, str(msg)))

            # move file
            try:
                start = os.path.join( jobOutDir, f)
                copy( start, os.path.join( newPath, ext) )
                os.unlink( start )

            except StandardError, msg:
                logging.error("Job %s : failed to move %s to %s: [%s]" % \
                              (self.fullId(job), os.path.join( jobOutDir, f), \
                               os.path.join( newPath, ext), str(msg)))

        # remove original files
        try:
            os.rmdir(jobOutDir)
            logging.debug("Job %s : removing baseDir %s" % \
                          (self.fullId(job), jobOutDir) )

        except StandardError, msg:
            logging.error("Job %s : error removing baseDir %s: %s" % \
                          (self.fullId(job), baseDir, str(msg)))

        # archive job
        try:
            job.runningJob['outputDirectory'] = newPath
            self.bossLiteSession.archive( job )

            # error, cannot archive job
        except JobError, msg:
            logging.error("Job %s : Failed to archive : %s" % \
                          (self.fullId(job), str(msg)))


        return reportfilename


    def notifyJobState(self, job):
        """
        __notifyJobState__

        Notify the JobState DB of finished jobs
        """

        # set finished job state
        try:
            try:
                JobState.finished(job)
                Session.commit()
            except :
                logging.warning(
                    "failed connection for JobState Notify, trying recovery" )
                self.recreateSession()
                JobState.finished(job)
                Session.commit()

        # error
        except Exception, ex:
            msg = "Error setting job state to finished for job: %s\n" \
                  % str(job)
            msg += str(ex)
            logging.error(msg)
        except :
            msg = "Error setting job state to finished for job: %s\n" \
                  % str(job)
            import traceback
            msg += traceback.format_exc()
            logging.error(msg)

        return

    def recreateSession(self):
        """
        __recreateSession__

        fix to recreate standard default session object
        """

        Session.set_database(self.database)

        # force a re connect operation
        try:
            Session.session['default']['connection'].close()
        except:
            pass
        Session.session = {}
        Session.set_database(self.database)
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

        logging.info("Job %s : report rebounce" % self.fullId(job) )

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
            logging.debug( 'Job %s REBOUNCE DBG : %s, %s' % \
                           (self.fullId(job), source, dest) )
            sbi.copy( source, dest, task['user_proxy'])
        except Exception, e:
            logging.info("Job %s Report rebounce transfer fail : %s " \
                         % ( self.fullId(job), str(e) ) )

        logging.info("Job %s Report rebounce completed" % self.fullId(job) )
        return

    def rebounceLoggingInfo( self, job ):
        """
        __rebounceLoggingInfo__

        """

        logging.info("Job %s : loggingInfo.log rebounce" % self.fullId(job) )
        
        localOutDir = job.runningJob['outputDirectory']

        source = os.path.join( localOutDir, 'loggingInfo.log' )
        if os.path.exists( source ):
            task = self.bossLiteSession.loadTask( job['taskId'], deep=False )

            seEl = SElement( self.configs["storageName"], \
                             self.configs["Protocol"],    \
                             self.configs["storagePort"] )
            loc = SElement("localhost", "local")

            ### copy ISB ###
            sbi = SBinterface( loc, seEl )

            # remote source name
            outputDirectory = task['outputDirectory']
            out = self.ft.match( outputDirectory )
            if out is not None :
                outputDirectory = outputDirectory[out.end()-1:]
            dest = os.path.join(outputDirectory, 'loggingInfo_'+str(job['jobId'])+'.log' )

            try:
                logging.info( 'Job %s : REBOUNCE DBG %s, %s' % \
                              (self.fullId(job), source, dest) )
                sbi.copy( source, dest, task['user_proxy'])
                #filesToClean.append(source)
            except Exception, e:
                logging.error(
                    "Job %s : loggingInfo.log rebounce transfer fail: %s " \
                             % ( self.fullId(job), str(e) ) )

            logging.info("Job %s : loggingInfo.log rebounce completed" \
                         % self.fullId(job) )
        else:
            logging.info("Job %s : Missing [%s] file" \
                         % ( self.fullId(job), source))
        return


    def rebounceOSB( self, job ):
        """
        __rebounceOSB__

        """
         
        logging.info("Job %s : Output rebounce" % self.fullId(job) )

        localOutDir = job.runningJob['outputDirectory']
        localOutputTgz = [ localOutDir +'/'+ f.split('/')[-1]
                           for f in job['outputFiles'] if '.tgz' in f ]
        localOutputTgz = [ f for f in localOutputTgz if os.path.exists(f) ]

        logging.info( 'Job %s : REBOUNCE DBG %s, %s, %s' \
                      % (self.fullId(job), localOutDir, localOutputTgz, \
                         [ localOutDir +'/'+ f.split('/')[-1]
                           for f in job['outputFiles'] ] ) )

        if len(localOutputTgz)==0:
            return   

        task = self.bossLiteSession.loadTask( job['taskId'], deep=False )
        seEl = SElement( self.configs["storageName"], \
                         self.configs["Protocol"],    \
                         self.configs["storagePort"] )
        loc = SElement("localhost", "local")

        ## copy OSB ##
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
                logging.info("Job %s : Output rebounce transfer fail: %s " \
                             % ( self.fullId(job), str(e) ) )
                continue 

        logging.info("Job %s : Output rebounce completed" % self.fullId(job) )
        for filetoclean in filesToClean:
            try: 
                os.remove( filetoclean )   

            except Exception, e:
                logging.info(
                    "Job %s : Output rebounce local clean fail: %s " \
                    % ( self.fullId(job), str(e) ) )
                continue
        logging.info("Job %s : Output rebounce cleaned" % self.fullId(job) )

        return 
    ######################


