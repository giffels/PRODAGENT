#!/usr/bin/env python
"""
_JobHandling_

"""


__revision__ = "$Id: JobHandling.py,v 1.14 2008/12/05 11:21:13 spiga Exp $"
__version__ = "$Revision: 1.14 $"

import os
import logging
import re
from shutil import copy
import threading

# PA configuration
#from ProdAgentCore.ProdAgentException import ProdAgentException

# Blite API import
#from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
#from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError

# Framework Job Report handling
from ProdCommon.FwkJobRep.ReportState import checkSuccess
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
        self.componentDir = params['componentDir']
        self.dropBoxPath = params['CacheDir']
        self.outputLocation = params['OutputLocation']
        self.bossLiteSession = params['bossLiteSession']
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
        success = False

        # get outdir and report file name
        outdir = job.runningJob['outputDirectory']

        # if SE rebounce fwjr
        if self.outputLocation == "SE" :
            try :
                self.reportRebounce( job )
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
                self.rebounceOSB( job )
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
            success = self.parseFinalReport(reportfilename, job)

            logging.debug("Job %s check Job Success: %s" % \
                          (self.fullId(job), str(success)) )

        # FwkJobReport not there: create one based on db or assume failed
        else:
            success = False
            job.runningJob["applicationReturnCode"] = str(50117)
            job.runningJob["wrapperReturnCode"] = str(50117)
            exitCode = 50117

            # write fake fwjr
            logging.debug("Job %s : write fake fwjr: %s" % \
                          (self.fullId(job), str(success)) )
            self.writeFwkJobReport( jobSpecId, exitCode, reportfilename )


        # in both cases: is the job successful?
        #if success:

            # yes, generate a job successful message and change status
            #self.publishJobSuccess(job, reportfilename)
            #self.notifyJobState(jobSpecId)

        #else:

            # no, generate a job failure message
            #self.publishJobFailed(job, reportfilename)

        return (job, success, reportfilename)


    def performErrorProcessing(self, job):
        """
        __performErrorProcessing__
        """

        logging.info( "Evaluate job %s and publish the job results" \
                      % self.fullId(job) )

        # get job information
        jobSpecId = job['name']
        reportfilename = os.path.join(job.runningJob['outputDirectory'], \
                                      'FrameworkJobReport.xml')

        # job status
        exitCode = -1

        # if SE rebounce fwjr
        if self.outputLocation in ["SE", "SEcopy"] :
            try :
                self.rebounceLoggingInfo( job )
            except :
                # as dirt as needed: any unknown error
                import traceback
                msg = traceback.format_exc()
                output = str(msg)
                logging.error("Job %s FAILED REBOUNCE : %s" % \
                              (self.fullId(job), output ) )
                return

        # write fake fwjr
        logging.debug("Job %s : write fake fwjr %s" % \
                      (self.fullId(job), reportfilename) )
        self.writeFwkJobReport( jobSpecId, exitCode, reportfilename )

        # generate a job failure message
        # self.publishJobFailed(job, reportfilename)

        return (job, False, reportfilename)



    def writeFwkJobReport( self, jobSpecId, exitCode, reportfilename ):
        """
        __writeFwkJobReport__

        write a fake reportfilename based on the status reported
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

        # skip empty files
        if os.path.getsize(reportfilename) == 0 :
            return False

        # check for success (needed for chain jobs
        success = checkSuccess(reportfilename)

        # read standard info
        # FIXME: avoid reading twice the same file!
        try :
            reports = readJobReport(reportfilename)
        except Exception, err:
            logging.error('Invalid Framework Job Report : %s' %str(err) )
            return False

        # if more than one fwjr (chain jobs) is enough!
        if len(reports) != 1 :
            return success

        # read CS specific info
        try :
            jobReport = reports[0]
            success = ( jobReport.status == "Success" )
            exitCode = jobReport.exitCode
            job.runningJob["wrapperReturnCode"] = exitCode
            cmsEx = None
            retCode = None
            for report in jobReport.errors:
                if report['Type'] == 'WrapperExitCode':
                    job.runningJob["wrapperReturnCode"] = report['ExitStatus']
                elif report['Type'] == 'ExeExitCode':
                    retCode = report['ExitStatus']
                elif report['Type'] == 'CMSException':
                    cmsEx = error['ExitStatus']
                else:
                    continue

            if retCode is not None:
                job.runningJob["applicationReturnCode"] = retCode
            elif cmsEx is not None:
                job.runningJob["applicationReturnCode"] = cmsEx
            else :
                job.runningJob["applicationReturnCode"] = exitCode
        except:
            pass

        return success




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
            if err.errno != 17:
                # not existing dir
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
                if  err.errno != 17:
                    # not existing dir
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
        credential = task['user_proxy'] 
        if self.configs["Protocol"].upper() == 'RFIO':
            username = task['name'].split('_')[0]
            credential = '%s::%s' % (username, credential)

        # transfer fwjr
        try:
            logging.debug( 'Job %s REBOUNCE DBG : %s, %s' % \
                           (self.fullId(job), source, dest) )
            sbi.copy( source, dest, credential )
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
            dest = os.path.join(
                outputDirectory, 'loggingInfo_' + str(job['jobId']) + '.log' )

            credential = task['user_proxy'] 
            if self.configs["Protocol"].upper() == 'RFIO':
                username = task['name'].split('_')[0]
                credential = '%s::%s' % (username, credential)

            try:
                logging.info( 'Job %s : REBOUNCE DBG %s, %s' % \
                              (self.fullId(job), source, dest) )
                sbi.copy( source, dest, credential)
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


    def buildOutdir( self, job, task ) :
        """
        __buildOutdir__

        compose outdir name and make the directory
        """

        # try with boss db
        if job.runningJob['outputDirectory'] is not None :
            outdir = job.runningJob['outputDirectory']

        # try to compose the path from task
        else :
            # SE?
            if self.outputLocation in ["SE", "SEcopy"] :
                outdir = self.dropBoxPath + '/' + task['name'] + '_spec'

            # fallback to task directory
            elif task['outputDirectory'] is not None \
                   and task['outputDirectory'] != '' :
                outdir = task['outputDirectory']

            # fallback to the component directory
            else :
                outdir = self.componentDir


            # FIXME: get outdir
            outdir = "%s/BossJob_%s_%s/Submission_%s/" % \
                 (outdir, job['taskId'], job['jobId'], job['submissionNumber'])

        # make outdir
        logging.info("%s: Creating directory %s" % \
                     (self.fullId( job ), outdir))
        try:
            os.makedirs( outdir )
        except OSError, err:
            # unless for existing dir
            if err.errno != 17:
                logging.error("%s: Cannot create directory %s : %s" % \
                     (self.fullId( job ), outdir, str(err)))
                raise

        # return outdir
        return outdir


    def fullId( self, job ):
        """
        __fullId__

        compose job primary keys in a string
        """

        return '[' + threading.currentThread().getName() + \
               '] Job ' + str( job['taskId'] ) + '.' \
               + str( job['jobId'] ) + '.' \
               + str( job['submissionNumber'] )






