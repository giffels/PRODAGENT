#!/usr/bin/env python

import logging 
import os
import urllib

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler
from ErrorHandler.Registry import retrieveHandler
from FwkJobRep.ReportParser import readJobReport
from JobState.JobStateAPI import JobStateInfoAPI



class RunFailureHandler(HandlerInterface):
    """
    _RunFailureHandler_

    Handles job run failures. Called by the error handler if a job run failure 
    event is received. We distinguish two classes of failures. A failure 
    that happens during a submission (job could not run), or a failure 
    during running of the job.  the payload for RunFailure is a url to 
    the job report.

    Based on the job report, we can retrieve the job id and use
    that to retrieve the job type in the database. Using this information.
    we propagate it to different job error handlers associated to different 
    job types.

    """

    def __init__(self):
         HandlerInterface.__init__(self)
         self.args={}

    def setJobReportLocation(self,jobReportLocation):
         self.args['jobReportLocation']=jobReportLocation

    def handleError(self,payload):
         """
         The payload of a job failure is a url to the job report
         """
         jobReportUrl= payload

         # prepare to retrieve the job report file.
         # NOTE: we assume that the report file has a relative unique name
         # NOTE: if that is not the case we need to add a unique identifier to it.
         slash = jobReportUrl.rfind('/')
         fileName = jobReportUrl[slash+1:]
         urllib.urlretrieve(jobReportUrl, \
              self.args['jobReportLocation']+'/'+fileName)
         logging.debug(">RunFailureHandler<: Retrieving job report from %s " % jobReportUrl)

         jobReport=readJobReport(self.args['jobReportLocation']+'/'+fileName)
         #NOTE: is this the right way to extract the job id.
         jobId=jobReport[0].jobSpecId
         logging.debug(">RunFailureHandler<:Retrieving jobId from job report "+\
                       "(used to dynamically load error handler) " \
                       "jobId="+str(jobId))

         # create the jobReportLocation jobId hierarchy if not exists.
         pipe=os.popen("mkdir -p "+self.args['jobReportLocation']+'/'+jobId)
         pipe.close()
         # move the report file to this new location.
         pipe=os.popen("mv "+self.args['jobReportLocation']+'/'+fileName+" "+ \
                       self.args['jobReportLocation']+'/'+jobId)
         logging.debug(">RunFailureHandler<:Moving job report to permanent storage: " \
                       +self.args['jobReportLocation']+'/'+jobId)
         pipe.close()

         # we retrieve the job type, and depending on the type we
         # pick an error handler (her we propagate the rest of the
         # the error handling.

         jobType=JobStateInfoAPI.general(jobId)['JobType']
         handlerType=jobType+"RunFailureHandler"
         try:
              handler=retrieveHandler(handlerType)
         except:
              raise Exception("ERROR","Could not find handler "+handlerType)

         #set the publication function
         handler.publishEvent=self.publishEvent
         # pass on the new jobreport location:
         jobReportLocation=self.args['jobReportLocation']+'/'+ \
                           jobId+'/'+fileName
         logging.debug(">RunFailureHandler<:Propagating error to new error handler for further "+\
                       " processing")
         handler.handleError(jobReportLocation)

registerHandler(RunFailureHandler(),"runFailureHandler")







