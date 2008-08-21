#!/usr/bin/env python
"""
_InsertReport_

Utilities to insert data from a Job Report
back into the MergeSensor DB

"""


import logging
import os
from MergeSensor.MergeSensorError import DuplicateLFNError
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from MergeSensor.HandleJobReport import HandleJobReport



class ReportHandler (list):
    """
    _ReportHandler_
    
    Handles the Framework job report for both processing and merge jobs. 
    Helper class to insert data from a physical job report file
    into the MergeSensor DB
    
    """
    def __init__(self, repFile, maxInputAccessFailures, enableMergeHandling = False):
        """
        _init_
        Initialization function
        """
	
	#// Base class constructor	
        list.__init__ (self)
	

        self.reportFile = repFile
        self.enableMergeHandling = enableMergeHandling
        self.maxInputAccessFailures = maxInputAccessFailures

    def __call__(self):
        """
        _operator()_
	
	Callable funtions to handle job report

        """     
		
        logging.info ('read job report.......') 
	 
	#  remove file:// from file name (if any)
        
	jobReport = self.reportFile.replace('file://','')

        # verify the file exists
        if not os.path.exists(jobReport):
            logging.error("Cannot process JobSuccess event: " \
                         + "job report %s does not exist." % jobReport)
            return None

        # read the report
        try:
            self.extend(readJobReport(jobReport))

        # check errors
        except Exception, msg:
            logging.error("Cannot process JobSuccess event for %s: %s" \
                           % (jobReport, msg))
            return None
	
	
	result = None
			
	try:
	
	
           for report in self:
               handler = HandleJobReport(report, jobReport, self.maxInputAccessFailures, self.enableMergeHandling)
	       result = handler()
               logging.info(handler.summarise())
	       
	
	except Exception, ex:
	   
           msg = "Failed to handle job report from processing job:\n"
           msg += "%s\n" % self.reportFile
           msg += str(ex)
           logging.error(msg)
	   return result
	
	return result   #// End __call__



