#!/usr/bin/env python
"""
_JobStatistics_

Object that can be created from a JobReport and inserted into the DB
tables for the stat information


"""


import logging

from FwkJobRep.ReportParser import readJobReport
from StatTracker.StatTrackerDB import insertStats


class JobStatistics(dict):
    """
    _JobStatistics_

    Dictionary based container for Job statistics.

    Base class that is specialised for a SuccessfulJob and a FailedJob.
    Defines fields in common between the two

    """
    def __init__(self):
        self.setdefault("workflow_spec_id", None)
        self.setdefault("job_spec_id", None)
        self.setdefault("exit_code", None)
        self.setdefault("task_name", None)
        self.setdefault("status", None)
        self.setdefault("site_name", None)
        self.setdefault("host_name", None)
        self.setdefault("se_name", None)
        self.setdefault("job_type", None)
        self.setdefault("timing", {})

    def insertIntoDB(self):
        """
        _insertIntoDB_

        Insert self into database based on class name.

        """
        msg = "Inserting Into DB: %s" % self.__class__.__name__
        logging.debug(msg)
        try:
            insertStats(self)
        except StandardError, ex:
            msg = "Unable to insert JobStatistics into DB:\n"
            msg += str(ex)
            logging.error(msg)
        return
        

    def populateCommon(self, jobRepInstance):
        """
        _populateCommon_

        Given a report, populate self with the information common to both
        successes and failures

        """
        self['job_spec_id'] = jobRepInstance.jobSpecId
        self['task_name'] = jobRepInstance.name
        self['workflow_spec_id'] = jobRepInstance.workflowSpecId
        self['status'] = jobRepInstance.status
        self['exit_code'] =  jobRepInstance.exitCode        
        self['job_type'] = jobRepInstance.jobType
        
        siteName = jobRepInstance.siteDetails.get("SiteName", "Unknown")
        hostName = jobRepInstance.siteDetails.get("HostName", "Unknown")
        seName = jobRepInstance.siteDetails.get("se-name", "Unknown")
        self["site_name"] = siteName
        self["host_name"] = hostName
        self["se_name"] = seName

        
        return

    def recordTiming(self, jobRepInstance):
        """
        _recordTiming_

        Get the timing information from the job report

        """
        timing = jobRepInstance.timing
        for key, value in timing.items():
            self['timing'][key] = value
        return
    
    def __str__(self):
        """string print of this object"""
        result = "JobStatistics\n"
        result += " type=%s\n" % self.__class__.__name__
        for key, value in self.items():
            result += " %s=%s\n" % (key, value)
        return result

class SuccessfulJob(JobStatistics):
    """
    _SuccessfulJob_

    Specialisation of JobStatistics for a successful job
    
    """
    def __init__(self):
        JobStatistics.__init__(self)
        self.setdefault("output_files", [])
        self.setdefault("output_datasets", [])
        self.setdefault("input_files", [])
        self.setdefault("events_read", None)
        self.setdefault("events_written", None)
        self.setdefault("run_numbers", [])


    def recordInputs(self, jobRepInstance):
        """
        _recordInputs_

        Extract the input file information from the job report instance

        """
        totalRead = 0
        for infile in jobRepInstance.inputFiles:
            totalRead += int(infile['EventsRead'])
            fileName = infile['LFN']
            if str(fileName).strip() in ("None", ""):
                fileName = infile['PFN']

            self["input_files"].append(fileName)
        self["events_read"] = totalRead
        return

    def recordOutputs(self, jobRepInstance):
        """
        _recordOutputs_

        iterate through the output files and extract information into
        this instance

        """
        totalWritten = 0
        datasets = []
        runs = []
        for ofile in jobRepInstance.files:
            totalWritten += int(ofile['TotalEvents'])
            
            fileName = ofile['LFN']
            if str(fileName).strip() in ("None", ""):
                fileName = infile['PFN']
            self['output_files'].append(fileName)

            for run in ofile.runs:
                if run not in runs:
                    runs.append(run)

            for dataset in ofile.dataset:
                dsName = "/%s/%s/%s" % (dataset['PrimaryDataset'],
                                        dataset['DataTier'],
                                        dataset['ProcessedDataset'])
                if dsName not in datasets:
                    datasets.append(dsName)

        self['run_numbers'] = runs
        self['output_datasets'] = datasets
        self['events_written'] = totalWritten
        return



        

class FailedJob(JobStatistics):
    """
    _FailedJob_

    Specialisation of JobStatistics for a failed job

    """
    def __init__(self):
        JobStatistics.__init__(self)
        self.setdefault("error_type", None)
        self.setdefault("error_code", 1)
        self.setdefault("error_desc", None)
        


def jobReportToJobStats(jobRepInstance):
    """
    _jobReportToJobStats_

    Convert a job report instance to a JobStatistics instance, based on the
    content of the job report.

    Returns a SuccessfulJob or FailedJob instance depending on wether report
    was a success or a failure.

    """
    if jobRepInstance.wasSuccess():
        return jobReportToSuccess(jobRepInstance)
    else:
        return jobReportToFailure(jobRepInstance)


def jobReportToSuccess(jobRepInstance):
    """
    _jobReportToSuccess_

    """
    result = SuccessfulJob()
    result.populateCommon(jobRepInstance)
    result.recordInputs(jobRepInstance)
    result.recordOutputs(jobRepInstance)
    result.recordTiming(jobRepInstance)
    return result
    

def jobReportToFailure(jobRepInstance):
    """
    _jobReportToFailure_

    Create a FailedJob instance from the job report instance provided

    """
    result = FailedJob()
    result.populateCommon(jobRepInstance)
    result.recordTiming(jobRepInstance)
    if len(jobRepInstance.errors) > 0:
        lastError = jobRepInstance.errors[-1]
                                          
        result['error_type'] = lastError['Type']
        result['error_code'] = lastError['ExitStatus']
        result['error_desc'] = lastError['Description']
        
        
    return result
    
    
