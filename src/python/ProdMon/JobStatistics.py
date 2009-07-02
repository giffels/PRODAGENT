# !/usr/bin/env python
"""
_JobStatistics_

Object that can be created from a JobReport and inserted into the DB
tables for the stat information


"""


from ProdMon.ProdMonDB import insertStats, getMergeInputFiles
from ProdAgent.WorkflowEntities import Job
from ShREEK.CMSPlugins.DashboardInfo import extractDashboardID
from JobQueue.JobQueueAPI import getSiteForReleasedJob

import os


class JobStatistics(dict):
    """
    _JobStatistics_

    Dictionary based container for Job statistics.

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("workflow_spec_id", None)
        self.setdefault("job_spec_id", None)
        self.setdefault("exit_code", -1)
        self.setdefault("task_name", None)
        self.setdefault("status", None)
        self.setdefault("site_name", None)
        self.setdefault("host_name", None)
        self.setdefault("ce_name", None)
        self.setdefault("se_name", None)
        self.setdefault("job_type", None)
        self.setdefault("timing", {})
        self["timing"]["AppStartTime"] = None
        self["timing"]["AppEndTime"] = None
        self.setdefault("scheduler_id", None)
        self.setdefault("dashboard_id", None)
        self.setdefault("input_datasets", [])
        self.setdefault("input_files", [])
        self.setdefault("request_id", None)
        self.setdefault("rc_site_index", None)
        
        # for successful jobs
        self.setdefault("events_read", 0)
        self.setdefault("events_written", 0)
        self.setdefault("run_numbers", [])
        self.setdefault("output_datasets", None)
        self.setdefault("output_files", [])
        self.setdefault("skipped_events", [])
        
        # for failed jobs
        self.setdefault("error_type", None)
        self.setdefault("error_code", None)
        self.setdefault("error_desc", None)

        #performance records
        self.setdefault("performance_report", None)
        
        # holds database_ids - used during insertions
        self.setdefault("database_ids", {})


    def insertIntoDB(self, *others):
        """
        _insertIntoDB_

        Insert self constituent information into database
        
        optionally take reports for other steps in the job instance

        """
        try:
            insertStats(*((self, ) + others))
        except Exception, ex:
            msg = "Unable to insert JobStatistics into DB:\n"
            msg += str(ex)
            # logging.error(msg)
            raise RuntimeError, msg
        return
        

    def populateCommon(self, jobRepInstance):
        """

        Given a report, populate self with the information common to both
        successes and failures

        """
        self['job_spec_id'] = jobRepInstance.jobSpecId
        self['task_name'] = jobRepInstance.name
        self['workflow_spec_id'] = jobRepInstance.workflowSpecId
        self['status'] = jobRepInstance.status
        self['exit_code'] =  jobRepInstance.exitCode        
        self['job_type'] = jobRepInstance.jobType
        
        self['site_name'] = jobRepInstance.siteDetails.get("SiteName","Unknown")
        self['host_name'] = jobRepInstance.siteDetails.get("HostName","Unknown")
        self['se_name'] = jobRepInstance.siteDetails.get("se-name","Unknown")
        self['ce_name'] = jobRepInstance.siteDetails.get("ce-name","Unknown")
        
        self['dashboard_id'] = jobRepInstance.dashboardId
        
        self['performance_report'] = jobRepInstance.performance
        
        # Fill in values not always in fjr
        if self["workflow_spec_id"] == None:
            self.__recordWorkflow(jobRepInstance)
        if self["job_type"] == None:
            self.__recordJobType(jobRepInstance)
        if self["dashboard_id"] == None:
            self.__recordDashboardId(jobRepInstance)
        
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


    def recordInputs(self, jobRepInstance):
        """
        _recordInputs_

        Extract the input file information from the job report instance

        """        
        totalRead = 0
        
        # If successful read input files from job report
        if jobRepInstance.wasSuccess():
            for infile in jobRepInstance.inputFiles:
                totalRead += int(infile['EventsRead'])
                fileName = infile['LFN']
                if str(fileName).strip() in ("None", ""):
                    fileName = infile['PFN']
                self["input_files"].append(str(fileName).strip())
        
                # skipped events
                self['skipped_events'] = [(dic["Run"], dic["Event"]) for \
                                          dic in jobRepInstance.skippedEvents]
        
        #  for failed merge job read from merge sensor tables
        elif jobRepInstance.jobType == "Merge":
            self.__recordMergeInputs(jobRepInstance)
        
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
                fileName = ofile['PFN']
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


    def __recordWorkflow(self, jobRepInst):
        """
        Set workflowId from db if not in jobReport
        """
        #self["workflow_spec_id"] = getWorkflow(jobRepInst.jobSpecId)
        generalInfo = Job.get(jobRepInst.jobSpecId)
        self["workflow_spec_id"] = generalInfo['workflow_id']
        return
        
        
    def __recordJobType(self, jobRepInst):
        """
        Set job id from db if not in jobReport
        """
        generalInfo = Job.get(jobRepInst.jobSpecId)
        self["job_type"] = generalInfo['job_type']
        return


    def __recordMergeInputs(self, jobRepInst):
        """
        _recordMergeInputs_

        If this is a failed merge job, pull in the inputfiles
        from the merge sensor tables and record them

        """
        if jobRepInst.jobType != "Merge":
            return
        self['input_files'] = getMergeInputFiles(jobRepInst.jobSpecId)
        return
    

    def __recordDashboardId(self, jobRepInst):
        """
        Set dashbaord id from jobSpec
        """
        generalInfo = Job.get(jobRepInst.jobSpecId)
        cacheDir = generalInfo['cache_dir']
        jobSpecFile = os.path.join(cacheDir, "%s-JobSpec.xml" % jobRepInst.jobSpecId)
        self["dashboard_id"] = extractDashboardID(jobSpecFile)[1]
        return


    def recordSiteIndex(self, jobRepInst):
        """
        get jobQ site index
        """
        
        self['rc_site_index'] = getSiteForReleasedJob(jobRepInst.jobSpecId)
        return
        

def jobReportToJobStats(jobRepInstance):
    """
    _jobReportToJobStats_

    Convert a job report instance to a JobStatistics instance, based on the
    content of the job report.

    Appropriate fields are filled depending on whether the job
    was successful or not.

    """
    result = JobStatistics()
    result.populateCommon(jobRepInstance)
    result.recordInputs(jobRepInstance)
    result.recordOutputs(jobRepInstance)
    result.recordTiming(jobRepInstance)
    result.recordSiteIndex(jobRepInstance)
    
    # record failure conditions
    if not jobRepInstance.wasSuccess():
        if len(jobRepInstance.errors) > 0:
            lastError = jobRepInstance.errors[-1]
            result['error_type'] = lastError['Type']
            result['error_code'] = lastError['ExitStatus']
            result['error_desc'] = lastError['Description']
        
    return result


def jobStatsGroupedBySpecId(reports):
    """
    Take job reports and return them grouped by job instance 
     - identified by same jobSpecId but diff step name
     - set first exit code to first error
    """
    result = {}
    for report in reports:
        stats = jobReportToJobStats(report)
        result.setdefault(stats['job_spec_id'], []).append(stats)

        first_task = result[stats['job_spec_id']][0]
        if not first_task['exit_code'] and stats['exit_code']:
            first_task['exit_code'] = stats['exit_code']
            first_task['error_desc'] = stats['error_desc']
            first_task['error_code'] = stats['error_code']
            first_task['error_type'] = stats['error_type']

    return [x for x in result.values()]
    
    
def wasSuccess(*jobStats):
    for stat in jobStats:
        if stat['exit_code']:
            return False
    return True