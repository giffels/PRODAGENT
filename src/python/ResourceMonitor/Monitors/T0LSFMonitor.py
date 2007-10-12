#!/usr/bin/env python
"""
_T0LSFMonitor_

ResourceMonitor plugin for the T0 LSF submission system


"""

import logging

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ProdAgent.Resources.LSF import LSFInterface

from ProdAgent.WorkflowEntities import Job


class T0LSFMonitor(MonitorInterface):
    """
    _T0LSFMonitor_

    Monitor to watch the LSF Queues at CERN for T0 jobs

    Note that the RM supports multiple sites in the ResourceControlDB,
    In this case we are really only using one, which is CERN.

    """
    
    def __call__(self):

        #
        # check that at least on threshold is set
        #
        if (len(self.allSites) == 0):
            msg = "ERROR: No Resource Control Entry"
            raise RuntimeError, msg

        try:
            jobList = LSFInterface.bjobs()
        except Exception, ex:
            # can only happen if bjobs call failed
            # do nothing in this case, next loop will work
            logging.debug("Call to bjobs failed, just wait for next loop")
            return result

        #
        # two different ways to operate
        #
        # 1. threshold for defaultSiteName CERN is set
        #
        #   old-style method, only one set of
        #   processing and merge threshold 
        #
        # 2. threshold for defaultSiteName CERN is not set
        #
        #   system assumes every threshold is for
        #   a different workflow, with site == workflow
        #
        defaultSiteName = 'CERN'

        #
        # keep track of total number of jobs as list, first
        # element is processing jobs count, second element
        # is merge job count
        #
        jobCountOverall = [0,0]

        #
        # the jobCount dictionary contains workflows as keys and
        # the corresponding values are a list, with the first
        # element the processing job count and the second
        # the merge job count
        #
        jobCountByWorkflow = {}
        if not self.allSites.has_key(defaultSiteName):
            for workflowId in self.allSites.iterkeys():
                jobCountByWorkflow[workflowId] = [0,0]

        #
        # loop over output of bjobs
        #
        for jobId in jobList.keys():
            if ( jobList[jobId] == 'PEND' or jobList[jobId] == 'RUN' ):

                # database query for job information from the workflow entities table
                # NOTE: might not work for ProdMgr jobs (not used for Tier0, so no problem)
                jobInfo = Job.get(jobId)

                jobType = jobInfo['job_type']
                workflowId = jobInfo['workflow_id']

                logging.debug("Found job %s from workflow %s" % (jobId,workflowId))

                if ( jobType == 'Processing' ):
                    if self.allSites.has_key(defaultSiteName):
                        jobCountOverall[0] += 1
                    elif jobCountByWorkflow.has_key(workflowId):
                        jobCountByWorkflow[workflowId][0] += 1
                elif ( jobType == 'Merge' ):
                    if self.allSites.has_key(defaultSiteName):
                        jobCountOverall[1] += 1
                    elif jobCountByWorkflow.has_key(workflowId):
                        jobCountByWorkflow[workflowId][1] += 1

        if self.allSites.has_key(defaultSiteName):
            logging.info("Number of processing jobs is %d" % jobCountOverall[0])
            logging.info("Number of merge jobs is %d" % jobCountOverall[1])
        else:
            for workflowId in self.allSites.iterkeys():
                logging.info("Number of processing jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][0]))
                logging.info("Number of merge jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][1]))

        result = []

        if self.allSites.has_key(defaultSiteName):
            self.releaseJobs(defaultSiteName,jobCountOverall,result,False)
        else:
            for workflowId in jobCountByWorkflow.iterkeys():
                self.releaseJobs(workflowId,jobCountByWorkflow[workflowId],result,True)

        return result


    def releaseJobs(self,siteName,jobCount,result,constrainWorkflow):

        siteData = self.allSites[siteName]
        siteIndex = siteData['SiteIndex']
        siteThresholds = self.siteThresholds[siteName]
        #siteAttrs = self.siteAttributes[siteName]

        processingThreshold = siteThresholds.get("processingThreshold")
        mergeThreshold = siteThresholds.get("mergeThreshold")
        
        missingProcessingJobs = processingThreshold - jobCount[0]
        missingMergeJobs = mergeThreshold - jobCount[1]

        minSubmit = siteThresholds.get("minimumSubmission")
        maxSubmit = siteThresholds.get("maximumSubmission")

        # check if we should release processing jobs
        if ( missingProcessingJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingProcessingJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingProcessingJobs

            # some more constraints
            constraint['type'] = "Processing"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # contraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d Processing jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d Processing jobs" % constraint['count'])

            result.append(constraint)

        # check if we should release merge jobs 
        if ( missingMergeJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingMergeJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingMergeJobs

            # some more constraints
            constraint['type'] = "Merge"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # contraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d Merge jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d Merge jobs" % constraint['count'])

            result.append(constraint)

        return

        
registerMonitor(T0LSFMonitor, T0LSFMonitor.__name__)
