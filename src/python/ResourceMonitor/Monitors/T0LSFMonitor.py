#!/usr/bin/env python
"""
_T0LSFMonitor_

ResourceMonitor plugin for the T0 LSF submission system


"""

import logging

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ProdAgent.Resources.LSF import LSFInterface

from ProdAgent.WorkflowEntities import JobState


class T0LSFMonitor(MonitorInterface):
    """
    _T0LSFMonitor_

    Monitor to watch the LSF Queues at CERN for T0 jobs

    Note that the RM supports multiple sites in the ResourceControlDB,
    In this case we are really only using one, which is CERN.

    """
    
    def __call__(self):

        result = []

        siteName = "CERN"
        if self.allSites.get(siteName, None) == None:
            #  //
            # // Cant do much if we can't find a CERN entry...
            #//
            msg = "ERROR: No Resource Control Entry for site: %s" % siteName
            msg += "This is a pretty big problem for the Tier 0..."
            raise RuntimeError, msg
        
        try:
            jobList = LSFInterface.bjobs()
        except Exception, ex:
            # can only happen if bjobs call failed
            # do nothing in this case, next loop will work
            logging.debug("Call to bjobs failed, do nothing")
            return result

        mergeJobs = 0
        processingJobs = 0
        for jobID in jobList.keys():
            if ( jobList[jobID] == 'PEND' or jobList[jobID] == 'RUN' ):
                # database query for job information from the workflow entities table
                # NOTE: might not work for ProdMgr jobs (not used for Tier0, so no problem)
                stateInfo = JobState.general(jobID)
                if ( stateInfo['JobType'] == 'Processing' ):
                    processingJobs += 1
                elif ( stateInfo['JobType'] == 'Merge' ):
                    mergeJobs += 1

        logging.info("Number of processing jobs is %d" % processingJobs)
        logging.info("Number of merge jobs is %d" % mergeJobs)

        siteData = self.allSites[siteName]
        siteIndex = siteData['SiteIndex']
        siteThresholds = self.siteThresholds[siteName]
        #siteAttrs = self.siteAttributes[siteName]

        processingThreshold = siteThresholds.get("processingThreshold")
        mergeThreshold = siteThresholds.get("mergeThreshold")

        missingProcessingJobs = processingThreshold - processingJobs
        missingMergeJobs = mergeThreshold - mergeJobs

        minSubmit = siteThresholds.get("minimumSubmission")
        maxSubmit = siteThresholds.get("maximumSubmission")

        logging.debug("processingThreshold = %d , mergeThreshold = %d , minSubmit = %d , maxSubmit = %d"
                      % (processingThreshold,mergeThreshold,minSubmit,maxSubmit))

        # check if we should release processing jobs
        if ( missingProcessingJobs >= minSubmit ):
            constraint = self.newConstraint()
            if ( missingProcessingJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingProcessingJobs
            constraint['type'] = "Processing"
            constraint['site'] = self.allSites[siteName]['SiteIndex']
            logging.info("Releasing %d processing jobs" % constraint['count'])
            result.append(constraint)

        # check if we should release merge jobs 
        if ( missingMergeJobs >= minSubmit ):
            constraint = self.newConstraint()
            if ( missingMergeJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingMergeJobs
            constraint['type'] = "Merge"
            constraint['site'] = self.allSites[siteName]['SiteIndex']
            logging.info("Releasing %d merge jobs" % constraint['count'])
            result.append(constraint)

        return result

        
registerMonitor(T0LSFMonitor, T0LSFMonitor.__name__)
