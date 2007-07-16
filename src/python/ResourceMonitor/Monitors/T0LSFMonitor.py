#!/usr/bin/env python
"""
_T0LSFMonitor_

ResourceMonitor plugin for the T0 LSF submission system


"""

import logging

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ProdAgent.Resources.LSF import LSFInterface


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

        activeJobs = 0
        for jobID in jobList.keys():
            if ( jobList[jobID] == 'PEND' or jobList[jobID] == 'RUN' ):
                activeJobs += 1

        logging.info("Number of active jobs is %d" % activeJobs)

        siteData = self.allSites[siteName]
        siteIndex = siteData['SiteIndex']
        siteThresholds = self.siteThresholds[siteName]
        #siteAttrs = self.siteAttributes[siteName]

        processingThreshold = siteThresholds.get("processingThreshold")
        missingJobs = processingThreshold - activeJobs

        minSubmit = siteThresholds.get("minimumSubmission")
        maxSubmit = siteThresholds.get("maximumSubmission")

        logging.debug("processingThreshold = %d , minSubmit = %d , maxSubmit = %d"
                      % (processingThreshold,minSubmit,maxSubmit))

        #  //
        # // Check if we are below the threshold
        #//    and over the submission minimum
        if ( missingJobs >= minSubmit ):
            constraint = self.newConstraint()
            if ( missingJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingJobs
            #constraint['type'] = "Processing"
            constraint['site'] = self.allSites[siteName]['SiteIndex']
            logging.info("Releasing %d jobs" % constraint['count'])
            result.append(constraint)

        return result

        
registerMonitor(T0LSFMonitor, T0LSFMonitor.__name__)
