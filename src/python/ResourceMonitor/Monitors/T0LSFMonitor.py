#!/usr/bin/env python
"""
_T0LSFMonitor_

ResourceMonitor plugin for the T0 LSF submission system


"""

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
            # // Cant do much if we can find a CERN entry...
            #//
            msg = "ERROR: No Resource Control Entry for site: %s" % siteName
            msg += "This is a pretty big problem for the Tier 0..."
            raise RuntimeError, msg
        siteData = self.allSites[siteName]
        siteIndex = siteData['SiteIndex']
        siteThresholds = self.siteThresholds[siteName]
        siteAttrs = self.siteAttributes[siteName]
        
        numJobs = 0
        jobList = LSFInterface.bjobs()
        for jobID in jobList.keys():
            if ( jobList[jobID] == 'PEND' or jobList[jobID] == 'RUN' ):
                numJobs += 1

        procThresh = siteThresholds.get("processingThreshold", 0)
        test = numJobs - procThresh

        minSubmit = siteThresholds.get("minimumSubmission", 1)

        #  //
        # // Check if we are below the threshold
        #//    and over the submission minimum
        if ( test < 0 and abs(test) >= minSubmit ):
            constraint = self.newConstraint()
            constraint['count'] = abs(test)
            #constraint['type'] = "Processing"
            constraint['site'] = self.allSites[siteName]['SiteIndex']
            print str(constraint)
            result.append(constraint)

        return result

        
registerMonitor(T0LSFMonitor, T0LSFMonitor.__name__)
