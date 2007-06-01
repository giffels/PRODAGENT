#!/usr/bin/env python
"""
_T0LSFMonitor_

ResourceMonitor plugin for the T0 LSF submission system


"""

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from CondorTracker.Trackers.T0LSFTracker import LSFInterface

#  //
# // Map of LSF group names to poll for, since this may 
#//  grow to allow configuration of different jobs
#  //The map is the LSF Group : threshold name in the RC DB
# // This could be used in the future to throttle individual 
#//  job types based on LSF Groups.
_LSFGroupNames = {
    "/groups/tier0/reconstruction" : "processingThreshold",
    }

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
        
        #  //
        # // Now poll LSF for each group in the LSFGroupNames map above
        #//  and find the matching threshold in the DB
        for groupName, groupThreshold in _LSFGroupNames.items():
            numJobs = self.queryForLSFGroup(groupName)
            threshValue = siteThresholds.get(groupThreshold, 0)
            test = numJobs - threshValue

            minSubmit = siteThresholds.get("minimumSubmission", 1)

            if test < 0:
                #  //
                # // We are below the threshold
                #//  Now check that we are over the submission minimum
                if abs(test) < minSubmit:
                    #  //
                    # // below threshold, but not enough for a bulk
                    #//  submission 
                    continue
                
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                #constraint['type'] = "Processing"
                constraint['site'] = self.allSites[siteName]['SiteIndex']
                print str(constraint)
                result.append(constraint)
        return result

    
                
    def queryForLSFGroup(self, groupName):
        """
        _queryForLSFGroup_

        Poll LSF for the group name provided and return the count of matching
        jobs

        """
        #  //
        # // Call out to bjobs and count entries in group
        #//
        #jobInfo = LSFInterface.bjobs(groupName)
        #numJobs = len(jobInfo.keys())
        #return numJobs
        
        #  //
        # // quick test
        #//
        import random
        return random.randint(0, 100)
        
        

        
registerMonitor(T0LSFMonitor, T0LSFMonitor.__name__)
