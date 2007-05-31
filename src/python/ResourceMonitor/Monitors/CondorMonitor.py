#!/usr/bin/env python
"""
_CondorMonitor_

ResourceMonitor plugin that monitors a condor Q

"""

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor


from ResourceMonitor.Monitors.CondorQ import processingJobs, mergeJobs


class CondorMonitor(MonitorInterface):
    """
    _CondorMonitor_

    Poll condor_q on the local machine and get details of all the ProdAgent
    jobs in there split by processing and merge type.

    Generate a per site constraint for each distinct site being used

    """
    
    def __call__(self):
        result = []
        #  // 
        # // get list of all active gatekeepers from DB info
        #//
        activeGatekeepers = [
            self.allSites[x]['CEName'] for x in self.activeSites ]

        #  //
        # // Reverse lookup table for ce -> site name
        #//
        ceToSite = {}
        [ ceToSite.__setitem__(
            self.allSites[x]['CEName'],
            self.allSites[x]['SiteName']) for x in self.activeSites ]  
        
        
        #  //
        # // get totals per active gatekeeper for merge and processing
        #//  jobs
        mergeInfo = mergeJobs(*activeGatekeepers)
        processingInfo = processingJobs(*activeGatekeepers)

        #  //
        # // Calculate available merge slots
        #//
        for gatekeeper, jobcounts in mergeInfo.items():
            idle = jobcounts["Idle"]
            site = ceToSite[gatekeeper]
            mergeThresh = self.siteThresholds[site]["mergeThreshold"]
            test = idle - mergeThresh


            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Merge"
                constraint['site'] = self.allSites[site]['SiteIndex']
                print str(constraint)
                result.append(constraint)


        for gatekeeper, jobcounts in processingInfo.items():
            idle = jobcounts["Idle"]
            site = ceToSite[gatekeeper]
            procThresh = self.siteThresholds[site]["processingThreshold"]
            minSubmit = self.siteThresholds[site]["minimumSubmission"]
            test = idle - procThresh
            
            
            if test < 0:
                if abs(test) < minSubmit:
                    #  //
                    # // below threshold, but not enough for a bulk
                    #//  submission
                    continue
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Processing"
                constraint['site'] = self.allSites[site]['SiteIndex']
                print str(constraint)
                result.append(constraint)
                
                
        return result
            

    
registerMonitor(CondorMonitor, CondorMonitor.__name__)
