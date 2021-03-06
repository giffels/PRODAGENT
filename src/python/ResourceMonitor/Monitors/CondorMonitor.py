#!/usr/bin/env python
"""
_CondorMonitor_

ResourceMonitor plugin that monitors a condor Q

"""
import logging
from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor


from ResourceMonitor.Monitors.CondorQ import CondorPAJobs,countJobs

# getJobs helper function
def extractGatekeeper(item):
    gatekeeper = item['GridResource'].replace(item['JobGridType'], '')
    gatekeeper = gatekeeper.strip()
    return gatekeeper

def getJobs(paJobs,jobType,*defaultGatekeepers):
    """
    _getJobs_

    Return a dictionary of gatekeeper: number of jobs for
    each gatekeeper found in the cached jobs for the job type.

    Default gatekeepers is a list of gatekeepers that you want to
    make sure appear in the output, since if there are no jobs, it
    wont be included. If there are no jobs at a default gatekeeper,
    then it will be returned as having 0 jobs
    """
    return countJobs(jobs=paJobs.copy(jobTypes=set([jobType])).jobs,
                     default_ids=defaultGatekeepers,
                     id_function=extractGatekeeper,
                     default_value='cmsosgce.fnal.gov/jobmanager-condor')


class CondorMonitor(MonitorInterface):
    """
    _CondorMonitor_

    Poll condor_q on the local machine and get details of all the ProdAgent
    jobs in there split by processing and merge type.

    Generate a per site constraint for each distinct site being used

    """
    
    def __call__(self):
        logging.debug("calling CondorMonitor...")
        result = []
        #  // 
        # // get list of all active gatekeepers from DB info
        #//
        activeGatekeepers = [
            self.allSites[x]['CEName'] for x in self.activeSites ]
        logging.debug(str(activeGatekeepers))

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
        paJobs=CondorPAJobs(jobTypes=set(["Processing","Merge","CleanUp"]),load_on_create=True)
        mergeInfo = getJobs(paJobs,'Merge',*activeGatekeepers)
        processingInfo = getJobs(paJobs,'Processing',*activeGatekeepers)
        cleanupInfo = getJobs(paJobs,'Cleanup',*activeGatekeepers)

        #  //
        # // Calculate available merge slots
        #//
        for gatekeeper, jobcounts in mergeInfo.items():
            idle = jobcounts["Idle"]
            msg="mergeInfo: %s"%str(mergeInfo)
            logging.debug(msg)
            msg="gatekeeper: %s"%gatekeeper
            logging.debug(msg)
            if ceToSite.get(gatekeeper):
              site = ceToSite[gatekeeper]
              mergeThresh = self.siteThresholds[site]["mergeThreshold"]
              test = idle - mergeThresh
              msg="CondorMonitor Merge: Site=%s, Idle=%s, Thresh=%s, Test=%s"%(site,idle,mergeThresh,test)
              logging.debug(msg)

              if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Merge"
                constraint['site'] = self.allSites[site]['SiteIndex']
                print str(constraint)
                result.append(constraint)

                        #  //
        # // Calculate available CleanUp slots
        # || Basically cloning the merge stuff here -- doesn't seem to make sense that this be a bulk thing
        # || Would expect in steady state for the jobs to just sort of dribble out behind successful merges
        #//
        for gatekeeper, jobcounts in cleanupInfo.items():
            idle = jobcounts["Idle"]
            msg="cleanupInfo: %s"%str(cleanupInfo)
            logging.debug(msg)
            msg="gatekeeper: %s"%gatekeeper
            logging.debug(msg)
            if ceToSite.get(gatekeeper):
              site = ceToSite[gatekeeper]
              cleanupThresh = self.siteThresholds[site]["cleanupThreshold"]
              test = idle - cleanupThresh
              msg="CondorMonitor CleanUp: Site=%s, Idle=%s, Thresh=%s, Test=%s"%(site,idle,cleanupThresh,test)
              logging.debug(msg)

              if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "CleanUp"
                constraint['site'] = self.allSites[site]['SiteIndex']
                print str(constraint)
                result.append(constraint)


        for gatekeeper, jobcounts in processingInfo.items():
            idle = jobcounts["Idle"]
            if ceToSite.get(gatekeeper):
              site = ceToSite[gatekeeper]
              procThresh = self.siteThresholds[site]["processingThreshold"]
              minSubmit = self.siteThresholds[site]["minimumSubmission"]
              maxSubmit = self.siteThresholds[site]["maximumSubmission"]
              test = idle - procThresh
              msg="CondorMonitor Proc: Site=%s, Idle=%s, Thresh=%s, Test=%s"%(site,idle,procThresh,test)
              logging.debug(msg)
            
              if test < 0:
                if abs(test) < minSubmit:
                    #  //
                    # // below threshold, but not enough for a bulk
                    #//  submission
                    continue
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                # enforce maximum number to submit
                if constraint['count'] > maxSubmit:
                   constraint['count'] = maxSubmit
                constraint['type'] = "Processing"
                constraint['site'] = self.allSites[site]['SiteIndex']
                print str(constraint)
                result.append(constraint)
                
                
        return result
            

    
registerMonitor(CondorMonitor, CondorMonitor.__name__)
