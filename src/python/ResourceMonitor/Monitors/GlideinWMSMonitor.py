#!/usr/bin/env python
"""
_GlideinWMSMonitor_

ResourceMonitor plugin that monitors a glideinWMS via condor_q

"""
import logging
from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor


from ResourceMonitor.Monitors.CondorQ import CondorPAJobs,countJobs

# getJobs helper function
def extractSite(item):
    # if there is more than one, get just the first one
    site = item['DESIRED_Sites'].split(',')[0]
    return site

def getJobs(paJobs,jobType,*defaultSites):
    """
    _getJobs_

    Return a dictionary of sites: number of jobs for
    each site found in the cached jobs for the job type.

    Default sites is a list of sites that you want to
    make sure appear in the output, since if there are no jobs, it
    wont be included. If there are no jobs at a default site,
    then it will be returned as having 0 jobs
    """
    return countJobs(jobs=paJobs.copy(jobTypes=set([jobType])).jobs,
                     default_ids=defaultSites,
                     id_function=extractSite,
                     default_value='Any')


class GlideinWMSMonitor(MonitorInterface):
    """
    _GlideinWMSMonitor_

    Poll condor_q on the local machine and get details of all the ProdAgent
    jobs in there split by processing and merge type.

    Generate a per site constraint for each distinct site being used

    """
    
    def __call__(self):
        logging.debug("calling GlideinWMSMonitor...")
        result = []
        #  // 
        # // get list of all active sites from DB info
        #//
        activeSites = [
            self.allSites[x]['SiteName'] for x in self.activeSites ]
        logging.debug(str(activeSites))

        #  //
        # // get totals per active site for merge and processing
        #//  jobs
        paJobs=CondorPAJobs(jobTypes=set(["Processing","Merge","CleanUp","Skim","Harvesting"]),load_on_create=True)
        mergeInfo = getJobs(paJobs,'Merge',*activeSites)
        processingInfo = getJobs(paJobs,'Processing',*activeSites)
        cleanupInfo = getJobs(paJobs,'Cleanup',*activeSites)
        skimInfo = getJobs(paJobs,'Skim',*activeSites)
        harvestingInfo = getJobs(paJobs,'Harvesting',*activeSites)

        #  //
        # // Calculate available merge slots
        #//
        msg="mergeInfo: %s"%str(mergeInfo)
        logging.debug(msg)
        for site, jobcounts in mergeInfo.items():
            idle = jobcounts["Idle"]
            running = jobcounts["Running"]
            msg="site: %s"%site
            logging.debug(msg)
            if self.siteThresholds.has_key(site):

              try:
                mergeRunThrottle = self.siteThresholds[site]["mergeRunningThrottle"]
              except:
                mergeRunThrottle = 1000000000

              mergeThresh = self.siteThresholds[site]["mergeThreshold"]
              test = idle - mergeThresh
              msg="GlideinWMSMonitor Merge: Site=%s, Idle=%s, Running=%s, Thresh=%s, Test=%s"%(site,idle,running,mergeThresh,test)
              logging.debug(msg)

              if running>mergeRunThrottle:
                 msg="GlideinWMSMonitor: Running merge jobs (%s) exceed throttle level (%s), not sending in more"%(running,mergeRunThrottle)
                 logging.debug(msg)
                 test=0


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
        msg="cleanupInfo: %s"%str(cleanupInfo)
        logging.debug(msg)

        for site, jobcounts in cleanupInfo.items():
            idle = jobcounts["Idle"]
            running = jobcounts["Running"]
            msg="site: %s"%site
            logging.debug(msg)
            if self.siteThresholds.has_key(site):
              cleanupThresh = self.siteThresholds[site]["cleanupThreshold"]
              test = idle - cleanupThresh
              msg="GlideinWMSMonitor CleanUp: Site=%s, Idle=%s, Running=%s, Thresh=%s, Test=%s"%(site,idle,running,cleanupThresh,test)
              logging.debug(msg)

              if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "CleanUp"
                constraint['site'] = self.allSites[site]['SiteIndex']
                print str(constraint)
                result.append(constraint)


        msg="processingInfo: %s"%str(processingInfo)
        logging.debug(msg)

        for site, jobcounts in processingInfo.items():
            idle = jobcounts["Idle"]
            running = jobcounts["Running"]
            if self.siteThresholds.has_key(site):
              procThresh = self.siteThresholds[site]["processingThreshold"]
              minSubmit = self.siteThresholds[site]["minimumSubmission"]
              maxSubmit = self.siteThresholds[site]["maximumSubmission"]
              try:
                 procRunThrottle = self.siteThresholds[site]["processingRunningThrottle"]
              except:
                 procRunThrottle = 1000000000
              test = idle - procThresh
              msg="GlideinWMSMonitor Proc: Site=%s, Idle=%s, Running=%s Thresh=%s, Test=%s"%(site,idle,running,procThresh,test)
              logging.debug(msg)

              #  //  
              # //  Some sites want us to throttle jobs to a certain level...  this is a crude way to do that
              #||   Basically don't send in more jobs if we have more running jobs than the throttle level
              #//
              logging.debug("procRunThrottle: %s"%procRunThrottle)           
              if running>procRunThrottle:
                 msg="GlideinWMSMonitor: Running processing jobs (%s) exceed throttle level (%s), not sending in more"%(running,procRunThrottle) 
                 logging.debug(msg)
                 test=0

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
                
               


 
        msg="skimInfo: %s"%str(skimInfo)
        logging.debug(msg)

        for site, jobcounts in skimInfo.items():
            idle = jobcounts["Idle"]
            running = jobcounts["Running"]
            if self.siteThresholds.has_key(site):
              skimThresh = self.siteThresholds[site]["skimThreshold"]
              minSubmit = self.siteThresholds[site]["minimumSubmission"]
              maxSubmit = self.siteThresholds[site]["maximumSubmission"]
              try:
                 skimRunThrottle = self.siteThresholds[site]["skimRunningThrottle"]
              except:
                 skimRunThrottle = 1000000000
              test = idle - skimThresh
              msg="GlideinWMSMonitor Skim: Site=%s, Idle=%s, Running=%s Thresh=%s, Test=%s"%(site,idle,running,skimThresh,test)
              logging.debug(msg)

              #  //  
              # //  Some sites want us to throttle jobs to a certain level...  this is a crude way to do that
              #||   Basically don't send in more jobs if we have more running jobs than the throttle level
              #//
              logging.debug("skimRunThrottle: %s"%skimRunThrottle)           
              if running>skimRunThrottle:
                 msg="GlideinWMSMonitor: Running skim jobs (%s) exceed throttle level (%s), not sending in more"%(running,skimRunThrottle) 
                 logging.debug(msg)
                 test=0

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
                constraint['type'] = "Skim"
                constraint['site'] = self.allSites[site]['SiteIndex']
                print str(constraint)
                result.append(constraint)



        msg="harvestingInfo: %s"%str(harvestingInfo)
        logging.debug(msg)

        for site, jobcounts in harvestingInfo.items():
            idle = jobcounts["Idle"]
            running = jobcounts["Running"]
            if self.siteThresholds.has_key(site):
              harvestingThresh = self.siteThresholds[site]["harvestingThreshold"]
              minSubmit = self.siteThresholds[site]["minimumSubmission"]
              maxSubmit = self.siteThresholds[site]["maximumSubmission"]
              try:
                 harvestingRunThrottle = self.siteThresholds[site]["harvestingRunningThrottle"]
              except:
                 harvestingRunThrottle = 1000000000
              test = idle - harvestingThresh
              msg="GlideinWMSMonitor Harvesting: Site=%s, Idle=%s, Running=%s Thresh=%s, Test=%s"%(site,idle,running,harvestingThresh,test)
              logging.debug(msg)

              #  //  
              # //  Some sites want us to throttle jobs to a certain level...  this is a crude way to do that
              #||   Basically don't send in more jobs if we have more running jobs than the throttle level
              #//
              logging.debug("harvestingRunThrottle: %s"%harvestingRunThrottle)
              if running>harvestingRunThrottle:
                 msg="GlideinWMSMonitor: Running harvesting jobs (%s) exceed throttle level (%s), not sending in more"%(running,harvestingRunThrottle)
                 logging.debug(msg)
                 test=0

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
                constraint['type'] = "Harvesting"
                constraint['site'] = self.allSites[site]['SiteIndex']
                print str(constraint)
                result.append(constraint)



        return result
            

   


 
registerMonitor(GlideinWMSMonitor, GlideinWMSMonitor.__name__)
