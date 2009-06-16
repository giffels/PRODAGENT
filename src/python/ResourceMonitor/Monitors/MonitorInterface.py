#!/usr/bin/env python
"""
_MonitorInterface_

Interface class for Monitor plugins.

Interface is pretty simple:

Override the Call method to return a ResourceConstraint instance,
which is the number of resources available for jobs and constraints.

The PluginConfig mechanism is used for this as well, so you can read
dynamic parameters from self.pluginConfig


"""

import logging
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.ResourceConstraint import ResourceConstraint

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdAgent.ResourceControl.ResourceControlDB import ResourceControlDB

from JobQueue.JobQueueDB import JobQueueDB
from ProdMon.ProdMonDB import selectRcSitePerformance

class MonitorInterface:
    """
    _MonitorInterface_
    
    
    Abstract Interface Class for Resource Monitor Plugins

    """
    def __init__(self):
        self.performanceInterval = 259200 #3 days
        self.activeSites = []
        self.allSites = {}
        self.siteThresholds = {}
        self.siteAttributes = {}
        self.sitePerformance = {}
        self.retrieveSites()
        self.pluginConfiguration = None
        

    def newConstraint(self):
        """
        _newConstraint_

        Factory method, returns a new, empty constraint

        """
        return ResourceConstraint()


    def retrieveSites(self):
        """
        _retrieveSites_

        Return a list of all  sites from the ResourceControl DB
        and stores them in this object for access by the plugins
        
        """
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        
        resCon = ResourceControlDB()
        siteNames = resCon.siteNames()
        
        for site in siteNames:
            siteData = resCon.getSiteData(site)
            self.allSites[site] = siteData
            siteIndex = siteData['SiteIndex']
            if siteData['Active'] == True:
                self.activeSites.append(site)
            self.siteThresholds[site] = resCon.siteThresholds(siteIndex)
            self.siteAttributes[site] = resCon.siteAttributes(siteIndex)
            self.sitePerformance[site] = \
                selectRcSitePerformance(siteIndex, self.performanceInterval)
            
        del resCon
        
        self.jq = JobQueueDB()
        self.sitejobs = self.jq.countQueuedActiveJobs()
        
        Session.commit_all()
        Session.close_all()        
        return 
    

#    def retrieveSitePerformance(self):
#        """
#        get site performance - i.e. throughput and quality
#        """
#        Session.set_database(dbConfig)
#        Session.connect()
#        Session.start_transaction()
#        
#        #only get for active sites
#        for site in self.activeSites:
#            index = self.allSites[site]["SiteIndex"]
#            self.sitePerformance[site] = \
#                        selectRcSiteDetails(index, self.performanceInterval)
#        
#        Session.commit_all()
#        Session.close_all()        
#        return
    
    
    def dynamicallyAdjustThresholds(self, jobStatus):
        """
        Increase site thresholds for sites with a high
        fraction of running jobs
        
        Takes a dict of {site : {job_type : {status : number}}}
        
        Where status is one of Queued, Running, Done, Other
        
        """
        getThreshold = lambda x,y: x['%sThreshold' % y.lower()]
        
        for sitename, thresholds in self.siteThresholds.iteritems():

            minSubmit = thresholds.get("minimumSubmission", 1)
            maxSubmit = thresholds.get("maximumSubmission", 100)
            siteIndex = self.allSites[sitename]['SiteIndex']

            for jobtype in ('Processing',): # processing only for the moment
                threshold = getThreshold(thresholds, jobtype)
                if threshold <= 0:
                    continue
                sitejobstatus = jobStatus.get(siteIndex, {}).get(jobtype, {})
                queuing = sitejobstatus.get('Queued', 0)
                others = sitejobstatus.get('Other', 0)
                running = sitejobstatus.get('Running', 0)
                done = sitejobstatus.get('Done', 0)
                pending = queuing + others
                released = self.sitejobs.get(siteIndex, {}).get(jobtype, 0)
                logging.debug("Scheduler: %s/%s/%s/%s/%s %s jobs pending/running/done/other/released at %s" % \
                    (queuing, running, done, others, released, jobtype, sitename))

                # attempt to keep a desired level of jobs queued at each site
                desired = max(minSubmit, threshold / 5)
                if desired > maxSubmit:
                    desired = maxSubmit
                # increase thresholds if:
                #   o less than desired queuing
                #   o we will reach/exceed threshold in next submission
                #   o new value is larger than the current threshold
                #   o less than 5 * threshold already released
                if pending < desired and \
                    released >= (threshold - maxSubmit) and \
                    (released + desired) > threshold and \
                    released < (threshold * 5):
                    thresholds['%sThreshold' % jobtype.lower()] = released + desired
                    logging.info('Increased %s threshold to %s at %s due to low number of queued jobs' % (jobtype, 
                                getThreshold(thresholds, jobtype), sitename))


    def __call__(self):
        """
        _operator()_

        Override this method to make whatever callouts you need to
        determine that you have resources available

        Should return a list of ResourceConstraint instances that will be
        published as  ResourceAvailable events
        """
        return [ResourceConstraint() ]

    
