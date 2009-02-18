#!/usr/bin/env python
"""
_LCGSiteMonitor_

ResourceMonitor plugin that monitors LCG sites

"""

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ResourceMonitor.Monitors.LCGSiteInfo import getSiteInfoFromBase
from ResourceMonitor.Monitors.WorkflowConstraints import siteToNotWorkflows
from JobQueue.JobQueueDB import JobQueueDB

import logging

import re, time, os


class LCGAdvanced(MonitorInterface):
    """
    _LCGAdvanced_

    Poll resources on the local machine and get details of all the ProdAgent
    jobs in there split by processing and merge type.
    
    Generate a per site constraint for each distinct site being used

    """

    SAMurl = "http://lxarda16.cern.ch/dashboard/request.py/latestresultssmry"
    FCRurl = "http://lcg-fcr.cern.ch:8083/fcr-data/exclude.ldif"
    
    jobTypes = []
    jobTypes.append({"type" : "Processing", "defaultFrac" : 1.0})
    jobTypes.append({"type" : "Merge", "defaultFrac" : 0.3})
    jobTypes.append({"type" : "CleanUp", "defaultFrac" : 0.3})
    jobTypes.append({"type" : "LogCollect", "defaultFrac" : 0.3})


    def __call__(self):
        self.loadPluginConfig()

        ## check for ill-formed entries
        good_sites = self.validateCENames(self.activeSites)

        ## no need to use phedex, give a dummy value as DBParam
        dbparam = 'TEST'
       
        self.siteinfo = getSiteInfoFromBase(good_sites,
                                            self.plugConf["FCRurls"],
                                            self.plugConf["BDII"],
                                            dbparam, self.plugConf["SAMurl"])
        
        good_sites = self.sitesPassingTests(good_sites)
      
        # if requested write site status file
        self.writeSiteStatusFile()
        
        # check sites have required cmssw versions for active workflows
        siteWorkflowIncompatibilities = siteToNotWorkflows(good_sites,
                                          self.allSites, self.siteinfo)
        logging.debug("Site/Workflow incompatibilities: %s" % \
                                          str(siteWorkflowIncompatibilities))

        # get active jobs released at a site
        jq = JobQueueDB()
        self.sitejobs = jq.countQueuedActiveJobs()
        logging.debug("Released jobs at sites: %s" % str(self.sitejobs))
        
        # get constraints for sites and jobtypes
        result = self.getConstraints(good_sites, siteWorkflowIncompatibilities)
        
        # reorder constraints - sites with most successful jobs first
        result = self.reOrderConstraints(result)        

        for constraint in result:
            logging.info("LCGAdvanced: Constraint :" + str(constraint))

        return result
            
            
    def loadPluginConfig(self):
        """
        load the relevant plugin config options
        """
        self.plugConf = self.pluginConfiguration.get("LCGAdvanced", None)

        if not self.plugConf:
            logging.info("LCGAdvanced: No plugin config file found for LCGAdvanced")
            self.plugConf = {}
            
        if self.plugConf.get("UseSAM", "false").lower() in ("true", "yes"):
            self.plugConf["UseSAM"] = True
            self.plugConf["SAMurl"] = self.plugConf.get("SAMurl", self.SAMurl)
        else:
            self.plugConf["UseSAM"] = False
            self.plugConf["SAMurl"] = None
            logging.info("ignoring SAM state")
        
        if self.plugConf.get("UseFCR", "false").lower() in ("true", "yes"):
            self.plugConf["UseFCR"] = True
            self.plugConf["FCRurls"] = self.plugConf.get("FCRurls", self.FCRurl)
        else:
            self.plugConf["UseFCR"] = False
            self.plugConf["FCRurls"] = []
            logging.info("Ignoring fcr state")

        if not self.plugConf.has_key("BDII"):
            self.plugConf["BDII"] = os.environ.get("LCG_GFAL_INFOSYS", None)
            if not self.plugConf["BDII"]:
                logging.error("LCGAdvanced: No bdii defined and no value for LCG_GFAL_INFOSYS found.")
                raise Exception("BDII not set in config file and env variable LCG_GFAL_INFOSYS not defined")
    
        self.plugConf["SiteQualityCutOff"] = float(self.plugConf.get("SiteQualityCutOff", 0.5))
    
    
    def setThresholds(self, siteData):
        """
        set thresholds for site
        """
        
        site = siteData['SiteName']
        gtk = siteData['CEName']
        st = self.siteThresholds[site]
        
        for jobtype in self.jobTypes:
            
            # if defined for site take that
            if st.has_key("%sThreshold" % jobtype["type"].lower()):
                thres = int(st["%sThreshold" % jobtype["type"].lower()])
            
            # see if fraction of bdii job slots set
            elif st.has_key("%sFractionTotal" % jobtype["type"].lower()):
                cpus = int(self.siteinfo[gtk]['max_slots'])
                thres = max(int(cpus*float(st["%sTotal" % jobtype["type"].lower()])), 1)

            # take default value of processing slots
            else:
                thres = max(int( int(st['processingThreshold']) * \
                                                float(jobtype["defaultFrac"])), 1)

            st["%sThreshold" % jobtype["type"].lower()] = thres
            logging.info("LCGAdvanced: %s threshold = %s for site %s" % \
                                        (jobtype["type"], str(thres), site))
        return
    

    def minSubmit(self, siteData):
        """
        get minimum amount of resources to publish for site
        """
        site = siteData['SiteName']
        
        mini = int(self.siteThresholds[site].get("minimumSubmission", 1))
        logging.debug("LCGAdvanced: minSubmit site "+site+" value "+str(mini))
        return mini


    def maxSubmit(self, siteData):
        """
        get maximum amount of resources to publish for site
        """
    
        site = siteData['SiteName']
        
        maxi = int(self.siteThresholds[site].get("maximumSubmission", 100))
        logging.debug("LCGAdvanced: maxSubmit site "+site+" value "+str(maxi))
        return maxi
    
    
    def writeSiteStatusFile(self):
        """
        write to file of current site status
        """
        if self.plugConf.has_key("DumpState"):
            place = self.plugConf["DumpState"]
            if not os.path.isfile(place):
                f = file(place,'w')
                f.write('## BEGIN\n\n')
                f.close()
            f = file(place,'a')
            f.write(str([time.localtime(), self.siteinfo])+'\n')
            f.close()
        return
            
            
    def sitesPassingTests(self, sites):
        """
        get list of sites passing tests and with good job quality
        """
        result = []
        
        for site in sites:
            siteData = self.allSites[site]
            
            if siteData['CEName'] not in self.siteinfo.keys():
                logging.info("LCGAdvanced: "+site+" not in information system. Skipping.")
                continue
            
            val = self.siteinfo[siteData['CEName']]
            
            if not val['state'] == 'Production':
                logging.info("LCGAdvanced: Removing site %s from available resources. Site state is %s, not Production" \
                                                                     % (site, val['state']))
                continue
            elif val['in_fcr'] and self.plugConf["UseFCR"]:
                logging.info("LCGAdvanced: Removing site %s from available resources. Site is in FCR" \
                                                                     % (site))
                continue
            elif val['SAMfail'] and self.plugConf["UseSAM"]:
                logging.info("LCGAdvanced: Removing site %s from available resources. CE is failing SAM tests" \
                                                                     % (site))
                continue
            
            quality = self.sitePerformance[site]['quality']
            if quality and quality < self.plugConf["SiteQualityCutOff"]:
                logging.info("LCGAdvanced: Removing site %s from available resources. Job quality is poor - %s" \
                                                                     % (site, str(quality)))
                continue

            result.append(site)
        return result
    
    
    def validateCENames(self, sites):
        """
        delete malformed CE's 
        """
        result = []
        gtk_reg = re.compile(r"(?P<ce>.*?)(:(?P<port>\d+))/(?P<queue>.*)$")
        for site in sites:
            ce = self.allSites[site]['CEName']
            if not gtk_reg.search(ce): 
                logging.warning("LCGAdvanced: Configured gatekeeper "+ce+" does not match regexp. Removing from resources to be checked.")
                continue
            result.append(site)
        return result
    
    
    def reOrderConstraints(self, constraints):
        """
        reorder constraints in terms of throughput
        """
        result = []

        # first sort performance list
        sitesOrdered = self.sitePerformance.items()
        sitesOrdered.sort(lambda x, y: x[1]['success'] - y[1]['success'])
        sitesOrdered.reverse()
        
        # then sort constraints
        for site in sitesOrdered:
            siteindex = self.allSites[site[0]]['SiteIndex']
            for constraint in constraints:
                if constraint['site'] == siteindex:
                    result.append(constraint)
        return result


    def getConstraints(self, sites, siteWorkflowIncompatibilities):
        """
        get the constriants for given sites
        """
        result = []
        for sitename in sites:
            site = self.allSites[sitename]
            
            self.setThresholds(site)
            thresholds = self.siteThresholds[sitename]
            minSub = self.minSubmit(site)
            maxSub = self.maxSubmit(site)

            workflowContraints = siteWorkflowIncompatibilities.get(sitename, [])

            # loop over job types and publish resource availability
            for type in self.jobTypes:
                jobtype = type['type']
                limit = thresholds['%sThreshold' % jobtype.lower() ]
                
                currentjobs = self.sitejobs.get(site['SiteIndex'], {}).get(jobtype, 0)
                logging.debug("%s jobs of type %s at %s" %(currentjobs, jobtype, sitename))
                available = limit - currentjobs
                
                # is there resources available
                if available > 0:
                    
                    #check min/max submit value for processing
                    # others dont obey min/max rules
                    if jobtype in ('Processing'):
                        if available < minSub:
                            # wait till minSub slots available
                            continue
                        elif available > maxSub:
                            available = maxSub
                
                    constraint = self.newConstraint()
                    constraint['count'] = available
                    constraint['type'] = jobtype
                    constraint['site'] = site['SiteIndex']
                    ## workflow constraint, default is None
                    if len(workflowContraints) > 0:
                        constraint['workflow'] = ','.join(workflowContraints)
                    result.append(constraint)
        
        return result
            
            
registerMonitor(LCGAdvanced, LCGAdvanced.__name__)
