#!/usr/bin/env python
"""
_LCGSiteMonitor_

ResourceMonitor plugin that monitors LCG sites

"""

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ResourceMonitor.Monitors.LCGSiteInfo import getSiteInfoFromBase
from ResourceMonitor.Monitors.WorkflowConstraints import siteToNotWorkflows
from ProdCommon.Database import Session

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

        # only look at active sites
        sites = [x for x in self.allSites.values() if x['SiteName'] in self.activeSites]

        ## check for ill-formed entries
        sites = self.validateCENames(sites)

        ## no need to use phedex, give a dummy value as DBParam
        dbparam = 'TEST'
       
        self.siteinfo = getSiteInfoFromBase(sites,
                                            self.plugConf["FCRurls"],
                                            self.plugConf["BDII"],
                                            dbparam, self.plugConf["SAMurl"])
        
        # filter out failing sites
        good_sites = self.sitesPassingTests(sites)
        
        # check sites have required cmssw versions for active workflows
        wf_constraints = siteToNotWorkflows(good_sites, self.siteinfo)
#        logging.debug("Site/Workflow incompatibilities: %s" % \
#                                          str(siteWorkflowIncompatibilities))

        
        # if requested write site status file
        self.writeSiteStatusFile(self.siteinfo)
        
        # Increase thresholds for sites with few jobs queuing
#        schedulerJobStatus = None # either from monitoring or API, check with Giuseppe
#        siteJobStatus = self.jq.combineSchedulerStatus(schedulerJobStatus)
        if self.plugConf.get("DynamicallyAdjustThresholds", False):
            self.dynamicallyAdjustThresholds(self.schedulerSiteStatus())
        
        # get constraints for sites and jobtypes
        result = self.getConstraints(good_sites, wf_constraints)
        
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
        
        if self.plugConf.get("DynamicallyAdjustThresholds", "false").lower() in ("true", "yes"):
            self.plugConf["DynamicallyAdjustThresholds"] = True
        else:
            self.plugConf["DynamicallyAdjustThresholds"] = False
    
    
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
    
    
    def writeSiteStatusFile(self, info):
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
            f.write(str([time.localtime(), info])+'\n')
            f.close()
        return
            
            
    def sitesPassingTests(self, sites):
        """
        get list of sites passing tests and with good job quality
        """
        result = []
        
        for rc_site in sites:
            
            name = rc_site['SiteName']
            site = self.siteinfo.get(name, None)
            if not site:
                logging.info("LCGAdvanced: Site %s not returned from info system query. Either see above for error or it is missing." \
                                                        % (name))
                continue
            
            quality = self.sitePerformance[name]['quality']
            if quality and quality < self.plugConf["SiteQualityCutOff"]:
                logging.info("LCGAdvanced: Removing site %s from available resources. Job quality is poor - %s" \
                                                                         % (name, str(quality)))
                continue
            
            good_ces = []
            # multiple CE's per site - require at least 1 to pass tests
            for ce, val in site.items():
                if not val['state'] == 'Production':
                    logging.info("LCGAdvanced: Removing %s from available resources. State is %s, not Production" \
                                                                         % (ce, val['state']))
                    site.pop(ce)
                    continue
                elif val['in_fcr'] and self.plugConf["UseFCR"]:
                    logging.info("LCGAdvanced: Removing %s from available resources. CE is in FCR" % ce)
                    site.pop(ce)
                    continue
                elif val['SAMfail'] and self.plugConf["UseSAM"]:
                    logging.info("LCGAdvanced: Removing %s from available resources. CE is failing SAM tests" \
                                                                         % ce)
                    site.pop(ce)
                    continue
                
                good_ces.append(ce)
            
            if site:
                result.append(rc_site)
            else:
                logging.info("LCGAdvanced: All CE's at %s fail tests. Ignoring site" % name)
            
        return result
    
    
    def validateCENames(self, sites):
        """
        delete malformed CE's 
        """
        result = []
        gtk_reg = re.compile(r"(?P<ce>.*?)(:(?P<port>\d+))/(?P<queue>.*)$")
        for site in sites:
            ce = site['CEName']
            if ce and not gtk_reg.search(ce): #ce not specified if letting wms manage job distribution
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
        for site in sites:
            sitename = site['SiteName']
            
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


    def schedulerSiteStatus(self):
        """
        Find the number of jobs per site with a given scheduler status
        This is naughty as we join with BossLite tables but the alternative is
        to get a list of all jobs and combine with we_Job which will be very
        heavy
        """
        statusMap = {'Waiting' : 'Queued',
                     'Ready' : 'Queued',
                     'Submitted' : 'Queued', 
                     'Scheduled' : 'Queued',
                      'Running' : 'Running',
                      'Done' : 'Done',
                      'Retrieved' : 'Done',
                      'Aborted'   : 'Done',
                      'Other' : 'Other',
                      None : 'Other', # can sometimes get this
                      'None' : 'Other'
                      }
        result = {}
#        query = """SELECT released_site,bl_task.job_type,status_scheduler,count(*)
#                    from bl_runningjob, bl_task, jq_queue, bl_job, we_Job
#                    WHERE bl_job.task_id=bl_task.id 
#                    and bl_runningjob.job_id = bl_job.job_id 
#                    and jq_queue.job_spec_id = bl_job.name
#                    and jq_queue.status = 'released'
#                    and bl_runningjob.closed != 'Y'
#                    and we_Job.id = jq_queue.job_spec_id
#                    and we_Job.status = 'inProgress'
#                    and bl_job.submission_number = bl_runningjob.submission
#                    GROUP BY status_scheduler,bl_task.job_type"""
        query = """SELECT released_site,job_type,status_scheduler,count(*)
                    FROM bl_runningjob
                    JOIN bl_job ON bl_runningjob.task_id=bl_job.task_id 
                      and bl_runningjob.job_id = bl_job.job_id
                      and bl_job.submission_number = bl_runningjob.submission
                    JOIN jq_queue ON jq_queue.job_spec_id = bl_job.name
                    WHERE jq_queue.status = 'released' and bl_runningjob.closed != 'Y'
                    GROUP BY status_scheduler,job_type"""
        Session.execute(query)
        temp = Session.fetchall()
        for site, jobtype, status, count in temp:
            if status not in statusMap:
                logging.error('Job status %s unknown - fall back to Other' % status)
                status = 'Other'
            
            result.setdefault(site, {}).setdefault(jobtype, {})[statusMap[status]] = count
        return result


registerMonitor(LCGAdvanced, LCGAdvanced.__name__)
