#!/usr/bin/env python
"""
_LCGSiteMonitor_

ResourceMonitor plugin that monitors LCG sites

"""

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ResourceMonitor.Monitors.LCGSiteInfo import getSiteInfoFromBase
from ResourceMonitor.Monitors.WorkflowConstraints import gtkToNotWorkflows
from ResourceMonitor.Monitors.BOSSInfo import anySiteJobs

import logging

import re, time, os


class LCGAdvanced(MonitorInterface):
    """
    _LCGAdvanced_

    Poll resources on the local machine and get details of all the ProdAgent
    jobs in there split by processing and merge type.

    Generate a per site constraint for each distinct site being used

    """


    def procThresh(self,gtk):
        defaultfraction=0.3
        if self.plugConf:
            if self.plugConf.has_key("processingFractionDefault"):
                defaultfraction=float(self.plugConf["processingFractionDefault"])
        logging.debug("LCGAdvanced: procThresh defaultfraction "+str(defaultfraction))


        cpus=int(self.siteinfo[gtk]['max_slots'])

        site=self.gtkToSite[gtk]
        st=self.siteThresholds[site]
        ## delete from rc_site_threshold where threshold_name = 'processingThreshold';
        if st.has_key("processingThreshold"):
            ## absolute value
            thres=int(st["processingThreshold"])
            pass
        elif st.has_key("processingFractionTotal"):
            ## a fraction of total available cpus as seen by BDII/lcg-infosites
            thres=int(cpus*float(st["processingFractionTotal"]))
            pass
        else:
            ## a deafault fraction of total available cpus as seen by BDII/lcg-infosites
            thres=int(cpus*defaultfraction)
            ## apply a maximum
            ## eg number of jobs that can be released in one poll cycle
            thres_max=300
            if thres > thres_max:
                logging.info("LCGAdvanced: procThresh "+str(thres)+" larger than thres_max "+str(thres_max))
                thres=thres_max
            pass

        logging.debug("LCGAdvanced: procThresh site "+site+" value "+str(thres))
        return thres	

    def mergeTresh(self,gtk):
        defaultfraction=1
        if self.plugConf:
            if self.plugConf.has_key("mergeFractionDefault"):
                defaultfraction=float(self.plugConf["mergeFractionDefault"])
        logging.debug("LCGAdvanced: mergeThresh defaultfraction "+str(defaultfraction))

        site=self.gtkToSite[gtk]
        st=self.siteThresholds[site]
        if st.has_key("mergeThreshold"):
            ## absolute value
            thres=int(st["mergeThreshold"])
            pass
        elif st.has_key("mergeFractionProc"):
            ## a fraction of total processingJobs
            thres=int(self.procTresh(gtk)*float(st["mergeFractionProc"]))
            pass
        elif st.has_key("mergeFractionTotal"):
            ## a fraction of total available cpus as seen by BDII/lcg-infosites
            thres=int(int(self.siteinfo[gtk]['max_slots'])*float(st["mergeFractionTotal"]))
            pass
        else:
            ## default is identical as processingJobs
            thres=int(defaultfraction*self.procThresh(gtk))
            pass

        logging.debug("LCGAdvanced: mergeThresh site "+site+" value "+str(thres))
        return thres

    def minSubmit(self,gtk):
        defaultfraction=0.5
        if self.plugConf:
            if self.plugConf.has_key("minimumSubmissionFractionDefault"):
                defaultfraction=float(self.plugConf["minimumSubmissionFractionDefault"])
        logging.info("LCGAdvanced: minSubmit defaultfraction "+str(defaultfraction))

        site=self.gtkToSite[gtk]
        st=self.siteThresholds[site]
        if st.has_key("minimumSubmission"):
            mini = int(self.siteThresholds[site]["minimumSubmission"])
        elif st.has_key("minimumSubmissionFractionProc"):
            ## a certain fraction of procThreshold
            mini = int(self.procThresh(gtk)*st["minimumSubmissionFractionProc"])
        elif st.has_key("minimumSubmissionFractionTotal"):
            ## a certain fraction of total availble CPUs 
            mini = int(int(self.siteinfo[gtk]['max_slots'])*float(st["minimumSubmissionFractionTotal"]))
        else:
            ## default is fraction of procTresh
            mini = int(defaultfraction*self.procThresh(gtk))

        logging.debug("LCGAdvanced: minSubmit site "+site+" value "+str(mini))
        return mini

    def maxSubmit(self,gtk):
        defaultfraction=0.2
        if self.plugConf:
            if self.plugConf.has_key("maximumSubmissionFractionDefault"):
                defaultfraction=float(self.plugConf["maximumSubmissionFractionDefault"])
        logging.debug("LCGAdvanced: maxSubmit defaultfraction "+str(defaultfraction))

        ## delete from rc_site_threshold where threshold_name = 'maximumSubmission';
        site=self.gtkToSite[gtk]
        st=self.siteThresholds[site]
        if st.has_key("maximumSubmission"):
            maxi = int(self.siteThresholds[site]["maximumSubmission"])
        elif st.has_key("maximumSubmissionFractionProc"):
            ## a certain fraction of procThreshold
            maxi = int(self.procThresh(gtk)*st["maximumSubmissionFractionProc"])
        elif st.has_key("maximumSubmissionFractionTotal"):
            ## a certain fraction of total availble CPUs 
            maxi = int(int(self.siteinfo[gtk]['max_slots'])*float(st["maximumSubmissionFractionTotal"]))
        else:
            ## default is fraction of procTresh
            ## maxi = int(defaultfraction*self.procThresh(gtk))

            ## using procTresh is stupid
            maxi = int(int(self.siteinfo[gtk]['max_slots'])*float(defaultfraction))

        logging.debug("LCGAdvanced: maxSubmit site "+site+" value "+str(maxi))
        if maxi < 1:
            logging.info("LCGAdvanced: maxSubmit site "+site+" value "+str(maxi)+" lower than 1. Resetting.")
            maxi=1

        return maxi

    def __call__(self):
        result = []

        self.plugConf=None
        if self.pluginConfiguration.has_key("LCGAdvanced"):
            self.plugConf=self.pluginConfiguration["LCGAdvanced"]

        if self.plugConf:
            logging.info("LCGAdvanced: Plugin config file found for LCGAdvanced")
            
        
        if self.plugConf.has_key("UseSAM") and self.plugConf["UseSAM"].lower() in ("true", "yes"):
            self.plugConf["UseSAM"] = True
        else:
            self.plugConf["UseSAM"] = False
            logging.info("ignoring SAM state")

        self.plugConf["SAMurl"] = self.plugConf.get("SAMurl", "http://lxarda16.cern.ch/dashboard/request.py/latestresultssmry")    
        if self.plugConf.has_key("UseFCR") and self.plugConf["UseFCR"].lower() in ("true", "yes"):
            self.plugConf["UseFCR"] = True
        else:
            self.plugConf["UseFCR"] = False
            logging.info("ignoring fcr state")

        self.plugConf["FCRurls"] = self.plugConf.get("FCRurls", 'http://lcg-fcr.cern.ch:8083/fcr-data/exclude.ldif')

    # Reverse lookup table for ce -> site name
    ## CE == gatekeeper !!
        self.gtkToSite={}
        [ self.gtkToSite.__setitem__(
                self.allSites[x]['CEName'],
                self.allSites[x]['SiteName']) for x in self.activeSites ]  

        logging.debug("LCGAdvanced: Found active gatekeepers/site "+str(self.gtkToSite))

        ## check for ill-formed entries
        gtk_reg=re.compile(r"(?P<ce>.*?)(:(?P<port>\d+))/(?P<queue>.*)$")
        for gtk in self.gtkToSite.keys():
            if not gtk_reg.search(gtk): 
                logging.info("LCGAdvanced: Configured gatekeeper "+gtk+" does not match regexp. Removing from resources to be checked.")
                del self.gtkToSite[gtk]

        ## get the resources for all sites
        site_names=[]
        for site in self.gtkToSite.values():
            name_in_bdii=site.split(' ')[0]
            if not name_in_bdii in site_names:
                site_names.append(name_in_bdii)

        fcr_urls = self.plugConf["FCRurls"].split(',')
        if self.plugConf["UseFCR"]:
             fcr_urls = self.plugConf["FCRurls"].split(',')
        else:
             fcr_urls = None      
 
        samURL = None
        if self.plugConf["UseSAM"]:
             samURL = self.plugConf["SAMurl"]

        ## no need to use phedex, give a dummy value as DBParam
        dbparam='TEST'

        ## BDII to use		
        bdii=None
        if self.plugConf.has_key("BDII"):
            bdii=self.plugConf["BDII"]
        if not bdii:
            if os.environ.has_key('LCG_GFAL_INFOSYS'):
                bdii=os.environ['LCG_GFAL_INFOSYS']
            else:
                logging.error("LCGAdvanced: No bdii defined and no value for LCG_GFAL_INFOSYS found.")
                return  result
        try:
            logging.debug("Calling getSiteInfoFromBase with site_names "+str(site_names)+" fcr_urls "+str(fcr_urls)+" bdii "+str(bdii)+" dbparam "+str(dbparam)+" sam " + str(samURL))
            self.siteinfo=getSiteInfoFromBase(site_names,fcr_urls,bdii,dbparam,samURL)
        except Exception, ex:
            logging.error("error during poll: %s" % str(ex))
            return result

        ## clean out self.siteinfo with unused entries
        to_del=[]
        for ceuid in self.siteinfo.keys():
            if not self.gtkToSite.has_key(ceuid):
                logging.info("LCGAdvanced: found ceuid "+ceuid+" but not in RCDB. Ignoring.")
                to_del.append(ceuid)
        for ceuid in to_del:
            del self.siteinfo[ceuid]

        ## dump current state of resources
        if self.plugConf.has_key("DumpState"):
            place=self.plugConf["DumpState"]
            if not os.path.isfile(place):
                f=file(place,'w')
                f.write('## BEGIN\n\n')
                f.close()
            f=file(place,'a')
            f.write(str([time.localtime(),self.siteinfo])+'\n')
            f.close()

        ## remove sites
        to_del={}
        for ceuid,val in self.siteinfo.items():
            if not val['state'] == 'Production':
                logging.info("LCGAdvanced: Removing ce/site \""+ceuid+"/"+val['site_name']+"\" combo from available resources. Site state is "+val['state']+", not Production")
                to_del[ceuid] = True
            if val['in_fcr'] and use_fcr:
                logging.info("LCGAdvanced: Removing ce/site \""+ceuid+"/"+val['site_name']+"\" combo from available resources. Site is in FCR.")
                to_del[ceuid] = True
            if val['SAMfail']:
                logging.info("LCGAdvanced: Removing ce/site \""+ceuid+"/"+val['site_name']+"\" combo from available resources. CE is failing SAM tests")
                to_del[ceuid] = True

        for ceuid,val in to_del.items():
            if val:
                del self.gtkToSite[ceuid]

        ## gtkToNotWorklfow map
        gtkToWorkflow=gtkToNotWorkflows(self.siteinfo)
        logging.debug("LCGAdvanced: gtkToNotWorkflow "+str(gtkToWorkflow))

        # get totals per active gatekeeper for any jobs
        ## returns a dictionary with {'Idle':\d+,'Running':\d+}
        anySiteInfo = anySiteJobs(self.gtkToSite.keys())
        logging.debug("LCGAdvanced: anySiteInfo "+str(anySiteInfo))

        for gtk, jobcounts in anySiteInfo.items():
            if not self.siteinfo.has_key(gtk):
                logging.info("LCGAdvanced: gtk "+gtk+" in self.gtkToSite but not in self.siteinfo. Skipping.")
                continue
            idle = jobcounts['Idle']
            site=self.gtkToSite[gtk]
            test = idle - self.procThresh(gtk)
            minSub = self.minSubmit(gtk)
            maxSub = self.maxSubmit(gtk)

            logging.debug("LCGAdvanced: idle "+str(idle)+" running "+str(jobcounts['Running'])+" site "+site+" test "+str(test)+" minSub "+str(minSub))

            if test < 0:
                if abs(test) < minSub:
                    # below threshold, but not enough for a bulk submission
                    continue
                constraint = self.newConstraint()
                cou = abs(test)
                if cou > maxSub:
                    logging.debug("LCGAdvanced: number of possible jobs to be released "+str(cou)+" is larger than maximum allowed "+str(maxSub))
                    cou = maxSub

                constraint['count'] = cou
                constraint['type'] = None
                constraint['site'] = self.allSites[site]['SiteIndex']
                ## workflow constraint, default is None
                if len(gtkToWorkflow[gtk])>0:
                    constraint['workflow'] = ','.join(gtkToWorkflow[gtk])
                #logging.info("LCGAdvanced: Constraint for site "+site+" :"+str(constraint))
                result.append(constraint)
                #logging.info("LCGAdvanced: All constraints:"+str(result))


        tmp={}
        import random

        for r in result:
            cou=int(r['count'])
            if not tmp.has_key(cou):
                tmp[cou]=[]
            tmp[cou].append(r)
            ## randomise site order for sites with equal amount of count
            random.shuffle(tmp[cou])

        t_k=tmp.keys()

        ## reoder the results. send smallest 'count' first (should favour small sites)
        ## favours small sites too much.
        #t_k.sort()

        ## randomise the list. doesn't favour anything
        random.shuffle(t_k)

        result=[]
        for k in t_k:
            for res in tmp[k]:
                result.append(res)

        logging.info("LCGAdvanced: reordered constraints")
        for constraint in result:
            logging.info("LCGAdvanced: Constraint :"+str(constraint))

        return result



            
registerMonitor(LCGAdvanced, LCGAdvanced.__name__)
