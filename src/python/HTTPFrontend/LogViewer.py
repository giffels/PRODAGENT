#!/usr/bin/env python
"""
_JobQueueMonitor_

CherryPy handler for displaying job logs

"""

from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig

from LogCollector.LogCollectorDB import getCollectedLogDetails, \
                                        getUnCollectedLogDetails

from ProdCommon.SiteDB import SiteDB

import os
import urllib2
import logging

sitePFNMappingURL = "http://t2.unl.edu/phedex/tfc/map/"

class LogViewer:

    def index(self):
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()

        logs = getUnCollectedLogDetails()

        # find site pfns
        ses = set()
        for wf, details in logs.items():
            for se, log in details.items():
                ses.add(se)
        sitesPFNMapping = self.getSitePFNMapping(ses)

        # now format html
        html = """<html><body><h2>Job Logs</h2>\n """ 

        html += "<table>\n"
        html += " <tr><th>Workflow</th><th>SE</th><th>Log</th></tr>\n"

        for wf, details in logs.items():
            for site, details in details.items():
                for log in details:

                    html += "  <tr><th>%s</th><th>%s</th><th>%s</th></tr>\n" % (wf, site, \
                                            self.formatSRMcommand(sitesPFNMapping, site, log))
#        html += "<td>%s</td></tr>\n" % len(queuedProcJobs)
#
#        html += " <tr><td>Processing</td><td>Released</td>"
#        html += "<td>%s</td></tr>\n" % len(releasedProcJobs)
#
#        html += " <tr><td>Merge</td><td>Queued</td>"
#        html += "<td>%s</td></tr>\n" % len(queuedMrgJobs)
#
#        html += " <tr><td>Merge</td><td>Released</td>"
#        html += "<td>%s</td></tr>\n" % len(releasedMrgJobs)
        
        
        html += "</table>\n"
        html += """</body></html>"""
        Session.commit_all()
        Session.close_all()


        return html
    index.exposed = True

    
    def getSitePFNMapping(self, se_names):
        """
        Contact Brians server and get PFN mapping for log url's.
        Due to different namng conventions between phedex and mc prod
        this function tries multiple possible combinations - 
        sorry for the extra server load
        """
        results = {}
        for se in se_names:
            if results.has_key(se):
                continue;
            pfnRoot = None
            #TODO: is this doable in one http call?
            siteDetails = SiteDB.getJSON("SEtoCMSName", name=se)
            if not siteDetails.has_key('0') and not siteDetails['0'].has_key('name'):
                continue
            site = siteDetails['0']['name']
            # Buffer should match so MSS not really needed - kept for completeness
            for siteName in (site, "%s_Buffer" % site, "%s_MSS" % site):
                try:
                    data = \
                    urllib2.urlopen("%s/%s?lfn=/store/&protocol=srmv2" % 
                                        (sitePFNMappingURL, siteName))
                    results[se] = data.readline()
                    break   #stop looping over this site
                except IOError:
                    pass
        return results


    def formatSRMcommand(self, sitesPFNMapping, site, lfn):
        
        if not sitesPFNMapping.has_key(site):
            return "Unable to map LFN to PFN at site"
        
        turl = "%s%s" % ( sitesPFNMapping[site], lfn.replace("/store/", "", 1))
        return "srmcp -2 %s file:///$PWD/%s" % (turl, os.path.basename(lfn))