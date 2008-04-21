#!/usr/bin/env python
"""
_ResourceMonitors_

Details from the ResourceControlDB and ResourceMonitor
plugin

"""


import ProdAgent.ResourceControl.ResourceControlAPI as ResConAPI
from ProdAgentCore.Configuration import loadProdAgentConfiguration

import ResourceMonitor.Monitors
from ResourceMonitor.Registry import retrieveMonitor

def getResourceMonitorPlugin():
    try:
        config = loadProdAgentConfiguration()
        compCfg = config.getConfig("ResourceMonitor")
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        raise RuntimeError, msg

    
    monitor = compCfg.get('MonitorName', None)
    if monitor == None:
        msg = "No MonitorName found in ResourceMonitor config\n"
        msg += "for this ProdAgent"
        raise RuntimeError, msg

    try:
        monitorInstance = retrieveMonitor(monitor)
    except Exception, ex:
        msg = "Unable to load monitor named: %s\n" % monitor
        msg += str(ex)
        raise RuntimeError, msg

    return monitorInstance
        

class ResourceDetails:

    def __init__(self):
        pass


    def index(self):
        html = """<html><body><h2>Resources Known to this ProdAgent</h2>\n """
        html += "<table>\n"
        html += "<tr><th>Index</th><th>Name</th><th>Active</th><th>CEName</th><th>SEName</th></tr>\n"

        siteData = ResConAPI.allSiteData()
        for site in siteData:
            html += "<tr><td>%s</td><td>%s</td>" % (site["SiteIndex"],
                                                    site["SiteName"])
            html += "<td>%s</td><td>%s</td><td>%s</td></tr>\n" % (
                site['Active'], site["CEName"], site['SEName'])
            
            
        
        

        html += "</table>\n"
        html += """</body></html>"""
        return html
    index.exposed = True


class ResourceStatus:

    def __init__(self):
        pass


    def index(self):
        html = """<html><body><h2>Resource Status for this ProdAgent</h2>\n """

        try:
            monInstance = getResourceMonitorPlugin()
        except RuntimeError, msg:
            html += "<p> Error retrieving monitor: %s</p>" % msg
            html += """</body></html>"""
            return html
        

        try:
            resourceConstraints = monInstance()
        except Exception, ex:
            import traceback
            html += "<p> Error retrieving resource status: %s</p>" % str(ex)
            html += "<pre>%s</pre>" % traceback.format_exc()
            html += """</body></html>"""
            return html

        siteNames = ResConAPI.createSiteNameMap()

        html += "<table>\n"
        html += "<tr><th>Count</th><th>Type</th><th>Site</th></tr>\n"
        for constraint in resourceConstraints:
            html += "<tr><td>%s</td>" % constraint['count']
            html += "<td>%s</td>" % constraint['type']
            siteName = None
            if constraint['site'] != None:
                siteName = siteNames.get(constraint['site'], None)
            html += "<td>%s</td></tr>\n" % siteName
            

        html += "</table>\n"        
        html += """</body></html>"""
        return html
    index.exposed = True

        
