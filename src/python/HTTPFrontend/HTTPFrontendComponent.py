#!/usr/bin/env python
"""
_HTTPFrontend_

Component that runs a CherryPy based web server to provide HTTP
access to the JobCreator cache.

May also add some interactive monitoring as time goes on.

Introduces a dependency on the cherrypy package

"""
import socket
import logging
import os
import cherrypy
from cherrypy.lib.static import serve_file
from ProdAgentCore.Configuration import prodAgentName
from ProdAgentCore.Configuration import loadProdAgentConfiguration

import ProdAgentCore.LoggingUtils as LoggingUtils

from TaskMonitor import TaskMonitor
from JobRetrievingMonitor import JobRetrievingMonitor
from JobResubmittingMonitor import JobResubmittingMonitor
from ResourceMonitors import ResourceDetails,ResourceStatus
#from Hist_SchStMonitor import Hist_SchStMonitor
from HistStatusMonitor import HistStatusMonitor
from HistHWMonitor import HistHWMonitor

#VM test page 
from HrMonitor import HrMonitor

def getLocalDBSURL():
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        dbsConfig = config.getConfig("LocalDBS")
    except StandardError, ex:
        msg = "Error reading configuration for LocalDBS:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return dbsConfig.get("DBSURL", None)




class Root:
    """
    _Root_

    Main index page for the component, will appear as the index page
    of the toplevel address

    """
    def __init__(self, myUrl):
        self.myUrl = myUrl
        
    def index(self):
        html = """<html><body><h1>CRAB WEB MONITOR at %s </h1>\n """ % (
            
            os.environ['HOSTNAME'], )
	html += "\n"
        html += "<h2>Current Status</h2>"
        html += "<table width=\"100"+'%'+"\">\n"
        html += "<tr><th  align=\"left\" width=\"20"+'%'+"\">Service</th><th  align=\"left\">Description</th></tr>\n"
        html += "<tr><td width=\"20"+'%'+"\"><a href=\"%s/task\">Task</a></td>\n" % (
            self.myUrl,)
        html += "<td>Task status information </td></td>\n"

        html += "<tr><td width=\"20"+'%'+"\"><a href=\"%s/resubmitting\">Job Resubmitting</a></td>\n" % (
            self.myUrl,)
        html += "<td>Show number of job in resubmitting</td></td>\n"
        html += "<tr><td><a href=\"%s/retrieving\">Job Retrieving </a></td>\n" % (
            self.myUrl,)
        html += "<td>Done Cleared vs Done not Cleared</td></td>\n"
#        html += "<tr><td><a href=\"%s/resources\">build...</a></td>\n" % (
#            self.myUrl,)
#        html += "<td>commento</td></td>\n"

        html += "</table><table width=\"100"+'%'+"\">\n"
        html += "<h2>History Plots</h2>"
        html += "<tr><th align=\"left\">Service</th><th align=\"left\">Description</th></tr>\n"
        
#
# Pages:
        html += "<tr><td width=\"20"+'%'+"\">Scheduler-status: <a href=\"%s/hist_schedstat24\">24</a>/<a href=\"%s/hist_schedstat7\">7</a></td>\n" % (
            self.myUrl,self.myUrl,)
        html += "<td>History plot of number of job per different scheduler status for the last 24 hours or 7 days</td></td>\n"
#        
#         html += "<tr><td width=\"20"+'%'+"\"><a href=\"%s/hist_schedstat7\">Scheduler-status/7</a></td>\n" % (
#             self.myUrl,)
#         html += "<td>History plot of number of job per different scheduler status, hourly for the last week</td></td>\n"
        
        html += "<tr><td width=\"20"+'%'+"\">Process-status: <a href=\"%s/hist_procsstat24\">24</a>/<a href=\"%s/hist_procsstat7\">7</a></td>\n" % (
            self.myUrl,self.myUrl,)
        html += "<td>History plot of number of job per different process status for the last 24 hours or 7 days</td></td>\n"
#        
#         html += "<tr><td width=\"20"+'%'+"\"><a href=\"%s/hist_procsstat7\">Process-status/7</a></td>\n" % (
#             self.myUrl,)
#         html += "<td>History plot of number of job per different process status, hourly for the last week</td></td>\n"
#        
        html += "<tr><td width=\"20"+'%'+"\">HW monitor: <a href=\"%s/hist_HW_24\">24</a>/<a href=\"%s/hist_HW_7\">7</a></td>\n" % (
            self.myUrl,self.myUrl)
        html += "<td>History plot of cpu load, memory and swap usage for last 24 hours or 7 days</td></td>\n"
#        
        html += """</table>"""
        html +="<br/><h6>version "+os.environ['CRAB_SERVER_VERSION']+"</h6>"
        html += """</body></html>"""
        
#        html +="version "+os.environ['CRAB_SERVER_VERSION']
	return html
    index.exposed = True


class Downloader:
    """
    _Downloader_

    Serve files from the JobCreator Cache via HTTP

    """
    def __init__(self, rootDir):
        self.rootDir = rootDir

    def index(self, filepath):
        """
        _index_

        index response to download URL, serves the file
        requested

        """
        pathInCache = os.path.join(self.rootDir, filepath)
        logging.debug("Download Agent serving file: %s" % pathInCache)
        return serve_file(pathInCache, "application/x-download", "attachment")
    index.exposed = True


class ImageServer:

    def __init__(self, rootDir):
        self.rootDir = rootDir

    def index(self, filepath):
        pathInCache = os.path.join(self.rootDir, filepath)
        logging.debug("ImageServer serving file: %s" % pathInCache)
        return serve_file(pathInCache, content_type="image/png")
    index.exposed = True

class HTTPFrontendComponent:


    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args['HTTPLogfile'] = None
        self.args['Host'] = socket.gethostname()
        self.args['Port'] = 8888
        self.args['ThreadPool'] = 10
        self.args['JobCreatorCache'] = None
        self.args.update(args)

        [ self.args.__setitem__(x, int(self.args[x])) for x in [
            'Port', 'ThreadPool'] ]

        self.staticDir = os.path.join(self.args['ComponentDir'], "static")
        self.imageDir = os.path.join(self.args['ComponentDir'],"image")
	if not os.path.exists(self.staticDir):
            os.makedirs(self.staticDir)
        
	if not os.path.exists(self.imageDir):
            os.makedirs(self.imageDir)
	
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        if self.args['HTTPLogfile'] == None:
            self.args['HTTPLogfile'] = os.path.join(self.args['ComponentDir'],
                                                    "HTTPLog")

	


    def __call__(self, message, payload):
        """
        _operator(message, payload)_

        Respond to messages: No messages for this component

        """
        pass



    def startComponent(self):
        """
        _startComponent_

        Start up the cherrypy service for this component

        """
        cherrypy.config.update({'environment': 'production',
                                'log.error_file': self.args['HTTPLogfile'],
                                'log.screen': True})
        cherrypy.config.update({
        "global" : {
        "server.socket_host" :  self.args['Host'],
        "server.socket_port" :  self.args['Port'],
        "server.thread_pool" :  self.args['ThreadPool'],
        }})
        
        baseUrl = "http://%s:%s" % (
            self.args['Host'], self.args['Port'])
        
        
        root = Root(baseUrl)
        root.download = Downloader(self.args['JobCreatorCache'])
        root.images = ImageServer(self.staticDir)
#        root.workflowgraph = WorkflowGraph(
#            "%s/images" % baseUrl,
#            self.staticDir)
        
            
        root.task = TaskMonitor()
        root.retrieving = JobRetrievingMonitor()

#        root.datasets = DatasetMonitor(getLocalDBSURL())

#        root.resources = ResourceDetails()
#        root.resourcestate = ResourceStatus()
#        root.mergedataset = MergeMonitor()
#        root.mergedgraph = MergeGraph(
#            "%s/images" % baseUrl,
#            self.staticDir)
        root.resubmitting = JobResubmittingMonitor()
        statuses = ['Retrieved', 'Done', 'Cleared', 'Aborted', 'Running', 'Created', 'NULL', 'Scheduled']
        root.hist_schedstat24 = HistStatusMonitor(96,900,'status_scheduler',statuses)
        root.hist_schedstat7 = HistStatusMonitor(7*24,3600,'status_scheduler',statuses)
        statuses = ['not_handled','handled','failed','failure_handled','output_requested','in_progress','output_retrieved','processed', 'NULL']
        root.hist_procsstat7 = HistStatusMonitor(7*24,3600,'process_status',statuses)
        root.hist_procsstat24 = HistStatusMonitor(96,900,'process_status',statuses)
        root.hist_HW_24 = HistHWMonitor(480)   # 480 times 3min is 24 hours
        root.hist_HW_7 = HistHWMonitor(3360)   # 3360 times 3min is 7 days
#            )

        
        cherrypy.quickstart(root)
        
