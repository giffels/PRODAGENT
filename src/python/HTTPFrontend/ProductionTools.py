#!/usr/bin/env python
"""
_ProductionTools_

Installer for production monitoring tools

"""

from ProdAgentCore.Configuration import prodAgentName
from ProdAgentCore.Configuration import loadProdAgentConfiguration


from HTTPFrontend.WorkflowMonitor import WorkflowMonitor,WorkflowGraph
from HTTPFrontend.JobQueueMonitor import JobQueueMonitor
from HTTPFrontend.MergeMonitor import MergeDatasetMonitor, MergeMonitor, MergeGraph
from HTTPFrontend.DatasetsMonitor import DatasetMonitor
from HTTPFrontend.ResourceMonitors import ResourceDetails,ResourceStatus
from HTTPFrontend.LogViewer import LogViewer
from HTTPFrontend.AlertMonitor import AlertMonitor, CurrentAlert, HistoryAlert
from HTTPFrontend.ConfDBEmulator import ConfDBEmulator
from HTTPFrontend.ComponentStatus import ComponentStatus,ComponentLogs,WriteLog,PostMorten

from cherrypy.lib.static import serve_file

import logging
import os

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
        html = """<html><body><h2>ProdAgent Instance: %s </h2>\n """ % (

            prodAgentName(), )

        html += "<table>\n"
        html += "<tr><th>Service</th><th>Description</th></tr>\n"
        html += "<tr><td><a href=\"%s/workflows\">Workflows</a></td>\n" % (
            self.myUrl,)
        html += "<td>Workflow Entities data in this ProdAgent</td></td>\n"

        html += "<tr><td><a href=\"%s/merges\">Merges</a></td>\n" % (
            self.myUrl,)
        html += "<td>Merge Subsystem data in this ProdAgent</td></td>\n"
        html += "<tr><td><a href=\"%s/jobqueue\">JobQueue</a></td>\n" % (
            self.myUrl,)
        html += "<td>Job Queue state in this ProdAgent</td></td>\n"
        html += "<tr><td><a href=\"%s/resources\">ResourceMonitor</a></td>\n" % (
            self.myUrl,)
        html += "<td>Resource information for this ProdAgent</td></td>\n"

        html += "<tr><td><a href=\"%s/logs\">Logs</a></td>\n" % (
            self.myUrl,)
        html += "<td>Production logs</td></td>\n"

        html += "<tr><td><a href=\"%s/alertmonitor\">AlertMonitor</a></td>"%(self.myUrl) + "<td>Alerts published by prodagent components</td></tr>"
        html += "<tr><td><a href=\"%s/confdbemu\">ConfDBEmulator</a></td>"%(self.myUrl) + "<td>ConfDB Emulator</td></tr>"
        html += "<tr><td><a href=\"%s/componentstatus\">ComponentStatus</a></td>"%(self.myUrl)
        html += "<td>ProdAgent Components Status</td></tr>\n"
        html += """</table></body></html>"""
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


def installer(**args):
    """
    _installer_



    """


    baseUrl = args['BaseUrl']

    root = Root(baseUrl)
    root.download = Downloader(args['JobCreatorCache'])
    root.images = ImageServer(args['StaticDir'])

    root.workflowgraph = WorkflowGraph(
        "%s/images" % baseUrl,
        args["StaticDir"])


    root.workflows = WorkflowMonitor(
        "%s/workflowgraph" % baseUrl
        )
    root.jobqueue = JobQueueMonitor()
    root.datasets = DatasetMonitor(getLocalDBSURL())

    root.resources = ResourceDetails()
    root.resourcestate = ResourceStatus()
    root.mergedataset = MergeMonitor()
    root.mergedgraph = MergeGraph(
        "%s/images" % baseUrl,
        args['StaticDir'])
    root.merges = MergeDatasetMonitor(
        "%s/mergedataset" % baseUrl,
        "%s/mergedgraph" % baseUrl,
        "%s/datasets" % baseUrl
        )
    root.alertmonitor = AlertMonitor(baseUrl)
    root.alertmonitor.currentalert = CurrentAlert (baseUrl)
    root.alertmonitor.historyalert=HistoryAlert(baseUrl)

    root.confdbemu = ConfDBEmulator()

    root.logs = LogViewer()

    root.writelog = WriteLog()
    root.complog = ComponentLogs(
        "%s/writelog" % baseUrl
        )
    root.postmorten = PostMorten(
        "%s/writelog" % baseUrl
        )
    root.componentstatus = ComponentStatus(
        "%s/complog" % baseUrl,
        "%s/postmorten" % baseUrl
        )

    return root
