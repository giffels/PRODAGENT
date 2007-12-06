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
import ProdAgentCore.LoggingUtils as LoggingUtils

from HTTPFrontend.WorkflowMonitor import WorkflowMonitor
from HTTPFrontend.JobQueueMonitor import JobQueueMonitor
from HTTPFrontend.MergeMonitor import MergeDatasetMonitor, MergeMonitor, MergeGraph



class Root:
    """
    _Root_

    Main index page for the component, will appear as the index page
    of the toplevel address

    """
    def index(self):
        html = """<html><body><h2>ProdAgent Instance: %s </h2> """ % (
            
            prodAgentName(), )
        html += """</body></html>"""
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
        if not os.path.exists(self.staticDir):
            os.makedirs(self.staticDir)
        
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
        
        
        
        root = Root()
        root.download = Downloader(self.args['JobCreatorCache'])
        root.images = ImageServer(self.staticDir)
        root.workflows = WorkflowMonitor()
        root.jobqueue = JobQueueMonitor()
        root.mergedataset = MergeMonitor()
        root.mergedgraph = MergeGraph(
            "http://%s:%s/images" % (
            self.args['Host'], self.args['Port']),
            self.staticDir)
        root.merges = MergeDatasetMonitor(
            "http://%s:%s/mergedataset" % (
            self.args['Host'], self.args['Port']),
            "http://%s:%s/mergedgraph" % (
            self.args['Host'], self.args['Port'])
            

            )

        
        cherrypy.quickstart(root)
        
