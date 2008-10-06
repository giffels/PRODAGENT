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

import ProdAgentCore.LoggingUtils as LoggingUtils





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

        baseUrl = "http://%s:%s" % (
            self.args['Host'], self.args['Port'])

        self.args['StaticDir'] = self.staticDir
        self.args['BaseUrl'] = baseUrl


        installerModule = self.args.get(
            "InstallerModule", "HTTPFrontend.ProductionTools")

        #Import module and call installer from it
        try:
            modRef = __import__(installerModule, globals(), locals(), "installer")
        except Exception, ex:
            msg = "Unable to load module:\n"
            msg += "%s\n" % installerModule
            msg += "Due to error:\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg

        installer = getattr(modRef, "installer", None)


        try:
            root = installer(**self.args)
        except Exception, ex:
            msg = "Unable to call installer\n"
            msg += str(ex)
            msg += "\n"
            import traceback
            msg += traceback.format_exc()
            logging.error(msg)
            raise RuntimeError(msg)

        try:
            cherrypy.quickstart(root)
        except Exception, ex:
            msg = "Error starting CherryPy:\n%s\n" % str(ex)
            logging.error(msg)
            cherrypy.engine.stop()
