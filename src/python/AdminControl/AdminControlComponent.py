#!/usr/bin/env python
"""
_AdminControlComponent_

Component that provides a window into the prodAgent itself
via an XMLRPC Interface for publishing events into the system
and also status information

"""

import os
import logging

from logging.handlers import RotatingFileHandler
from threading import Thread

from AdminControl.AdminControlServer import AdminControlServer

from MessageService.MessageService import MessageService

class XMLRPCServerThread(Thread):
    """
    Thread that runs XMLRPC Server  
    """

    def __init__(self, serverInstance):
        """
        __init__

        Initialize thread and set polling callback
        """
        Thread.__init__(self)
        self.server = serverInstance

    def run(self):
        """
        __run__

        Performs polling on DBS
        """
        self.server.serve_forever()


class AdminControlComponent:
    """
    _AdminControlComponent_

    ProdAgent component that provides monitoring and XMLRPC API interface
    for monitoring and interacting with the live ProdAgent

    """
    def __init__(self, **args):
        self.args = {}
        self.args['AdminControlHost'] = "127.0.0.1"
        self.args['AdminControlPort'] = 8081
        self.args['Logfile'] = None
        self.args.update(args)
        self.args['AdminControlPort'] = int(self.args['AdminControlPort'])
        self.server = AdminControlServer(self.args['AdminControlHost'],
                                         self.args['AdminControlPort'])
        
      
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        logging.info("AdminControl Component Started...")                                

    def __call__(self, event, payload):
        """
        _operator()_

        Event handler method

        """
        if event == "AdminControl:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "AdminControl:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        
    def startComponent(self):
        """
        _startComponent_

        """
        self.ms = MessageService()
        # register
        self.ms.registerAs("AdminControl")
        
        # subscribe to messages
        self.ms.subscribeTo("AdminControl:StartDebug")
        self.ms.subscribeTo("AdminControl:EndDebug")
        
        self.thread = XMLRPCServerThread(self.server)
        self.thread.start()
        
        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            self.__call__(messageType, payload)
            self.ms.commit()
        

if __name__ == '__main__':
    comp = AdminControlComponent()
    comp.startComponent()
