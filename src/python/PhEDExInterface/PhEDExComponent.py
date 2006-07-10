#!/usr/bin/env python
"""
_PhEDExComponent_



"""

import os
from MessageService.MessageService import MessageService

import logging
from logging.handlers import RotatingFileHandler


                                                                               


class PhEDExComponent:
    """
    _PhEDExComponent_


    """
    def __init__(self, **args):
        self.args = {}

        self.args.setdefault("Logfile", None)
        self.args.update(args)

        if self.args['Logfile'] == None:
           self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        #  //
        # // Log Handler is a rotating file that rolls over when the
        #//  file hits 1MB size, 3 most recent files are kept
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        #  //
        # // Set up formatting for the logger and set the 
        #//  logging level to info level
        logFormatter = logging.Formatter("%(asctime)s:%(module)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        
        logging.info("PhEDExComponent Started...")
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)

        if event == "PhEDExInterface:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "PhEDExInterface:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        return


    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        # create message service
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("PhEDExInterface")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("PhEDExInterface:StartDebug")
        self.ms.subscribeTo("PhEDExInterface:EndDebug")
                                                                                
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("PhEDExComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
                                                                                

