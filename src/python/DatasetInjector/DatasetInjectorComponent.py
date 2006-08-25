#!/usr/bin/env python
"""
_DatasetInjector_

Generate a set of JobSpecs to consume a dataset and inject them into
the ProdAgent.

Note: This component is potentially capable of creating LOTS of jobs
if the dataset is large.

"""


__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "evansde@fnal.gov"


import os
import logging
from logging.handlers import RotatingFileHandler


from MessageService.MessageService import MessageService


class DatasetInjectorComponent:
    """
    _DatasetInjectorComponent_

    Component to generate JobSpecs based on DBS/DLS information for a
    dataset

    """
    def __init__(self, **args):
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args.update(args)
        
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        self.ms = None
        logging.info("DatasetInjector Component Started")



    def __call__(self, event, payload):
        """
        _operator()_

        Define call for this object to allow it to handle events that
        it is subscribed to
        """
        logging.debug("Event: %s Payload: %s" % (event, payload))
        if event == "DatasetInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "DatasetInjector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        
        
        return





    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component

        """                                   
        # create message service
        self.ms = MessageService()

        # register
        self.ms.registerAs("DatasetInjector")
        
        # subscribe to messages
        
        self.ms.subscribeTo("DatasetInjector:StartDebug")
        self.ms.subscribeTo("DatasetInjector:EndDebug")
        
        # wait for messages
        while True:
            msgtype, payload = self.ms.get()
            self.ms.commit()
            self.__call__(msgtype, payload)

        
