#!/usr/bin/env python
"""
_DLSComponent_

Skeleton implementation of the DLSComponent for the ProdAgentLite.

Subscribes to a single event called NewFileBlock, and creates
a new DLS file block based on the payload.

At present it is still in discussion where this event will actually
come from, and I am not too clued in on what the details will be,
so if someone in the know can flesh out the requirements for this
component it would be useful -- Dave.

"""

__version__ = "$Revision: 1.3 $"
__revision__ = "$Id: DLSComponent.py,v 1.3 2006/03/08 15:19:42 ckavka Exp $"

import os
import socket
import DLS
import logging
from logging.handlers import RotatingFileHandler

from MessageService.MessageService import MessageService

class DLSComponent:
    """
    _DLSComponent_

    ProdAgentLite Component for adding DLS File Blocks

    """
    def __init__(self, **args):
        self.args = {}
        self.args['DLSAddress'] = None
        self.args['DLSType'] = None
        self.args['Logfile'] = None
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
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        
        logging.info("DLSComponent Started...")


    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events

        """
        logging.debug("Event: %s %s" % (event, payload))
        if event == "NewFileBlock":
            self.newFileBlock(payload)
            return
        if event == "DLSInterface:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "DLSInterface:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        return 


    def newFileBlock(self, fileBlockInfo):
        """
        _newFileBlock_

        create a new DLS replica: fileblock <-> location

        The fileBlockInfo argument should be a dictionary like: 
           {'fileblock': fileblock , 'location': location}
        """
        block=fileBlockInfo['fileblock']
        blocklocation=fileBlockInfo['location']
        logging.info("Creating New DLS replica : mapping fileblock %s to location %s" %(block, blocklocation))
 
        ### Contact DLS using the DLS class
        try:
            dlsinfo=DLS.DLS()

            # insert replica
            try:
                dlsinfo.addReplica(block,blocklocation)
            except DLS.DLSCLIError, ex:
                logging.error(
                    "Caught exception %s: \n %s" % (
                    ex.getClassName(), ex.getErrorMessage(),
                    )
                    )
              

        except DLS.DLSConfigError, ex:
            logging.error(ex.getErrorMessage())
            raise RuntimeError, ex.getErrorMessage()

        return
        

    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
     
        # create message service
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("DLSComponent")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("NewFileBlock")
        self.ms.subscribeTo("DLSInterface:StartDebug")
        self.ms.subscribeTo("DLSInterface:EndDebug")
        
        
        # wait for messages
        while True:
            type, payload = self.ms.get()
            logging.debug("DLSComponent: %s %s" % ( type, payload))
            self.__call__(type, payload)
            self.ms.commit()

