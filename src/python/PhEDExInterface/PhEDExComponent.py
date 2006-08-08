#!/usr/bin/env python
"""
_PhEDExComponent_



"""

import os
from MessageService.MessageService import MessageService

import logging
from logging.handlers import RotatingFileHandler


from PhEDExInterface.DBSDLSToolkit import DBSDLSToolkit
from PhEDExInterface.InjectionSpec import InjectionSpec


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
        logging.getLogger().setLevel(logging.DEBUG)
        
        logging.info("PhEDExComponent Started...")

        
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)

        if event == "MergeRegistered":
            self.handleMergeRegistered(payload)
            return

        if event == "PhEDExInterface:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "PhEDExInterface:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        return


    def handleMergeRegistered(self, datasetName):
        """
        _handleMergeRegistered_

        Get the fileblock details for the dataset provided and generate
        the XML Spec for PhEDEx injection.
        
        """
        logging.info("Handling MergeRegistered Event for Dataset:\n %s" % (
            datasetName,)
                     )
        dbsdls = DBSDLSToolkit()

        blocks = dbsdls.listFileBlocksForDataset(datasetName)
        
        #  //
        # // Write the file into the component Dir.
        #//  
        xmlFile = "%s/%s.xml" % (self.args['ComponentDir'],
                                 datasetName.replace("/", "_"))
        
        logging.info("InjectionSpec: %s" % xmlFile)
        
        #  //
        # // Instantiate an InjectionSpec object
        #//
        #  //
        # // Note: No info on wether the dataset is closed or transient
        #//  is provided here yet.
        spec = InjectionSpec(
            dbsdls.dbsName(),
            datasetName,
            )

        #  //
        # // add the block information to the spec
        #//
        blockLocations = {}
        for block in blocks:
            blockEntry = spec.getFileblock(block['blockName'])
            #  //
            # // Grab the se-name from DLS
            #//
            locations = dbsdls.getFileBlockLocation(block['blockName'])
            blockLocations[block['blockName']] = locations
            #  //
            # // Add each file to the fileBlock.
            #//
            for fileEntry in block['fileList']:
                blockEntry.addFile(fileEntry['logicalFileName'],
                                   fileEntry['checkSum'],
                                   fileEntry['fileSize'])

        #  //
        # // OK, so have here an xml File containing the information 
        #//  required for doing the injection and
        #  //a map of fileBlock name to se-names to be used to map
        # // to the PhEDEx node name as an interim solution.
        #//
        spec.write(xmlFile)
        print blockLocations
        
        
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
        self.ms.subscribeTo("MergeRegistered")
                                                                                
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("PhEDExComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
                                                                                

