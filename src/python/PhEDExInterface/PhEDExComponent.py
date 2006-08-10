#!/usr/bin/env python
"""
_PhEDExComponent_



"""

import os
import time
import popen2
from MessageService.MessageService import MessageService

import logging
from logging.handlers import RotatingFileHandler


from PhEDExInterface.DBSDLSToolkit import DBSDLSToolkit
from PhEDExInterface.InjectionSpec import InjectionSpec
from PhEDExInterface.PhEDExNodeMap import PhEDExNodeMap

_NodeMap = PhEDExNodeMap()

class PhEDExComponent:
    """
    _PhEDExComponent_


    """
    def __init__(self, **args):
        self.args = {}

        self.args.setdefault("Logfile", None)
        self.args.setdefault("PhEDExDropBox", None)
        self.args.setdefault("FailedDrops", None)
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

        #  //
        # // Directories used by this component
        #//
        if self.args['PhEDExDropBox'] == "None":
            self.args['PhEDExDropBox'] = None
        if self.args['PhEDExDropBox'] == None:
            msg = "PhEDExDropBox is no provided for PhEDExInterface"
            logging.warning(msg)
        else:
            if not os.path.exists(self.args['PhEDExDropBox']):
                msg = "PhEDExDropBox dir not found:\n"
                msg += "%s\n" % self.args['PhEDExDropBox']
                logging.warning(msg)
        if self.args['FailedDrops'] == None:
            self.args['FailedDrops'] = os.path.join(self.args['ComponentDir'],
                                                    "FailedDrops")
        if not os.path.exists(self.args['FailedDrops']):
            os.makedirs(self.args['FailedDrops'])
        
        
                              
        
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
        
        uniqueDir = "%s/%s-%s" % (self.args['ComponentDir'],
                                  datasetName.replace("/", "_"),
                                  time.time())
        if not os.path.exists(uniqueDir):
            os.makedirs(uniqueDir)
        
        xmlFile = "%s/%s.xml" % (uniqueDir,
                                 datasetName.replace("/", "_"))
        nodeFile = "%s/PhEDEx-Nodes.txt" % uniqueDir
        optionsFile = "%s/Options.txt" % uniqueDir
        goFile = "%s/go" % uniqueDir
        
        logging.info("InjectionSpec: %s" % xmlFile)
        logging.info("Nodes List: %s" % nodeFile)
        
        #  //
        # // Instantiate an InjectionSpec object
        #//
        #  //
        # // Note: No info on wether the dataset is closed or transient
        #//  is provided here yet.
        spec = InjectionSpec(
            dbsdls.dbsName(),
            dbsdls.dlsName(),
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

        spec.write(xmlFile)

        #  //
        # // create the nodes list file
        #//
        nodesList = []
        for key in blockLocations.keys():
            for seName in blockLocations[key]:
                nodeName =  _NodeMap.translateSE(seName)
                if nodeName == None:
                    logging.error("No PhEDExNode Map for SE: %s" % seName)
                    continue
                if nodeName not in nodesList:
                    nodesList.append(nodeName)

        handle = open(nodeFile, 'w')
        for node in nodesList:
            handle.write("%s\n" % node)
        handle.close()


        #  //
        # // create the options file
        #//
        handle = open(optionsFile, 'w')
        handle.write("!strict\n")
        handle.close()

        #  //
        # // create the (empty) go file
        #//
        handle = open(goFile, 'w')
        handle.write("")
        handle.close()

        #  //
        # // dispatch the directory to PhEDEx
        #//
        self.sendToDropBox(uniqueDir)
        return
        
        
    def sendToDropBox(self, dirName):
        """
        _sendToDropBox_

        Move the directory dirName to the PhEDEx Drop Box.

        """
        logging.info("sendToDropBox:%s" % dirName)

        targetDir = self.args['PhEDExDropBox']
        testExists = os.path.exists(self.args['PhEDExDropBox'])
        logging.debug("PhEDExDropBox exists: %s" % testExists)
        if not testExists:
            msg = "PhEDExDropBox doesnt exist,\n"
            msg += " moving to failure dir instead\n"
            msg += "Drop: %s\n" % dirName
            msg += "Failure Dir: %s\n" % self.args['FailedDrops']
            logging.error(msg)
            targetDir = "%s/%s" % (self.args['FailedDrops'],
                                   os.path.basename(dirName))
            

        command = "/bin/mv  %s %s" % (dirName, targetDir)
        logging.debug("sendToDropBox: %s " % command)
        
        pop = popen2.Popen3(command)
        output = pop.fromchild.read()
        pop.wait()
        exitCode = pop.poll()
        if exitCode:
            msg = "Error moving Drop to DropBox:\n"
            msg += "Drop: %s\n" % dirName
            msg += "Drop Box : %s\n" % targetDir
            msg += str(output)
            logging.error(msg)
            return
        logging.info("Drop moved successfully")
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
                                                                                

