#!/usr/bin/env python
"""
_RssFeeder_

Component that implements an RSS feeder for the components to submit  
important messages.

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import os
import inspect
from shutil import copyfile

# Message service import
from MessageService.MessageService import MessageService

# threads
from threading import Condition

# logging
import logging
from logging.handlers import RotatingFileHandler

# RSS channel
from RssFeeder.Channel import Channel
from RssFeeder.Feeder import Feeder

##############################################################################
# RssFeederComponent class
##############################################################################
                                                                                
class RssFeederComponent:
    """
    _RssFeederComponent_

    Component that provides an RSS channel 

    """

    ##########################################################################
    # RSS Feeder component initialization
    ##########################################################################

    def __init__(self, **args):
        """
        
        Arguments:
        
          args -- all arguments from StartComponent.
          
        Return:
            
          none

        """
       
        # get base directory
        thisModule = os.path.abspath(
                          inspect.getsourcefile(RssFeederComponent))
        baseDir = os.path.dirname(thisModule)
 
        # initialize the server
        self.args = {}
        self.args.setdefault("ComponentDir", baseDir)
        self.args.setdefault("ItemListLength", 100)
        self.args.setdefault("ProdAgentName", None)
        self.args.setdefault("Logfile", None)
        self.args.setdefault("Port", 8100)
 
        # update parameters
        self.args.update(args)

        # define log file
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'], 
                                                "ComponentLog")
        # create log handler
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)

        # define log format
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        # inital log information
        logging.info("RssFeeder starting...") 
        
        # message service instances
        self.ms = None 
        
        # set port number
        self.port = int(args['Port'])

        # set channel parameters
        Channel.setProdAgentInstance(self.args['ProdAgentName'])
        Channel.setItemListLength(self.args['ItemListLength'])
        Channel.setPort(self.port)
        Channel.setBaseDirectory(os.path.join(self.args['ComponentDir'], \
                                              'files'))

        # create channel structure
        self.channel = {}

        # create thread synchronization condition variable
        self.cond = Condition()

        # initialize feeder
        self.feeder = None

        # define working directories
        self.basePath = self.args['ComponentDir']
        self.filesPath = os.path.join(self.basePath, 'files')

        # create directories if not defined
        if not os.path.exists(self.filesPath):
            os.makedirs(self.filesPath)

        # copy rss logo
        rssLogo = os.path.join(baseDir, "rss.gif")
        targetPath = os.path.join(self.filesPath, "rss.gif")
        copyfile(rssLogo, targetPath)
        
        # get list of old channels
        channels = [aFile for aFile in os.listdir(self.filesPath) \
                      if aFile.endswith('.xml')]        

        # try to recreate channels 
        for channel in channels:

            logging.info('Restoring channel from %s' % channel)

            # get channel name
            name = os.path.basename(channel).replace('.xml', '')

            # create channel
            try:
                newChannel = Channel(name, fromFile=True)

            # display warning message if cannot be done
            except IOError, msg:
                logging.warning("Cannot restore channel from file %s: %s" % \
                                (str(channel), str(msg)))
                continue

            # add to channels
            self.channel[name] = newChannel

        # create main channel if not created before
        if not 'ProdAgent' in self.channel.keys():
            self.channel['ProdAgent'] = Channel('ProdAgent')

    ##########################################################################
    # handle events
    ##########################################################################

    def __call__(self, event, payload):
        """
        _operator()_

        Used as callback to handle events that have been subscribed to

        Arguments:
           
          event -- the event name
          payload -- its arguments
          
        Return:
            
          none
          
        """
        
        logging.debug("Received Event: %s" % event)
        logging.debug("Payload: %s" % payload)

        # add item event
        if event == "RssFeeder:AddItem":
            logging.info("AddItem: %s" % payload)
            self.addItem(payload)
            return

        # add file event
        if event == "RssFeeder:AddFile":
            logging.info("AddFile: %s" % payload)
            self.addFile(payload)
            return

        # start debug event
        if event == "RssFeeder:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return

        # stop debug event
        if event == "RssFeeder:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        # wrong event
        logging.debug("Unexpected event %s, ignored" % event)

    ##########################################################################
    # handle an RssFeeder:AddItem event
    ##########################################################################

    def addItem(self, item):
        """
        _addItem_

        Add an item to the feed.

        Arguments:
            
          item -- the new item to add
          
        Return:
            
          none
          
        """
      
        # get the channel, author, title and text
        data = item.split("::", 3)
        if len(data) != 4:
            logging.error("Wrong item: " + str(data))
            return

        channel, author, title, text = data

        # verify if the channel is new
        if not channel in self.channel.keys():

            # yes, then create it
            newChannel = Channel(channel)
            self.cond.acquire()
            self.channel[channel] = newChannel
            self.cond.release()

        # add item to the channel
        self.cond.acquire()
        self.channel[channel].addItem(author, title, text)
        self.cond.release()

        return

    ##########################################################################
    # handle an RssFeeder:AddFile event
    ##########################################################################

    def addFile(self, item):
        """
        _addFile_

        Add an item with file specification to the feed.

        Arguments:

          item -- the new file item to add

        Return:

          none

        """

        # get the channel, author, title, description and file name
        data = item.split("::", 4)
        if len(data) != 5:
            logging.error("Wrong item: " + str(data))
            return

        channel, author, title, description, path = data

        # verify that the file exists
        if not os.path.exists(path):
            logging.error("The file %s does not exist." % path)
            return

        # verify if the channel is new
        if not channel in self.channel.keys():

            # yes, then create it
            newChannel = Channel(channel)
            self.cond.acquire()
            self.channel[channel] = newChannel
            self.cond.release()

        # add item to the channel
        self.cond.acquire()
        try:
            self.channel[channel].addItem(author, title, description, path)
        except IOError, msg:
            logging.error(msg)
        self.cond.release()

        return
   
    ##########################################################################
    # start component execution
    ##########################################################################

    def startComponent(self):
        """
        _startComponent_

        Fire up the two main threads
        
        Arguments:
        
          none
          
        Return:
        
          none

        """
       
        # create message service instance
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("RssFeeder")
        
        # subscribe to messages
        self.ms.subscribeTo("RssFeeder:AddItem")
        self.ms.subscribeTo("RssFeeder:AddFile")

        # try to start the feeder
        try:
            self.feeder = Feeder(self.cond, self.channel, self.port, \
                                 self.args['ProdAgentName'], \
                                 self.args['ComponentDir'])
            self.feeder.start()
        except Exception, msg:
            logging.error("Cannot start Feeder. The component will run " + \
                          "but without feeding abilities: " + \
                          str(msg))
 
        # wait for messages
        while True:
            
            # get a single message
            messageType, payload = self.ms.get()

            # commit the reception of the message
            self.ms.commit()
            
            # perform task
            self.__call__(messageType, payload)
            
        
    ##########################################################################
    # get version information
    ##########################################################################

    @classmethod
    def getVersionInfo(cls):
        """
        _getVersionInfo_
        
        return version information of all components used by
        the MergeSensor
        """
        
        return "RssFeeder: " + __version__ + "\n"
   
