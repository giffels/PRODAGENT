#!/usr/bin/env python
"""
_LoggingUtils_

Common logging setup for all components

"""

__revision__ = "$Id$"
__version__ = "$Revision$"

import logging
from logging.handlers import RotatingFileHandler

import os
from time import time, ctime
from MessageService.MessageService import MessageService

def installLogHandler(componentRef):
    """
    _installLogHandler_

    Setup the logging handlers for a component
    Read arguments from the component configuration for component by component customisation


    """

    logSize = componentRef.args.get("LogSize", 1000000)
    logSize = int(logSize)
    logRotate = componentRef.args.get("LogRotate", 3)
    logRotate = int(logRotate)

    defaultLevel = componentRef.args.get("LogLevel", "info")
    loggingLevel = logging.INFO

    # get RSS feed status for this component
    rss = str(componentRef.args.get("RssFeed", "no")).lower()
    rss = rss in ['yes', 'y']
      
    if defaultLevel.lower() == "debug":
        loggingLevel = logging.DEBUG
    
    logHandler = RotatingFileHandler(componentRef.args['Logfile'],
                                     "a", logSize, logRotate)
    logFormatter = logging.Formatter("%(asctime)s:%(message)s")
    logHandler.setFormatter(logFormatter)
    logging.getLogger().addHandler(logHandler)
    logging.getLogger().setLevel(loggingLevel)

    # add RSS feed abilities only if required
    if rss:
        rssFilter = RssFilter(componentRef)
        logging.getLogger().addFilter(rssFilter)

    return
    
##############################################################################
# RSS filter class
##############################################################################

class RssFilter(logging.Filter):

    """
    Implements a filter for logging that sends error and higher level log
    records to the RssFeeder component.
    """

    ##########################################################################
    # RSS filter initialization
    ##########################################################################

    def __init__(self, componentRef):
        """
        Arguments:

          componentRef -- a reference to the original component

        Return:

          none

        """

        # call base class init method
        logging.Filter.__init__(self)

        # get component name
        componentName = str(componentRef.__module__)
        dotPosition = componentName.index('.')
        if dotPosition > 0:
            name = componentName[0:dotPosition]
        else:
            name = componentName
        self.componentName = name

        # store reference to original component
        self.componentRef = componentRef

        # prepare directory
        self.baseDir = os.path.join(componentRef.args['ComponentDir'], 'RSS')
        if not os.path.exists(self.baseDir):
            os.makedirs(self.baseDir)
        self.counter = 0

    ##########################################################################
    # RSS filter
    ##########################################################################

    def filter(self, rec):

        """
        Arguments:

          rec -- the log record

        Return:

          true -- record processed

        """

        # only send to RSS all logs higher than warning level
        if rec.levelno <= logging.WARNING:
            return True

        # remove old RSS items (older than 10 minutes), should be
        # more than enough to allow the RssFeeder component to
        # get them
        items = [aFile for aFile in os.listdir(self.baseDir) \
                      if aFile.endswith('.txt')]
        for item in items:
            try:
                createdOn = float(item[0:item.index('-')])
            except ValueError:
                continue
            if createdOn < time() - 600:
                try:
                    os.remove(os.path.join(self.baseDir, item))
                # should not happen, but just in case, do not stop logging
                # due to a file that cannot be removed
                except OSError:
                    continue

        # file path
        filePath = os.path.join(self.baseDir, str(rec.created) + '-' + \
                                str(self.counter) + '.txt')
        self.counter = (self.counter + 1) % 1000

        # save it
        msg = str(rec.msg)
        try:
            aFile = open(filePath, 'w')
            aFile.write(msg)
            aFile.close()

        # errors should not happen, but just in case, do not stop logging
        except IOError:
            return True

        # build payload
        try:
            firstEndOfLine = msg.index('\n')
        except ValueError:
            firstEndOfLine = len(msg)
        if firstEndOfLine <= 80:
            description =  msg 
        else:
            description = msg[:80] + '...'
        description = description.replace("'","''").replace("::",":")
        title = str(rec.levelname) + ' generated on ' + ctime(time()) + \
                ' from ' + self.componentName
        payload = self.componentName + '::' + self.componentName + '::' + \
                  title + '::' + description + '::' + \
                  filePath

        # publish it
        self.publishRssItem(payload)

        # done
        return True

    ##########################################################################
    # publish message to RssFeeder
    ##########################################################################

    def publishRssItem(self, payload):

        """
        Arguments:

          payload -- the message

        Return:

          none

        """

        ms = MessageService()
        ms.registerAs("RssFeeder")
        ms.publish("RssFeeder:AddFile", payload)
        ms.commit()
        ms.close()
       

