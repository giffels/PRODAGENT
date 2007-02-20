#!/usr/bin/env python
"""
_Feeder_

Startup a web server wich provides access to the RSS channel 

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

# threads
from threading import Thread

# cherrypy
import cherrypy

# RSS feeder
from RssFeeder.RSSFeeder import RSSFeeder
from RssFeeder.FileRequest import FileRequest

##############################################################################
# Feeder class
##############################################################################

class Feeder(Thread):
    """
    _Feeder_

    Provides access to RSS channel

    """

    ##########################################################################
    # Feeder initialization
    ##########################################################################

    def __init__(self, condition, documents, port, instance, baseDir):
        """
        __init__

        Initialize thread and set instance variables

        Arguments:

          condition -- the condition variable used for synchronization
          documents -- the xml documents
          port -- the port used to provide RSS info
          instance -- PA instance name
          baseDir -- the base directory

        Return:

          nothing

        """

        # initialize thread 
        Thread.__init__(self)

        # initialize instance variables
        self.cond = condition
        self.doc = documents
        self.port = port
        self.instance = instance
        self.baseDirectory = baseDir

    ##########################################################################
    # thread main body
    ##########################################################################

    def run(self):
        """
        __run__

        Serve RSS channel 

        Arguments:

          none

        Return:

          none

        """

        # configure cherrypy
        configuration = { 'global':  { \
                             'server.socket_port' : self.port, \
                             'server.thread_pool' : 10 }, \
                           'log': { \
                             'log.screen' : False }, \
                        }

        # create pages
        root = RSSFeeder(self.cond, self.doc, self.instance)
        root.files = FileRequest(self.baseDirectory)

        # start it
        cherrypy.quickstart(root, config = configuration)

        
