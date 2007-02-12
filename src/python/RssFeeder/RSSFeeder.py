#!/usr/bin/env python
"""
_RSSFeeder_

Serve an RSS channel

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

# threads
from threading import Thread

# cherrypy
import cherrypy

##############################################################################
# RSSFeeder class
##############################################################################

class RSSFeeder(Thread):
    """
    _RSSFeeder_

    Provides access to RSS channel

    """

    ##########################################################################
    # RSSFeeder initialization
    ##########################################################################

    def __init__(self, condition, document, instance):
        """
        __init__

        Initialize instance variables

        Arguments:

          condition -- the condition variable used for synchronization
          document -- the document root
          instance -- PA instance name

        Return:

          none
        """

        Thread.__init__(self)
        self.cond = condition
        self.doc = document
        self.instance = instance

    ##########################################################################
    # RSSFeeder channel
    ##########################################################################

    def default(self, path):
        """
        __default__

        Provides RSS channel information.

        Arguments:

          path -- the RSS channel name 

        Return:

          none

        """
        # verify xml suffix
        if not path.endswith('.xml'):

            # not there, generate page not found
            raise cherrypy.NotFound

        # remove it
        path = path.replace('.xml','')

        # get keys
        self.cond.acquire()
        keys = self.doc.keys()
        self.cond.release()

        # verify existence of the requested key
        if not path in keys:

            # generate page not found
            raise cherrypy.NotFound

        # get document
        self.cond.acquire()
        document = self.doc[path].doc.toprettyxml()
        self.cond.release()

        # return it 
        return document

    # declare RSS feed exposed
    default.exposed = True

    ##########################################################################
    # provides a list of channels
    ##########################################################################

    def index(self):
        """
        __default__

        Provides the list of current RSS channels.

        Arguments:

          none

        Return:

          none

        """
     
        # prepare document
        document = "<html><body>\n" + \
                   "<h2>RSS channels provided by ProdAgent instance " + \
                   str(self.instance) + "</h2>\n" + \
                   "<table>\n" 
        
        # get keys
        self.cond.acquire()
        keys = self.doc.keys()
        self.cond.release()

        # create list of channels
        for channel in keys:
            document = document + '<tr><td> ' + str(channel) + '</td><td>' + \
                       '<a href="' + str(channel) + '.xml">' + \
                       '<img src="files/rss.gif">' + '</a></td></tr>\n'

        # close document
        document = document + "</table>\n</body>\n</html>\n"

        # return it
        return document

    # declare index exposed
    index.exposed = True

