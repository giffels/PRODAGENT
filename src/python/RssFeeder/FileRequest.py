#!/usr/bin/env python
"""
_FileRequest_

Serve a static file request

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import os

# threads
from threading import Thread

# cherrypy
import cherrypy
from cherrypy.lib.static import serve_file

##############################################################################
# RSSFeeder class
##############################################################################

class FileRequest(Thread):
    """
    _FileRequest_

    Returns a file

    """

    ##########################################################################
    # RSSFeeder initialization
    ##########################################################################

    def __init__(self):
        """
        __init__

        Initialize instance variables

        Arguments:

          none

        Return:

          none
        """

        Thread.__init__(self)

        # get current directory
        self.currentdir = os.path.dirname(os.path.abspath(__file__))

    ##########################################################################
    # File request
    ##########################################################################

    def default(self, path):
        """
        __default__

        Provides the file.

        Arguments:

          path -- the file requested 

        Return:

          none

        """

        # remove it
        path = os.path.join(self.currentdir, path)

        # verify existence of the requested file
        if not os.path.exists(path):

            # generate page not found
            raise cherrypy.NotFound

        # return it 
        return serve_file(path, content_type='image/gif')

    # declare default exposed
    default.exposed = True

