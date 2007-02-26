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
    # File request initialization
    ##########################################################################

    def __init__(self, baseDir):
        """
        __init__

        Initialize instance variables

        Arguments:

          none

        Return:

          none
        """

        Thread.__init__(self)

        # set base directory
        self.baseDirectory = os.path.join(baseDir, 'files')

    ##########################################################################
    # File request
    ##########################################################################

    def default(self, *path):
        """
        __default__

        Provides the file.

        Arguments:

          path -- the path of the file requested 

        Return:

          none

        """

        # compute virtual path
        virtualPath = '/'.join(path)

        # get real path
        path = os.path.join(self.baseDirectory, virtualPath)

        # verify existence of the requested file
        if not os.path.exists(path):

            # generate page not found
            raise cherrypy.NotFound

        # determine type
        if path.endswith('.html'):
            contentType = 'text/html'
        elif path.endswith('.gif'):
            contentType = 'image/gif'
        elif path.endswith('.txt'):
            contentType = 'text/plain'
        else:
            contentType = 'unknown'

        # return it 
        return serve_file(path, contentType)

    # declare default exposed
    default.exposed = True

