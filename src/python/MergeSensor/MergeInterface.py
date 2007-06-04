#!/usr/bin/env python
"""
_MergeInterface_

Standard Interface for Merge plugins

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import logging

class MergeInterface:
    """
    _MergeInterface_

    Interface that define merge policy plugins behavior.

    """

    def __init__(self):
        pass

    ##########################################################################
    # select files for merging
    ##########################################################################
    
    def applySelectionPolicy(self, files, parameters, forceMerge = False):
        """
        _applySelectionPolicy_
        
        return the set of files to be merged
        
        Arguments:

          files -- the file set
          parameters -- a dictionary for policy parameters
          forceMerge -- True indicates that a set should be returned
                        even if merge conditions does not apply  

        Return:

          the list of files to be merged

        """

        msg = "Not Implemented: %s.applySelectionPolicy %s,%s,%s" % (
             self.__class__.__name__ ,
             files, parameters, forceMerge)
        logging.warning(msg)
        raise NotImplementedError, msg


