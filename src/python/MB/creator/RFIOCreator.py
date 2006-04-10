#!/usr/bin/env python
"""
_RFIOCreator_

Creator Implementation for creating dirs via rfmkdir command

TODO: Add file creation implementation

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: RFIOCreator.py,v 1.1 2005/12/30 18:51:39 evansde Exp $"
__author__ = "evansde@fnal.gov"


import os

from MB.creator.Creator import Creator
from MB.creator.CreatorException import CreatorException



class RFIOCreator(Creator):
    """
    _RFIOCreator_

    Implement creation via rfio commands

    NOTE: !!!File creation has not yet been implemented!!!
    
    """

    
    def __init__(self):
        Creator.__init__(self)


    def createDir(self):
        """
        _createDir_

        Create a dir via RFIO rfmkdir command

        """
        binary = mbInstance.get("RfmkdirBinary", "rfmkdir")
        options = mbInstance.get("RfmkdirOptions", "-p")
        
        command = "%s %s " % (binary, options)
        if mbInstance['TargetHostName'] != None:
            command += "%s:" % mbInstance['TargetHostName']

        command += "%s"  % mbInstance['TargetAbsName']

        try:
            self.runShellCommand(command)
        except CreatorException, ex:
            ex.addInfo(RemoteDirName = mbInstance['TargetAbsName'],
                       RemoteHostName = mbInstance['TargetHostName'],
                       DMBInstance = mbInstance)
            raise
        return
    

