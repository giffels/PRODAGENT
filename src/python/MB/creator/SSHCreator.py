#!/usr/bin/env python
"""
_SSHCreator_

Simple ssh based creation implementation using touch and
makedir to implement a creator for making files and
dirs on a remote machine via ssh

"""

import os
import popen2

from MB.creator.Creator import Creator
from MB.creator.CreatorException import CreatorException




class SSHCreator(Creator):
    """
    _SSHCreator_

    Creator implementation to create files and dirs
    on a local directory using os commands

    """
    def __init__(self):
        Creator.__init__(self)



    def createDir(self, mbInstance):
        """
        _createDir_

        Create a Directory based on the Target Directory AbsName
        in the mbInstance

        """
        absName = mbInstance['TargetAbsName']
        hostName = mbInstance['TargetHostName']
        self._CreateDir(hostName, absName, self._ExtractArgs(mbInstance))
        return
        
    def createFile(self, mbInstance):
        """
        _createFile_

        Create a file based on the Target AbsName and BaseNames
        provided in the metabroker instance
        """
        dirName = os.path.dirname(mbInstance['TargetAbsName'])
        hostName = mbInstance['TargetHostName']
        args = self._ExtractArgs(mbInstance)
        self._CreateDir(hostName, dirName, args)
        #  //
        # // Now create the file
        #//
        fileName = mbInstance['TargetAbsName']
        comm = "%s %s " % (args['SSHBinary'], args['SSHOptions'])
        comm += "%s \'touch %s\' " % (hostName, fileName)
        try:
            self.runShellCommand(comm)
        except CreatorException, ex:
            ex.addInfo(RemoteHost = hostName,
                       RemoteFile = fileName,
                       FMBInstance = mbInstance)
            raise
        return


    def _ExtractArgs(self, mbInstance):
        """
        _ExtractArgs_

        Internal method to generate an options dictionary based
        on any options provided in the MetaBroker instance
        """
        args = {}
        args['SSHBinary'] = mbInstance.get("SSHBinary",
                                           "ssh")
        args['SSHOptions'] = mbInstance.get("SSHOptions", "")
        return args
    
        

    def _CreateDir(self, hostName, dirName, args):
        """
        _CreateDir_

        Internal dir creation method used to build dirs for
        both createDir and createFile methods
        """
        comm = "%s %s " % (args['SSHBinary'], args['SSHOptions'])
        comm += "%s \'mkdir -p %s\' " % (hostName, dirName)
        try:
            self.runShellCommand(comm)
        except CreatorException, ex:
            ex.addInfo(RemoteHost = hostName,
                       RemoteDir = dirName)
            raise
        return



