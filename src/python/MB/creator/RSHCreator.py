#!/usr/bin/env python
"""
_RSHCreator_

Simple rsh based creation implementation using touch and
makedir to implement a creator for making files and
dirs on a remote machine via rsh

"""

import os
import popen2

from MB.creator.Creator import Creator
from MB.creator.CreatorException import CreatorException



class RSHCreator(Creator):
    """
    _RSHCreator_

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
        
        #  //
        # // Now create the file
        #//
      
        fileName = mbInstance['TargetAbsName']
        comm = "%s %s " % (args['RSHBinary'], args['RSHOptions'])
        comm += "%s \'touch %s\' " % (hostName, fileName)
        pop = popen2.Popen4(comm)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        output = pop.fromchild.read()
        del pop
        if exitCode:
            msg = "Unable to create File on remote host via rsh\n"
            msg += "Host: %s\n" % hostName
            msg += "File: %s\n" % fileName
            msg += "Command:\n%s\n" % comm
            msg += "Exited with Status: %s\n" % exitCode
            msg += "Output:\n%s\n" % output
            raise CreatorException(msg, ClassInstance = self,
                                   FileName = fileName)
        return


    def _ExtractArgs(self, mbInstance):
        """
        _ExtractArgs_

        Internal method to generate an options dictionary based
        on any options provided in the MetaBroker instance
        """
        args = {}
        args['RSHBinary'] = mbInstance.get("RSHBinary",
                                           "rsh")
        args['RSHOptions'] = mbInstance.get("RSHOptions", "")
        return args
    
        

    def _CreateDir(self, hostName, dirName, args):
        """
        _CreateDir_

        Internal dir creation method used to build dirs for
        both createDir and createFile methods
        """
        comm = "%s %s " % (args['RSHBinary'], args['RSHOptions'])
        comm += "%s \'mkdir -p %s\' " % (hostName, dirName)
        pop = popen2.Popen4(comm)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        output = pop.fromchild.read()
        del pop
        if exitCode:
            msg = "Error creating directory on remote host via rsh\n"
            msg += "Host: %s\n" % hostName
            msg += "Directory: %s\n" % dirName
            msg += "Command:\n%s\n" % comm
            msg += "Exited with Status: %s\n" % exitCode
            msg += "Output:\n%s\n" % output
            raise CreatorException(msg, ClassInstance = self,
                                   DirName = dirName)
        return



