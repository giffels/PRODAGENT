#!/usr/bin/env python
"""
_LocalCreator_

Simple local creation implementation using touch and
makedir to implement a creator for making files and
dirs in locally mounted directories

"""

import os

from MB.creator.Creator import Creator
from MB.creator.CreatorException import CreatorException




class LocalCreator(Creator):
    """
    _LocalCreator_

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
        self._CreateDir(absName)
        return
        
    def createFile(self, mbInstance):
        """
        _createFile_

        Create a file based on the Target AbsName and BaseNames
        provided in the metabroker instance
        """
        dirName = os.path.dirname(mbInstance['TargetAbsName'])
        self._CreateDir(dirName)
        fileName = mbInstance['TargetAbsName']
        try:
            handle = open(fileName, 'w')
            handle.close()
        except IOError, ex:
            msg = "Unable to create File:\n%s\n" % fileName
            msg += str(ex)
            raise CreatorException(msg, ClassInstance = self,
                                   FileName = fileName)
        return


    def _CreateDir(self, dirName):
        """
        _CreateDir_

        Internal dir creation method used to build dirs for
        both createDir and createFile methods
        """
        try:
            if not os.path.exists(dirName):
                os.makedirs(dirName)
        except IOError, ex:
            msg = "Unable to create Directory:\n%s\n" % dirName
            msg += str(ex)
            raise CreatorException(msg, ClassInstance = self,
                                   DirectoryName = dirName)
        except OSError, ex:
            msg = "Unable to create Directory:\n%s\n" % dirName
            msg += str(ex)
            raise CreatorException(msg, ClassInstance = self,
                                   DirectoryName = dirName)
        return



