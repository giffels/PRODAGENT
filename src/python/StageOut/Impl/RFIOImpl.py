#!/usr/bin/env python
"""
_RFIOImpl_

Implementation of StageOutImpl interface for RFIO

"""
import os 
from StageOut.Registry import registerStageOutImpl
from StageOut.StageOutImpl import StageOutImpl

from StageOut.Execute import execute


class RFIOImpl(StageOutImpl):
    """
    _SRMImpl_

    Implement interface for srmcp command
    
    """
    executeCommand = staticmethod(execute)

    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

         uses pfn

        """
        return "%s" % pfn

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        create dir with group permission
        """
        command = "rfmkdir -m 775 -p %s" % os.path.dirname(targetPFN)
        self.executeCommand(command)

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an rfcp command

        """
        result = "rfcp "
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s " % targetPFN
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "rfrm %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("rfio", RFIOImpl)
