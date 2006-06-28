#!/usr/bin/env python
"""
_SRMImpl_

Implementation of StageOutImpl interface for SRM

"""

from StageOut.Registry import registerStageOutImpl
from StageOut.StageOutImpl import StageOutImpl



class SRMImpl(StageOutImpl):
    """
    _SRMImpl_

    Implement interface for srmcp command
    
    """
    
    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        return "file:///%s" % pfn

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an srmcp command

        """
        result = "srmcp "
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
        print "NOT IMPLEMENTED CLEANUP FOR SRM YET"
        command = "echo \"cleaning up %s\"" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("srm", SRMImpl)
