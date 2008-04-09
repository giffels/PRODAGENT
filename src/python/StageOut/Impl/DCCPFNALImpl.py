#!/usr/bin/env python
"""
_DCCPFNALImpl_

Implementation of StageOutImpl interface for DCCPFNAL

"""
import os
from StageOut.Registry import registerStageOutImpl
from StageOut.StageOutImpl import StageOutImpl


_CheckExitCodeOption = True



class DCCPFNALImpl(StageOutImpl):
    """
    _DCCPFNALImpl_

    Implement interface for srmcp command
    
    """
    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        Create a dir for the target pfn by translating it to
        a /pnfs name and calling mkdir

        PFN will be of the form:
        dcap://cmsdca.fnal.gov:22125/pnfs/fnal.gov/usr/cms/WAX/11/store/blah

        We need to convert that into /pnfs/cms/WAX/11/store/blah, as it
        will be seen from the worker node
        
        """
        # only create dir on remote storage
        if not targetPFN.find('/pnfs/'):
            return
        
        pfnSplit = targetPFN.split("WAX/11/store/", 1)[1]
        filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
        directory = os.path.dirname(filePath)
        command = "#!/bin/sh\n"
        command += "if [ ! -e \"%s\" ]; then\n" % directory 
        command += "  mkdir -p %s\n" % directory
        command += "fi\n"
        self.executeCommand(command)
        
        
    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        dccp takes a local path, so all we have to do is return the
        pfn as-is

        """
        return pfn

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build a dccp command with a pnfs mkdir to generate the directory

        """
        optionsStr = ""
        if options != None:
            optionsStr = str(options)
        dirname = os.path.dirname(targetPFN)
        result = "#!/bin/sh\n"
        result += "dccp %s %s %s" % ( optionsStr, sourcePFN, targetPFN)
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        pfnSplit = pfnToRemove.split("WAX/11/store/", 1)[1]
        filePath = "/pnfs/cms/WAX/11/store/%s" % pfnSplit
        command = "rm -f %s" % pfnToRemove
        self.executeCommand(command)
        

registerStageOutImpl("dccp-fnal", DCCPFNALImpl)
