#!/usr/bin/env python
"""
_DCCPFNALImpl_

Implementation of StageOutImpl interface for DCCPFNAL

"""
import os
import commands
from StageOut.Registry import registerStageOutImpl
from StageOut.StageOutImpl import StageOutImpl


_CheckExitCodeOption = True



class DCCPFNALImpl(StageOutImpl):
    """
    _DCCPFNALImpl_

    Implement interface for dcache door based dccp command

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
        createTargetName

        generate the target PFN

        """
        if not pfn.startswith("srm"):
            return pfn

        print "Translating PFN: %s\n To use dcache door" % pfn
        dcacheDoor = commands.getoutput(
            "/opt/d-cache/dcap/bin/setenv-cmsprod.sh; /opt/d-cache/dcap/bin/select_RdCapDoor.sh")
        
        
        pfn = pfn.split("/store/")[1]
        pfn = "%s%s" % (dcacheDoor, pfn)
        
        
        print "Created Target PFN with dCache Door: ", pfn
        
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
        result += "export DCACHE_IO_TUNNEL=/opt/d-cache/dcap/lib/libtelnetTunnel.so\n"
        result += "export DCACHE_IO_TUNNEL_TELNET_PWD=/home/cmsprod/passwd4dCapDoor.cmsprod\n"

        result += "dccp -d 255 -X -role=cmsprod %s %s %s" % ( optionsStr, sourcePFN, targetPFN)
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
