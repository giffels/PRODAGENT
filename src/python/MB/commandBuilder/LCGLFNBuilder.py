#!/usr/bin/env python
"""
_LCGLFNBuilder_

LCG based command building for transfer using LFN as opposed to SFN

"""
import os
from MB.commandBuilder.CommandBuilder import CommandBuilder
from MB.commandBuilder.CommandFactory import getCommandFactory
from MB.MBException import MBException

class LCGLFNBuilder(CommandBuilder):
    """
    _LCGLFNBuilder_

    Create commands for handling files using LCG commands and
    logical filenames.

    Options understood:

    LCGVO  - VO to be used as part of the command

    LCGCPBinary - lcg-cp by default

    LCGOptions - other options to be passed to the command
    
    """
    def transportSourceToCurrent(self, mbInstance):
        """
        _transportSourceToCurrent_

        Create a command to move the remote source to local current settings.
        
        SourceBaseName is treated as an lfn.

        """

        sourceFile = "lcg:%s" % mbInstance['SourceBaseName']
        targetFile = "file://%s" % mbInstance['AbsName']
        binary = mbInstance.get("LCGCPBinary", "lcg-cp")
        vorg = self._CheckVO(mbInstance)
        opts = mbInstance.get("LCGOptions", "")
      
        command = binary
        command += " %s " % opts
        command += " --vo %s " % vorg
        command += " %s " % sourceFile
        command += " %s " % targetFile

        return command
        

        
        
    def transportCurrentToTarget(self, mbInstance):
        """
        _transportCurrentToTarget_

        create a command to perform a file -> SE transport
        using lcg-cr to upload the file

        
        
        """
        args = self._ExtractArgs(mbInstance)
        lfn = args['LFN'] or mbInstance['TargetBaseName']
        guid = args['GUID']
                                                                                                                                 
        sourceFile = "file://%s" % mbInstance['AbsName']
        ## use lcg-cp for now, without using a grid catalog
        #command = "lcg-cr --vo %s " % args['VO']
        #command += " -d %s " % mbInstance['TargetHostName']
        #command += " -P %s " % mbInstance['TargetPathName']
        #command += " -l lfn:%s " % lfn
        #
        lcgcpBinary = mbInstance.get("LCGCPBinary", "lcg-cp")
        lcgcpOptions = mbInstance.get("LCGOptions", " -v -t 1200 ")
        #binary = "lcg-cp"
        command = lcgcpBinary
        command += " %s " % lcgcpOptions
        command += " --vo %s " % args['VO']
        #subdir=mbInstance['TargetBaseName'].split("-")[0]
        destFile="gsiftp://%s%s/%s"%(mbInstance['TargetHostName'], mbInstance['TargetPathName'].replace("\n","").strip(),mbInstance['TargetBaseName'])
        if guid != None:
            command += " -g %s " % guid
        command += " %s " % sourceFile
        command += " %s " % destFile
        print "\n SE_OUT: %s"%mbInstance['TargetHostName']
        print "\n SE_PATH: %s/%s"%(mbInstance['TargetPathName'].replace("\n","").strip(),os.path.dirname(mbInstance['TargetBaseName']))
        return command

    
    def transportSourceToTarget(self, mbInstance):
        """
        _transportSourceToTarget_

        Replicate SE source to SE Target using lcg-rep
        and the lfn of the source file
        
        """
        binary = mbInstance.get("LCGREPBinary", "lcg-rep")
        sourceFile = "lcg:%s" % mbInstance['SourceBaseName']
        targetSE = mbInstance['TargetHost']
        vorg = self._CheckVO(mbInstance)
        opts = mbInstance.get("LCGOptions", "")

        command = binary
        command += " %s " % opts
        command += " --vo %s " % vorg

        command += " -d %s " % targetSE
        command += " %s " % sourceFile

        return command


    def sourceExists(self, mbInstance):
        """
        _sourceExists_

        Generate a command to check if the source lfn exists
        using lcg-lr
        
        """
        binary = mbInstance.get("LCGLRBinary", "lcg-lr")
        vorg = self._CheckVO(mbInstance)
        opts = mbInstance.get("LCGOptions", "")
        sourceLFN = "lfn:%s" % mbInstance['SourceBaseName']
        command = binary
        command += " %s " % opts
        command += " --vo %s " % vorg
        command += " %s" % sourceLFN
        return command
        
    def targetExists(self, mbInstance):
        """
        _targetExists_

        Generate a command to check if the target lfn exists
        using lcg-lr
        
        """
        binary = mbInstance.get("LCGLRBinary", "lcg-lr")
        vorg = self._CheckVO(mbInstance)
        opts = mbInstance.get("LCGOptions", "")
        targetLFN = "lfn:%s" % mbInstance['TargetBaseName']
        command = binary
        command += " %s " % opts
        command += " --vo %s " % vorg
        command += " %s" % sourceLFN
        return command

    def targetURL(self, mbInstance):
        """
        _targetURL_
        
        Create a URL for the target using the lfn
        """
        return "lfn:%s" % mbInstance['TargetBaseName']
    
    def sourceURL(self, mbInstance):
        """
        _sourceURL_

        Create a URL for the source for ssh access
        """
        return "lfn:%s" % mbInstance['SourceBaseName']
    
    def createTargetDir(self, mbInstance):
        """
        _createTargetDir_

        Null operation, since lcg-cp makes dirs for you

        """
        return "echo \"LCG-CP makes its own directories\" "
    

    
    def currentURL(self, mbInstance):
        """
        _currentURL_

        Create a URL for the current values for ssh access

        """
        return "lfn:%s" % mbInstance['BaseName']
    

    def _CheckVO(self, mbInstance):
        """
        _CheckVO_

        Make sure that the LCGVO argument if provided

        """
        vorg = mbInstance.get('LCGVO', None)
        if vorg == None:
            msg = "Virtual Organisation not specified for LCG Command\n"
            msg += "You must supply a LCGVO key in the MetaBroker"
            raise MBException(msg, ClassInstance = self,
                              MetaBroker = mbInstance)
        return vorg


    def _ExtractArgs(self, mbInstance):
        """
        _ExtractArgs_
                                                                                                                                 
        Retrieve Args from the mbInstance
                                                                                                                                 
        """
        args = {}
        #args.setdefault("VO", mbInstance.get("VO", None))
        args.setdefault("VO", "cms")
        args.setdefault("LCGProtocol", mbInstance.get("LCGProtocol", "sfn"))
        args['LCGProtocol'] = args['LCGProtocol'].lower()
        args.setdefault('LFN', mbInstance.get('LFN', None))
        args.setdefault('GUID', mbInstance.get('GUID', None))
        if args['VO'] == None:
            msg = "VO Not specified in MetaBroker\n"
            msg += "Unable to transfer file from SE\n"
            raise TransportException(msg, ClassInstance = self,
                                     MetaBrokerInstance = mbInstance)
        return args

    
factory = getCommandFactory()
factory.registerAccessProtocol("lcg", LCGLFNBuilder)
   
