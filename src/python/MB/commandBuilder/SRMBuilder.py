#!/usr/bin/env python
"""
_SRMBuilder_

SRM based command building

"""

from MB.commandBuilder.CommandBuilder import CommandBuilder
from MB.commandBuilder.CommandFactory import getCommandFactory


class SRMBuilder(CommandBuilder):
    """
    _SRMBuilder_

    SRM command builder used for creating commands to manipulate
    files via srm

    Options used by this builder are:

    SRMProxyFile - path to the proxy file to be used for the SRM xfer

    SRMCPBinary - path to the srmcp binary to be used

    SRMCPOptions - options to be passed to the srmcp command

    SRMCPPort - Port number to be used for srmcp
    
    """
    def transportSourceToCurrent(self, mbInstance):
        """
        _transportSourceToCurrent_

        Create a command to move a remote srm file to
        a local file url
        """
        srmcpBin = mbInstance.get("SRMCPBinary", "srmcp")
        srmcpOpts = mbInstance.get("SRMCPOptions", "")
        srmcpPort = mbInstance.get("SRMCPPort", None)
        proxy = mbInstance.get("SRMProxyFile", None)
        if srmcpPort == None:
            srmcpPort = ""
        else:
            srmcpPort = ":%s" % srmcpPort

        if proxy == None:
            srmproxy = ""
        else:
            srmproxy = " -x509_user_proxy=%s " % proxy
        command = "%s %s %s " % (srmcpBin, srmcpOpts, srmproxy)
        
        command += " srm://%s%s/%s " % (mbInstance["SourceHostName"],
                                        srmcpPort,
                                        mbInstance["SourceAbsName"])
        command += " file:///%s " % mbInstance["AbsName"]
        return command


    def transportCurrentToTarget(self, mbInstance):
        """
        _transportCurrentToTarget_

        create a command to perform a file -> srm transport

        """
        srmcpBin = mbInstance.get("SRMCPBinary", "srmcp")
        srmcpOpts = mbInstance.get("SRMCPOptions", "")
        srmcpPort = mbInstance.get("SRMCPPort", None)
        proxy = mbInstance.get("SRMProxyFile", None)
        if srmcpPort == None:
            srmcpPort = ""
        else:
            srmcpPort = ":%s" % srmcpPort
        if proxy == None:
            srmproxy = ""
        else:
            srmproxy = " -x509_user_proxy=%s " % proxy
        command = "%s %s %s " % (srmcpBin, srmcpOpts, srmproxy)
        command += " file:///%s " % mbInstance["AbsName"]
        command += " srm://%s%s/%s" % (mbInstance["TargetHostName"],
                                       srmcpPort,
                                       mbInstance["TargetAbsName"])
        return command


    def transportSourceToTarget(self, mbInstance):
        """
        _transportSourceToTarget_

        Create a commmand to transfer source to target via a
        gsiftp -> gsiftp command
        """
        srmcpBin = mbInstance.get("SRMCPBinary", "srmcp")
        srmcpOpts = mbInstance.get("SRMCPOptions", "")
        srmcpPort = mbInstance.get("SRMCPPort", None)
        srmcpSrcPort = mbInstance.get("SRMCPSourcePort", None)
        srmcpTgtPort = mbInstance.get("SRMCPTargetPort", None)
        proxy = mbInstance.get("SRMProxyFile", None)
        srcPort = ""
        if srmcpSrcPort != None:
            srcPort = ":%s" % srmcpSrcPort
        else:
            if srmcpPort != None:
                srcPort = ":%s" % srmcpPort
        tgtPort = ""
        if srmcpTgtPort != None:
            tgtPort = ":%s" % srmcpTgtPort
        else:
            if srmcpPort != None:
                tgtPort = ":%s" % srmcpPort
        if proxy == None:
            srmproxy = ""
        else:
            srmproxy = " -x509_user_proxy=%s " % proxy
        
        command = "%s %s %s " % (srmcpBin, srmcpOpts, srmproxy)
        
        command += " srm://%s%s/%s " % (mbInstance["SourceHostName"],
                                        srcPort,
                                        mbInstance["SourceAbsName"])
        command += " srm://%s/%s" % (mbInstance["TargetHostName"],
                                     srmtgtPort,
                                     mbInstance["TargetAbsName"])
        
        return command


    def sourceExists(self, mbInstance):
        """
        _sourceExists_

        Check existence of file using srm-get-metadata
        """
        srmBin = mbInstance.get("SRMGetMetadataBinary", "srm-get-metadata")
        srmOpts = mbInstance.get("SRMGetMetadataOptions", "")
        srmTimeout = mbInstance.get("SRMGetMetadataTimeout", "1000")
        srmRetries = mbInstance.get("SRMGetMetadataRetries", "2")
        srmcpPort = mbInstance.get("SRMCPPort", None)
        proxy = mbInstance.get("SRMProxyFile", None)

        if srmcpPort == None:
            srmcpPort = ""
        else:
            srmcpPort = ":%s" % srmcpPort
            
        if proxy == None:
            srmproxy = ""
        else:
            srmproxy = " -x509_user_proxy=%s " % proxy
        command = "%s %s %s -retry_num=%s -retry_timeout=%s " % (
            srmBin, srmOpts, srmproxy,
            srmRetries, srmTimeout,
            )
        
        command += " srm://%s%s/%s " % (mbInstance["SourceHostName"],
                                        srmcpPort,
                                        mbInstance["SourceAbsName"])
        return command

    def targetExists(self, mbInstance):
        """
        _targetExists_

        Check existence of Target via srm-get-metadata

        """
        srmBin = mbInstance.get("SRMGetMetadataBinary", "srm-get-metadata")
        srmOpts = mbInstance.get("SRMGetMetadataOptions", "")
        srmTimeout = mbInstance.get("SRMGetMetadataTimeout", "1000")
        srmRetries = mbInstance.get("SRMGetMetadataRetries", "2")
        srmcpPort = mbInstance.get("SRMCPPort", None)
        proxy = mbInstance.get("SRMProxyFile", None)

        if srmcpPort == None:
            srmcpPort = ""
        else:
            srmcpPort = ":%s" % srmcpPort
            
        if proxy == None:
            srmproxy = ""
        else:
            srmproxy = " -x509_user_proxy=%s " % proxy
        command = "%s %s %s -retry_num=%s -retry_timeout=%s " % (
            srmcpBin, srmcpOpts, srmproxy,
            srmRetries, srmTimeout,
            )
        
        command += " srm://%s/%s" % (mbInstance["TargetHostName"],
                                     srmcpPort,
                                     mbInstance["TargetAbsName"])
        return command


    def targetURL(self, mbInstance):
        """
        _targetURL_
        
        Create a URL for the target for srmcp
        """
        srmcpPort = mbInstance.get("SRMCPPort", None)
        if srmcpPort == None:
            srmcpPort = ""
        else:
            srmcpPort = ":%s" % srmcpPort
        return "srm://%s%s/%s" % (
            mbInstance['TargetHostName'], srmcpPort,
            mbInstance['TargetAbsName']
            )
    
    
    def sourceURL(self, mbInstance):
        """
        _sourceURL_

        Create a URL for the source for ssh access
        """
        srmcpPort = mbInstance.get("SRMCPPort", None)
        if srmcpPort == None:
            srmcpPort = ""
        else:
            srmcpPort = ":%s" % srmcpPort
        return "srm://%s%s/%s" % (
            mbInstance['SourceHostName'], srmcpPort,
            mbInstance['SourceAbsName']
            )
    
        

    
    def currentURL(self, mbInstance):
        """
        _currentURL_

        Create a URL for the current values for ssh access

        """
        srmcpPort = mbInstance.get("SRMCPPort", None)
        if srmcpPort == None:
            srmcpPort = ""
        else:
            srmcpPort = ":%s" % srmcpPort
        return "srm://%s%s/%s" % (
            mbInstance['HostName'], srmcpPort,
            mbInstance['AbsName']
            )
    


factory = getCommandFactory()
factory.registerAccessProtocol("srm", SRMBuilder)
        
