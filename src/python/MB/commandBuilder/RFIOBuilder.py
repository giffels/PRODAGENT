#!/usr/bin/env python
"""
_RFIOBuilder_

Implementation of CommandBuilder for secure shell access protocol

"""


import os
from MB.commandBuilder.CommandBuilder import CommandBuilder
from MB.commandBuilder.CommandFactory import getCommandFactory


class RFIOBuilder(CommandBuilder):


    def transportSourceToCurrent(self, mbInstance):
        """
        _transportSourceToCurrent_

        use rfcp to transport the source to values to the current values 
        
        """
        rfcpBinary = mbInstance.get('RfcpBinary', 'rfcp')
        rfcpOptions = mbInstance.get('RfcpOptions', '')
        command = "%s %s " % (rfcpBinary, rfcpOptions)
        command += mbInstance['SourceAbsName']
        command += " %s " % mbInstance['AbsName']
        return command


    
    def transportCurrentToTarget(self, mbInstance):
        """
        _transportCurrentToTarget_

        create a rfcp commmand to transport current to target
        """
        rfcpBinary = mbInstance.get('RfcpBinary', 'rfcp')
        rfcpOptions = mbInstance.get('RfcpOptions', '')
        command = "%s %s " % (rfcpBinary, rfcpOptions)
        command += " %s " % mbInstance['AbsName']
        command += " %s " % mbInstance['TargetAbsName']
        
        return command


    def transportSourceToTarget(self, mbInstance):
        """
        _transportSourceToTarget_

        create an rfcp command to move Source to target
        """

        rfcpBinary = mbInstance.get('RfcpBinary', 'rfcp')
        rfcpOptions = mbInstance.get('RfcpOptions', '')
        command = "%s %s " % (rfcpBinary, rfcpOptions)
        command += " %s " % mbInstance['SourceAbsName']
        command += " %s " % mbInstance['TargetAbsName']
            
        return command


    def sourceExists(self, mbInstance):
        """
        _sourceExists_

        create an rfio existence check for the source

        """
        rfsatBin = mbInstance.get("RfstatBinary", "rfstat")
        rfstatOpts = mbInstance.get("RfstatOptions", "")
        command = "%s %s " % (rfsatBin, rfstatOpts)
        command += "%s" % mbInstance["SourceAbsName"]
        return command



    def targetExists(self, mbInstance):
        """
        _targetExists_

        create an rfio existence check for the target
        """
        rfsatBin = mbInstance.get("RfstatBinary", "rfstat")
        rfstatOpts = mbInstance.get("RfstatOptions", "")
        command = "%s %s " % (rfsatBin, rfstatOpts)
        command += "%s" % mbInstance["TargetAbsName"]
            
        return command




    def currentExists(self, mbInstance):
        """
        _currentExists_

        create an rfio existence check for the current value
        """
        rfsatBin = mbInstance.get("RfstatBinary", "rfstat")
        rfstatOpts = mbInstance.get("RfstatOptions", "")
        command = "%s %s " % (rfsatBin, rfstatOpts)
        command += "%s" % (
            mbInstance["AbsName"],
            )
        return command
    
    def createTargetDir(self, mbInstance):
        """
        _createTargetDir_

        create a directory based on the Target values of the MetaBroker
        using rfio access
        """
        rfmkdirBin = mbInstance.get("Rfmkdir", "rfmkdir -m 775 -p ")
        rfmkdirOpts = mbInstance.get("RfmkdirOptions", "")
        command = "%s %s " % (rfmkdirBin, rfmkdirOpts)
        command += "%s " % (
            os.path.dirname(mbInstance['TargetAbsName']),
            )
        return command

    

    def createTargetFile(self, mbInstance):
        """
        _createTargetFile_

        create a file based on the Target values of the MetaBroker
        using local filesystem tools
        """
        command = "No touch command in rfio"
        command += "we could prbably create an empty file and copy it" 
        return command


    
    def targetURL(self, mbInstance):
        """
        _targetURL_

        Create a URL for the target for rfio access
        """
        return "rfio:%s" % (
            mbInstance['TargetAbsName'],
            )
    

    def sourceURL(self, mbInstance):
        """
        _sourceURL_

        Create a URL for the source for rfio access
        """
        return "rfio:%s" % (
            mbInstance['SourceAbsName'],
            )

    
    def currentURL(self, mbInstance):
        """
        _currentURL_

        Create a URL for the current values for rfio access

        """
        return "rfio:%s" % (
            mbInstance['AbsName'],
            )


factory = getCommandFactory()
factory.registerAccessProtocol("rfio", RFIOBuilder)
