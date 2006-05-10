#!/usr/bin/env python
"""
_DCAPBuilder_

Implementation of CommandBuilder for simple dccp access protocol on local systems
"""



from MB.commandBuilder.CommandBuilder import CommandBuilder
from MB.commandBuilder.CommandFactory import getCommandFactory


class DCAPBuilder(CommandBuilder):

    

    def transportSourceToCurrent(self, mbInstance):
        """
        _transportSourceToCurrent_

        use dccp to transport the source to values to the current values 
        
        """
        dccpBinary = mbInstance.get('DccpBinary', 'dccp')
        dccpOptions = mbInstance.get('DccpOptions', '')
        command = "%s %s " % (dccpBinary, dccpOptions)
        command += " %s " % mbInstance['SourceAbsName']
        command += " %s " % mbInstance['AbsName']
        return command
        



    def transportCurrentToTarget(self, mbInstance):
        """
        _transportCurrentToTarget_

        create a dccp commmand to transport current to target
        """
        dccpBinary = mbInstance.get('DccpBinary', 'dccp')
        dccpOptions = mbInstance.get('DccpOptions', '')
        command = "%s %s " % (dccpBinary, dccpOptions)
        command += " %s " % mbInstance['AbsName']
        command += " %s " % mbInstance['TargetAbsName']
        return command
        


    def transportSourceToTarget(self, mbInstance):
        """
        _transportSourceToTarget_

        create a dccp command to move Source to target
        """
        dccpBinary = mbInstance.get('DccpBinary', 'dccp')
        dccpOptions = mbInstance.get('DccpOptions', '')
        command = "%s %s " % (dccpBinary, dccpOptions)
        command += " %s " % mbInstance['SourceAbsName']
        command += " %s " % mbInstance['TargetAbsName']
        return command

    def sourceExists(self, mbInstance):
        """
        _sourceExists_

        create a local existence check for the source

        """
        command = "/bin/sh -c \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance["SourceAbsName"],
            )
        return command



    def targetExists(self, mbInstance):
        """
        _targetExists_

        create a local existence check for the target
        """
        command = "/bin/sh -c \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance["TargetAbsName"],
            )
        return command
        




    def currentExists(self, mbInstance):
        """
        _currentExists_

        create a local existence check for the current value
        """
        command = "/bin/sh -c \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance["TargetAbsName"],
            )
        return command
        


    def createTargetDir(self, mbInstance):
        """
        _createTargetDir_

        create a directory based on the Target values of the MetaBroker
        using local filesystem tools
        """
        command = "mkdir -p %s" % mbInstance['TargetAbsName']
        return command

    

    def createTargetFile(self, mbInstance):
        """
        _createTargetFile_

        create a file based on the Target values of the MetaBroker
        using local filesystem tools
        """
        command = "touch %s" % mbInstance['TargetAbsName']
        return command

    

    def targetURL(self, mbInstance):
        """
        _targetURL_

        Create a URL for the target for local access
        """
        if mbInstance['TargetHostName'] == None:
            return "dcap://%s" % (
            mbInstance['TargetAbsName']
            )
        return "dcap://%s/%s" % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName']
            )
    

    def sourceURL(self, mbInstance):
        """
        _sourceURL_

        Create a URL for the source for local access
        """
        return "dcap://%s/%s" % (
            mbInstance['SourceHostName'],
            mbInstance['SourceAbsName']
            )

    
    def currentURL(self, mbInstance):
        """
        _currentURL_

        Create a URL for the current values for local access

        """
        return "dcap://%s/%s" % (
            mbInstance['HostName'],
            mbInstance['AbsName']
            )

    def deleteCurrent(self, mbInstance):
        """
        _deleteCurrent_

        Generate a commadn to delete a file from dCache
        
        """
        return "/bin/rm %s" % mbInstance['AbsName']
    


factory = getCommandFactory()
factory.registerAccessProtocol("dccp", DCAPBuilder)

