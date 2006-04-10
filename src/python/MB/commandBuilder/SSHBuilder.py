#!/usr/bin/env python
"""
_SSHBuilder_

Implementation of CommandBuilder for secure shell access protocol

"""



from MB.commandBuilder.CommandBuilder import CommandBuilder
from MB.commandBuilder.CommandFactory import getCommandFactory


class SSHBuilder(CommandBuilder):


    def transportSourceToCurrent(self, mbInstance):
        """
        _transportSourceToCurrent_

        use scp to transport the source to values to the current values 
        
        """
        scpBinary = mbInstance.get('ScpBinary', 'scp')
        scpOptions = mbInstance.get('ScpOptions', '')
        command = "%s %s " % (scpBinary, scpOptions)
        command += " %s:%s " % (
            mbInstance['SourceHostName'],
            mbInstance['SourceAbsName'],
            )
        command += " %s " % mbInstance['AbsName']
        return command


    
    def transportCurrentToTarget(self, mbInstance):
        """
        _transportCurrentToTarget_

        create a scp commmand to transport current to target
        """
        scpBinary = mbInstance.get('ScpBinary', 'scp')
        scpOptions = mbInstance.get('ScpOptions', '')
        command = "%s %s " % (scpBinary, scpOptions)
        command += " %s " % mbInstance['AbsName']
        command += " %s:%s " % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName'],
            )
        return command


    def transportSourceToTarget(self, mbInstance):
        """
        _transportSourceToTarget_

        create an scp command to move Source to target
        """
        scpBinary = mbInstance.get('ScpBinary', 'scp')
        scpOptions = mbInstance.get('ScpOptions', '')
        command = "%s %s " % (scpBinary, scpOptions)
        command += " %s:%s " % (
            mbInstance['SourceHostName'],
            mbInstance['SourceAbsName'],
            )
        command += " %s:%s " % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName'],
            )
        return command


    def sourceExists(self, mbInstance):
        """
        _sourceExists_

        create an ssh existence check for the source

        """
        sshBin = mbInstance.get("SshBinary", "ssh")
        sshOpts = mbInstance.get("SshOptions", "")
        command = "%s %s " % (sshBin, sshOpts)
        command += "%s \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance["SourceHostName"],
            mbInstance["SourceAbsName"],
            )
        return command



    def targetExists(self, mbInstance):
        """
        _targetExists_

        create an ssh existence check for the target
        """
        sshBin = mbInstance.get("SshBinary", "ssh")
        sshOpts = mbInstance.get("SshOptions", "")
        command = "%s %s " % (sshBin, sshOpts)
        command += "%s \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance["TargetHostName"],
            mbInstance["TargetAbsName"],
            )
        return command




    def currentExists(self, mbInstance):
        """
        _currentExists_

        create an ssh existence check for the current value
        """
        sshBin = mbInstance.get("SshBinary", "ssh")
        sshOpts = mbInstance.get("SshOptions", "")
        command = "%s %s " % (sshBin, sshOpts)
        command += "%s \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance["HostName"],
            mbInstance["AbsName"],
            )
        return command
    
    def createTargetDir(self, mbInstance):
        """
        _createTargetDir_

        create a directory based on the Target values of the MetaBroker
        using ssh access
        """
        sshBin = mbInstance.get("SshBinary", "ssh")
        sshOpts = mbInstance.get("SshOptions", "")
        command = "%s %s " % (sshBin, sshOpts)
        command += "%s \'mkdir -p %s\' " % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName'],
            )
        return command

    

    def createTargetFile(self, mbInstance):
        """
        _createTargetFile_

        create a file based on the Target values of the MetaBroker
        using local filesystem tools
        """
        sshBin = mbInstance.get("SshBinary", "ssh")
        sshOpts = mbInstance.get("SshOptions", "")
        command = "%s %s " % (sshBin, sshOpts)
        command += "%s \'touch %s\' " % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName'],
            )
        return command


    
    def targetURL(self, mbInstance):
        """
        _targetURL_

        Create a URL for the target for ssh access
        """
        return "ssh://%s/%s" % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName']
            )
    

    def sourceURL(self, mbInstance):
        """
        _sourceURL_

        Create a URL for the source for ssh access
        """
        return "ssh://%s/%s" % (
            mbInstance['SourceHostName'],
            mbInstance['SourceAbsName']
            )

    
    def currentURL(self, mbInstance):
        """
        _currentURL_

        Create a URL for the current values for ssh access

        """
        return "ssh://%s/%s" % (
            mbInstance['HostName'],
            mbInstance['AbsName']
            )


factory = getCommandFactory()
factory.registerAccessProtocol("ssh", SSHBuilder)
