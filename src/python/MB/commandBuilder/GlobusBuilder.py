#!/usr/bin/env python
"""
_GlobusBuilder_

Implementation of CommandBuilder for creating globus-job-run and
globus-url-copy based commands

"""


from MB.commandBuilder.CommandBuilder import CommandBuilder
from MB.commandBuilder.CommandFactory import getCommandFactory


class GlobusBuilder(CommandBuilder):
    """
    _GlobusBuilder_

    Implementation of the command builder interface to build globus
    commands.

    Optional Fields used by this Implementation:

    GlobusJobRunBinary - Executable to be used for globus-job-run

    GlobusJobRunOptions - Options to be passed to globus-job-run commands

    GlobusJobRunPort - Optional Port Number for the globus-job-run URL
    
    GlobusJobRunService - Optional Service Name for the globus-job-run URL

    GlobusJobRunSubject - Optional Subject Name for the globus-job-run URL

    GlobusURLCopyBinary - Binary to be used for globus-url-copy

    GlobusURLCopyOptions - Options to be passed to globus-url-copy commands

    """

    def transportSourceToCurrent(self, mbInstance):
        """
        _transportSourceToCurrent_

        Move source to current using gsiftp -> file Xfer
        """
        gucBin = mbInstance.get("GlobusURLCopyBinary", "globus-url-copy")
        gucOpts = mbInstance.get("GlobusURLCopyOptions", "")
        command = "%s %s " % (gucBin, gucOpts)
        command += " gsiftp://%s/%s " % (mbInstance["SourceHostName"],
                                       mbInstance["SourceAbsName"])
        command += " file://%s " % mbInstance["AbsName"]
        return command

    def transportCurrentToTarget(self, mbInstance):
        """
        _transportCurrentToTarget_

        create a command to perform a file -> gsiftp transport

        """
        gucBin = mbInstance.get("GlobusURLCopyBinary", "globus-url-copy")
        gucOpts = mbInstance.get("GlobusURLCopyOptions", "")
        command = "%s %s " % (gucBin, gucOpts)
        command += " file://%s " % mbInstance["AbsName"]
        command += " gsiftp://%s/%s" % (mbInstance["TargetHostName"],
                                       mbInstance["TargetAbsName"])
        return command

    def transportSourceToTarget(self, mbInstance):
        """
        _transportSourceToTarget_

        Create a commmand to transfer source to target via a
        gsiftp -> gsiftp command
        """
        gucBin = mbInstance.get("GlobusURLCopyBinary", "globus-url-copy")
        gucOpts = mbInstance.get("GlobusURLCopyOptions", "")
        command = "%s %s " % (gucBin, gucOpts)
        command += "gsiftp://%s/%s " % (mbInstance["SourceHostName"],
                                       mbInstance["SourceAbsName"])
        command += " gsiftp://%s/%s" % (mbInstance["TargetHostName"],
                                       mbInstance["TargetAbsName"])
        return command

    
    def sourceExists(self, mbInstance):
        """
        _sourceExists_

        Create a globus-job-run command to test file existence
        of the source
        """
        gjrBin = mbInstance.get("GlobusJobRunBinary", "globus-job-run")
        gjrOpts = mbInstance.get("GlobusJobRunOptions", "")
        port = mbInstance.get("GlobusJobRunPort", None)
        svc = mbInstance.get('GlobusJobRunService', None)
        subj = mbInstance.get('GlobusJobRunSubject', None)

        url = self._BuildGlobusURL(mbInstance['SourceHostName'],
                                   port, svc, subj)

        command = "%s %s " % (gjrBin, gjrOpts)
        command += " %s " % url
        command += "/bin/sh -c \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance['SourceAbsName'],
            )
        return command
        

    def targetExists(self, mbInstance):
        """
        _targetExists_

        Create a globus-job-run based existence check for the target
        
        """
        gjrBin = mbInstance.get("GlobusJobRunBinary", "globus-job-run")
        gjrOpts = mbInstance.get("GlobusJobRunOptions", "")
        port = mbInstance.get("GlobusJobRunPort", None)
        svc = mbInstance.get('GlobusJobRunService', None)
        subj = mbInstance.get('GlobusJobRunSubject', None)

        url = self._BuildGlobusURL(mbInstance['TargetHostName'],
                                   port, svc, subj)

        command = "%s %s " % (gjrBin, gjrOpts)
        command += " %s " % url
        command += "/bin/sh -c \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance['TargetAbsName'],
            )
        return command

    def currentExists(self, mbInstance):
        """
        _currentExists_

        Create a globus-job-run based existence check for the current info
        """
        gjrBin = mbInstance.get("GlobusJobRunBinary", "globus-job-run")
        gjrOpts = mbInstance.get("GlobusJobRunOptions", "")
        port = mbInstance.get("GlobusJobRunPort", None)
        svc = mbInstance.get('GlobusJobRunService', None)
        subj = mbInstance.get('GlobusJobRunSubject', None)

        url = self._BuildGlobusURL(mbInstance['HostName'],
                                   port, svc, subj)

        command = "%s %s " % (gjrBin, gjrOpts)
        command += " %s " % url
        command += "/bin/sh -c \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance['AbsName'],
            )
        return command


    
    def createTargetDir(self, mbInstance):
        """
        _createTargetDir_

        create a directory based on the Target values of the MetaBroker
        using globus-job-run
        """
        gjrBin = mbInstance.get("GlobusJobRunBinary", "globus-job-run")
        gjrOpts = mbInstance.get("GlobusJobRunOptions", "")
        port = mbInstance.get("GlobusJobRunPort", None)
        svc = mbInstance.get('GlobusJobRunService', None)
        subj = mbInstance.get('GlobusJobRunSubject', None)

        url = self._BuildGlobusURL(mbInstance['HostName'],
                                   port, svc, subj)

        command = "%s %s " % (gjrBin, gjrOpts)
        command += " %s " % url
        command += "/bin/sh -c \'mkdir -p %s\' " % (
            mbInstance['TargetAbsName'],
            )
        
        return command

    

    def createTargetFile(self, mbInstance):
        """
        _createTargetFile_

        create a file based on the Target values of the MetaBroker
        using globus-job-run
        """
        gjrBin = mbInstance.get("GlobusJobRunBinary", "globus-job-run")
        gjrOpts = mbInstance.get("GlobusJobRunOptions", "")
        port = mbInstance.get("GlobusJobRunPort", None)
        svc = mbInstance.get('GlobusJobRunService', None)
        subj = mbInstance.get('GlobusJobRunSubject', None)

        url = self._BuildGlobusURL(mbInstance['HostName'],
                                   port, svc, subj)

        command = "%s %s " % (gjrBin, gjrOpts)
        command += " %s " % url
        command += "/bin/sh -c \'touch %s\' " % (
            mbInstance['TargetAbsName'],
            )
        
        return command



    def targetURL(self, mbInstance):
        """
        _targetURL_

        Create a URL for the target for gsiftp
        """
        return "gsiftp://%s/%s" % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName']
            )
    

    def sourceURL(self, mbInstance):
        """
        _sourceURL_

        Create a URL for the source for ssh access
        """
        return "gsiftp://%s/%s" % (
            mbInstance['SourceHostName'],
            mbInstance['SourceAbsName']
            )

    
    def currentURL(self, mbInstance):
        """
        _currentURL_

        Create a URL for the current values for ssh access

        """
        return "gsiftp://%s/%s" % (
            mbInstance['HostName'],
            mbInstance['AbsName']
            )
    




    def _BuildGlobusURL(self, host, port, service, subject):
        """
        _BuildGlobusURL_

        Construct a globus service URL from the args provided

        """
        url = "%s" % host
        if port != None:
            url += ":%s" % port
        if service != None:
            url += "/%s" % service
        if subject != None:
            url += ":%s" % subject
        return url




factory = getCommandFactory()
factory.registerAccessProtocol("globus", GlobusBuilder)
        
