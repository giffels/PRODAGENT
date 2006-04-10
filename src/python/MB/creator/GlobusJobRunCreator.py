#!/usr/bin/env python
"""
_GlobusJobRunCreator_

Creator implementation based on the globus-job-run command

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: GlobusJobRunCreator.py,v 1.1 2005/12/30 18:51:39 evansde Exp $"
__author__ = "evansde@fnal.gov"


import os

from MB.creator.Creator import Creator
from MB.creator.CreatorException import CreatorException



class GlobusJobRunCreator(Creator):
    """
    _GlobusJobRunCreator_

    Implement creation commands via the globus-job-run command
    The Jobmanager, port and subject for the contact string
    are optional, and can be passed as arguments in the
    MetaBroker

    GlobusJobRunPort

    GlobusJobRunService

    GlobusJobRunSubject

    If these are not provided, the just the host name is used.

    """


    def __init__(self):
        Creator.__init__(self)



    def createDir(self, mbInstance):
        """
        _createDir_

        Create dir on remote host via globus-run-job command

        """
        args = self._ExtractArgs(mbInstance)

        comm = "%s %s " % (args['GlobusJobRunBinary'],
                           args['GlobusJobRunOptions'])

        url = self._BuildContactURL(mbInstance)
        comm += " %s " % url
        comm += "/bin/sh -c \"mkdir -p %s\"" % mbInstance['TargetAbsName']

        try:
            self.runShellCommand(comm)
        except CreatorException, ex:
            ex.addInfo(RemoteDirName = mbInstance['TargetAbsName'],
                       RemoteHostName = mbInstance['TargetHostName'],
                       DMBInstance = mbInstance)
            raise
        return
        

    def createFile(self, mbInstance):
        """
        _createFile_

        Create a file placeholder via the globus-run-job command

        """
        args = self._ExtractArgs(mbInstance)

        comm = "%s %s " % (args['GlobusJobRunBinary'],
                           args['GlobusJobRunOptions'])

        url = self._BuildContactURL(mbInstance)
        dirName = os.path.dirname(mbInstance['TargetAbsName'])
        comm += " %s " % url
        dirComm = "%s /bin/sh -c \"mkdir -p %s\"" % (
            comm,
            dirName,
            )

        try:
            self.runShellCommand(dirComm)
        except CreatorException, ex:
            ex.addInfo(RemoteDirName = dirName,
                       RemoteHostName = mbInstance['TargetHostName'],
                       FMBInstance = mbInstance)
            raise

        fileComm = "%s /bin/sh -c \"touch %s\"" % (
            comm,
            mbInstance['TargetAbsName'],
            )
        try:
            self.runShellCommand(fileComm)
        except CreatorException, ex:
            ex.addInfo(RemoteFileName = mbInstance['TargetAbsName'],
                       RemoteHostName = mbInstance['TargetHostName'],
                       FMBInstance = mbInstance)
            raise
        
        return



    def _ExtractArgs(self, mbInstance):
        """
        _ExtractArgs_

        Retrieve the arguments from the metabroker

        """
        args = {}
        args['GlobusJobRunPort'] = mbInstance.get('GlobusJobRunPort', None)

        args['GlobusJobRunService'] = mbInstance.get('GlobusJobRunService',
                                                     None)

        args['GlobusJobRunSubject'] = mbInstance.get('GlobusJobRunSubject',
                                                     None)
        
        args['GlobusJobRunBinary'] = mbInstance.get('GlobusJobRunBinary',
                                                    'globus-job-run')

        args['GlobusJobRunOptions'] = mbInstance.get('GlobusJobRunOptions',
                                                     '')

        return args

    def _BuildContactURL(self, mbInstance):
        """
        _BuildContactURL_

        Internal method to construct the remote contact string to execute
        the command on
        """
        hostName = mbInstance['TargetHostName']
        args = self._ExtractArgs(mbInstance)

        url = "%s" % hostName
        if args['GlobusJobRunPort'] != None:
            url += ":%s" % args['GlobusJobRunPort']
        if args['GlobusJobRunService'] != None:
            url += "/%s" % args['GlobusJobRunService']
        if args['GlobusJobRunSubject'] != None:
            url += ":%s" % args['GlobusJobRunSubject']

        return url
            
        
        
