#!/usr/bin/env python
"""
_LCGTransporter_

Transporter Implementation for retrieving files from
an LCG Storage Element via the lcg commands

"""
__version__ = "$Revision: 1.6 $"
__revision__ = "$Id: LCGTransporter.py,v 1.6 2006/03/28 17:33:52 afanfani Exp $"
__author__ = "evansde@fnal.gov"


import popen2
import string, os, time
from MB.transport.Transporter import Transporter
from MB.transport.TransportException import TransportException
from MB.transport.TransportException import TransportFailed
from MB.commandBuilder.CommandFactory import getCommandFactory
_CommandFactory = getCommandFactory()

class LCGTransporter(Transporter):
    """
    _LCGTransporter_

    Class which provides transport functionality for moving
    files to and from Storage Elements.

    There are three LCG Commands used for transfers

    TransferIn performs a transfer from an SE to a local disk using lcg-cp

    TransferOut performs a transfer from a local disk to and SE using lcg-cr

    TransferInOut performs a replication transfer from SE to SE using lcg-rep

    Transactions are performed in terms of sfn urls. LFN support is provided
    so that the files can be referred to by LFN as well as sfn

    """
    def __init__(self):
        Transporter.__init__(self)


    
    def transportIn(self, mbInstance):
        """
        _transportIn_

        Invoke the lcg-cp command to retrieve the source file
        from the Storage Element.

        The MetaBroker source fields are treated as follows:

        VO - Name of VO, this is a required field

        LCGProtocol - default is SFN, can be set to LFN. This field changes
        how the source fields are interpreted:

        SFN:
          SourceHostName - SE Hostname
          SourceAbsName - Absolute Name of file
          source file will be sfn:SourceHostName:SourceAbsName

        LFN:
          SourceHostName - SE Hostname
          SourceBaseName - used as LFN
          source file will be lfn:SourceBaseName

        """
        commandMaker = _CommandFactory['lcg']
        command = commandMaker.transportSourceToCurrent(mbInstance)
                                                                                                                             
        print "Copy command: %s"%command

        #args = self._ExtractArgs(mbInstance)
        #
        #if args['LCGProtocol'] not in ('sfn', 'lfn'):
        #    msg = "LCGTransporter does not support LCGProtocol: %s\n" % (
        #        args['LCGProtocol'],
        #        )
        #    msg += "Valid Protocols are sfn, lfn"
        #    raise TransportException(msg, ClassInstance = self,
        #                             MetaBrokerInstance = mbInstance)
        #
        #protocol = args['LCGProtocol']
        #sourceFile = "%s:" % protocol
        #
        #if protocol == 'sfn':
        #    sourceFile += mbInstance["SourceHostName"]
        #    sourceFile += "%s" % mbInstance['SourceAbsName']
        #elif protocol == 'lfn':
        #    sourceFile += mbInstance['SourceBaseName']
        #
        #destFile = "file:%s" % mbInstance['AbsName']
        #command = "lcg-cp --vo %s " % args['VO']
        #command += " %s " % sourceFile
        #command += " %s " % destFile
        try:
            self.runCommand(command)
        except TransportException, ex:
#            ex.addInfo(
#                MetaBrokerInstance = mbInstance,
#                LCGProtocol = protocol,
#                VO = args['VO'])
#            raise
            raise TransportFailed(
                msg, ClassInstance = self,
                MetaBroker = mbInstance)

        mbInstance['URL'] = "file://%s" % mbInstance['AbsName']
        return
        

    def transportOut(self, mbInstance):
        """
        _transportOut_

        Perform a Current -> Target Xfer using lcg-cr command.
        The BaseName of the current file is used as the LFN, unless the
        LFN field is provided. Similarly if a GUID field is provided then
        it is also used in the command. The output URL is the sfn returned
        by the lcg-cr command. If not set, the LFN and GUID fields will be
        set to the values provided by the return value

        TargetHostName is used as the name of the SE Host

        TargetPathName is used as the relative Path to send the file to
        
        """
        commandMaker = _CommandFactory['lcg']
        command = commandMaker.transportCurrentToTarget(mbInstance)

        print "Copy command: %s"%command
        try:
#            result = self.runCommand(command)
            result = self.runCommand(command,1200)
        except TransportException, ex:
            raise TransportFailed(
                str(ex), ClassInstance = self,
                MetaBroker = mbInstance)
#what's the following lin for?
        mbInstance['LFN'] = mbInstance['TargetBaseName']
        
        return True


    def transportInOut(self, mbInstance):
        """
        Handle Transport from Source to Target values
        """
        commandMaker = _CommandFactory['lcg']
        command = commandMaker.transportSourceToTarget(mbInstance)
        print "Copy command: %s"%command
        try:
            result = self.runCommand(command)
        except TransportException, ex:
            raise TransportFailed(
                str(ex), ClassInstance = self,
                MetaBroker = mbInstance)
        return True
        
        
#    def runCommand(self, command):
#        """
#        _runCommand_
#
#        Popen based callout to a shell command, raises an
#        Exception if a non-zero exit code is returned
#        """
#        pop = popen2.Popen4(command)
#        while pop.poll() == -1:
#            exitCode = pop.poll()
#        exitCode = pop.poll()
#        output = pop.fromchild.read()
#        if exitCode:
#            msg = "Command Failed with Exit Code: %s\n" % exitCode
#            msg += "Command:\n%s\n" % command
#            msg += "Command Output:\n"
#            msg += output
#            raise TransportException(msg, ClassInstance = self)
#        return output

    def runCommand(self, command, timeout=-1):
        """
        _runCommand_
       
        Popen based callout to a shell command, with timeout.
        It raises an Exception if a non-zero exit code is returned
        """
        pop = popen2.Popen4(command)
        pop.tochild.close()             # don't need to talk to child

        if timeout > 0 :
          maxwaittime = time.time() + timeout

        # pop.poll() Returns -1 if child process hasn't completed yet, or its return code otherwise.
        while ( pop.poll() == -1 and (timeout == -1 or time.time() < maxwaittime) ):
            exitCode = pop.poll()
            #print "debug exitCode %s"%exitCode
        exitCode = pop.poll()  # -1 if process got timedout

        if exitCode == -1:
          os.kill (pop.pid, 9)
          msg = "Killing after timeout %s sec Command: %s \n" %(str(timeout),command)
          raise TransportException(msg, ClassInstance = self)

        output = pop.fromchild.read()
        if exitCode:
            msg = "Command Failed with Exit Code: %s\n" % exitCode
            msg += "Command:\n%s\n" % command
            msg += "Command Output:\n"
            msg += output
            raise TransportException(msg, ClassInstance = self)

        return output

