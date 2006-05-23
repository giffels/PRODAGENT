#!/usr/bin/env python
# pylint: disable-msg=W0613
"""
Base class for all transport objects for moving
objects referenced by metabrokers

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: Transporter.py,v 1.1 2006/04/10 17:10:00 evansde Exp $"


import popen2
from MB.MetaBroker import MetaBroker
from MB.transport.TransportException import TransportException, TransportFailed

class Transporter:
    """
    _Transporter_

    Base class for Transporter Objects, defines API
    for transporting a MetaBroker instance based on its keys.
    
    """
    def __init__(self):
        pass


    def __call__(self, mbInstance):
        """
        _Operator()_

        Define the action of the transporter on a
        MetaBroker Object. The argument must be a
        MetaBroker instance, based on the
        keys in the MetaBroker, the appropriate
        transport method is called

        Args --

        - *mbInstance* : MetaBroker instance to be transported
        
        """
        if not isinstance(mbInstance, MetaBroker):
            msg = "Non MetaBroker Instance passed to Transporter:"
            msg +="%s" % self.__class__.__name__
            raise TransportException(
                msg, ModuleName = "MB.transport.Transporter",
                ClassName = "Transporter",
                MethodName = "__call__",
                BadInstance = mbInstance)
        
     
        testSource = mbInstance['Source'] != None
        testTarget = mbInstance['Target'] != None
        if testSource and testTarget:
            self.transportInOut(mbInstance)
            return  1

        if testSource:
            self.transportIn(mbInstance)
            return 1
        if testTarget:
            self.transportOut(mbInstance)
            return 1
        return 0
        
        
        
        

        
        
        

    def transportIn(self, mbInstance):
        """
        _transportIn_

        *Abstract Method*

        Method called to transfer from Source to Memory values
        of MetaBroker. Should be Overriden by subclass

        - *mbInstance* : MetaBroker instance to be acted upon
        
        """
        msg = "transportIn Not implemented for %s" % (
            self.__class__.__name__,
            )
        raise TransportException(msg, MethodName = "transportIn")

    def transportOut(self, mbInstance):
        """
        _transportOut_

        *Abstract Method*

        Method called to transfer from Memory to Target values
        of MetaBroker. Should be Overriden by subclass

        - *mbInstance* : MetaBroker instance to be acted upon
        
        """
        msg = "transportOut Not implemented for %s" % (
            self.__class__.__name__,
            )
        raise TransportException(msg, MethodName = "transportOut")


    def transportInOut(self, mbInstance):
        """
        _transportInOut_

        *Abstract Method*

        Method called to transfer from Source to Target values
        of MetaBroker. Should be Overriden by subclass

        - *mbInstance* : MetaBroker instance to be acted upon
        
        """


        msg = "transportInOut Not implemented for %s" % (
            self.__class__.__name__,
            )
        raise TransportException(msg, MethodName = "transportInOut")



    
        
        
        

    def runCommand(self, command):
        """
        _runCommand_

        Implement a safe way to run a shell command

        """
        pop = popen2.Popen4(command)
        pop.tochild.close()
        output = pop.fromchild.read()
        exitCode = pop.wait()
        
        if exitCode > 0:
            msg = "Transporter.runCommand failed for %s\n" % (
                self.__class__.__name__,
                )
            msg += "Command Used: %s" % command
            raise TransportFailed(
                msg, ClassInstance = self,
                Command = command)
        return True

