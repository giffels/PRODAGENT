#!/usr/bin/env python
# pylint: disable-msg=W0613
"""
Base class for all creator objects for creating files or dirs
referenced by metabrokers

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: Creator.py,v 1.1 2005/12/30 18:51:39 evansde Exp $"
__author__ = "evansde@fnal.gov"

import popen2

from MB.MetaBroker import MetaBroker
from MB.FileMetaBroker import FileMetaBroker
from MB.creator.CreatorException import CreatorException

class Creator:
    """
    _Creator_

    Base class for Creator Objects, defines API
    for creating a file or dir based on the content of the
    MetaBroker instance it acts on.
    
    """
    def __init__(self):
        pass


    def __call__(self, mbInstance):
        """
        _Operator()_

        Define the action of the creator on a
        MetaBroker Object. The argument must be a
        MetaBroker instance, based on the
        keys in the MetaBroker, the appropriate
        creator method is called to create the appropriate
        location

        Args --

        - *mbInstance* : MetaBroker instance to be creatored
        
        """
        if not isinstance(mbInstance, MetaBroker):
            msg = "Non MetaBroker Instance passed to Creator:"
            msg +="%s" % self.__class__.__name__
            raise CreatorException(
                msg, 
                ClassInstance = self,
                BadInstance = mbInstance)
        
        testFile = isinstance(mbInstance, FileMetaBroker)
        testTarget = mbInstance['Target'] != None
        

        if testFile and testTarget:
            self.createFile(mbInstance)
            return 1
        if testTarget:
            self.createDir(mbInstance)
            return 1
        return 0



    def createFile(self, mbInstance):
        """
        _createFile_

        Override this method to create a file using the
        CreatorMethod this is implemented for

        """
        msg = "createFile not implemented for "
        msg += "%s object" % self.__class__.__name__
        raise CreationException(msg, ClassInstance = self)

    def createDir(self, mbInstance):
        """
        _createDir_

        Override this method to create a file using the creator
        method this object is implemented for
        """
        msg = "createDir not implemented for "
        msg += "%s object" % self.__class__.__name__
        raise CreationException(msg, ClassInstance = self)
        
        
    
        
    def runShellCommand(self, command):
        """
        _runShellCommand_

        Utility for running a shell command in a popen object
        and returning the return code.
        If the return code is non-zero, then a CreatorException
        is raised containing the output from the command

        """
        pop = popen2.Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        output = pop.fromchild.read()
        del pop
        if exitCode:
            msg = "Error running command:\n"
            msg += "Command:\n%s\n" % command
            msg += "Exited with Status: %s\n" % exitCode
            msg += "Output:\n%s\n" % output
            raise CreatorException(msg, ClassInstance = self,
                                   ExitCode = exitCode)
        return

        
        
        
        



    
        
        
        

        
