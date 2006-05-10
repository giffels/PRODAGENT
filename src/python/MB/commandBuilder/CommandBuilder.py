#!/usr/bin/env python
"""
_CommandBuilder_

Interface class for CommandBuilder objects.

Developers should implement this class for a particular system
and then have the implementation register itself with the
CommandBuilder factory.

"""

from MB.MBException import MBException


class CommandBuilder:
    """
    _CommandBuilder_

    Interface class that defines the basic interfaces required
    by CommandBuilder implementations

    """

    def __init__(self):
        pass



    def transportSourceToCurrent(self, mbInstance):
        """
        _transportSourceToCurrent_

        Create a command that transfers the file indicated by the
        Source values in the MB instance to the Current values 

        """
        raise MBException("Method not implemented", ClassInstance = self)



    def transportCurrentToTarget(self, mbInstance):
        """
        _transportCurrentToTarget_

        Create a command that transfers the file indicated by the
        current values in the MB instance to the Target values
        """
        raise MBException("Method not implemented", ClassInstance = self)

    def transportSourceToTarget(self, mbInstance):
        """
        _transportSourceToTarget_

        Create a command that performs a third party like transfer
        of the source file to the target
        
        """
        raise MBException("Method not implemented", ClassInstance = self)




    def sourceExists(self, mbInstance):
        """
        _sourceExists_

        Create a command that returns true is the Source exists, and
        false if it doesnt
        """
        raise MBException("Method not implemented", ClassInstance = self)



    def targetExists(self, mbInstance):
        """
        _targetExists_

        Create a command that returns true if the target exists
        """
        raise MBException("Method not implemented", ClassInstance = self)



    def currentExists(self, mbInstance):
        """
        _currentExists_

        Create a command that returns true if the current values
        exist
        """
        raise MBException("Method not implemented", ClassInstance = self)




    def createTargetDir(self, mbInstance):
        """
        _createTargetDir_

        return a command that creates a directory based on the Target
        values of the MB instance
        """
        raise MBException("Method not implemented", ClassInstance = self)

    

    def createTargetFile(self, mbInstance):
        """
        _createTargetFile_

        return a command thet creates an empty file based on the Target
        values of the MB Instance
        """
        raise MBException("Method not implemented", ClassInstance = self)


    
    def targetURL(self, mbInstance):
        """
        _targetURL_

        Create a URL for the target based on the Access Protocol being used
        """
        raise MBException("Method not implemented", ClassInstance = self)

    def sourceURL(self, mbInstance):
        """
        _sourceURL_

        Create a URL for the source based on the Access Protocol being used
        """
        raise MBException("Method not implemented", ClassInstance = self)
    
    def currentURL(self, mbInstance):
        """
        _currentURL_

        Create a URL for the current values based on the Access Protocol
        being used

        """
        raise MBException("Method not implemented", ClassInstance = self)
        
    
    def deleteCurrent(self, mbInstance):
        """
        _deleteCurrent_

        Take the current values and generate a command to delete that file

        """
        raise MBException("Method not implemented", ClassInstance = self)
