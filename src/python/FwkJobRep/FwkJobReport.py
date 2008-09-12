#!/usr/bin/env python
"""
_FwkJobReport_

Toplevel object for representing a Framework Job Report and
manipulating the bits and pieces of it.


"""

from FwkJobRep.FileInfo import FileInfo
from FwkJobRep.Message import Message

from IMProv.IMProvNode import IMProvNode


class FwkJobReport:
    """
    _FwkJobReport_

    Framework Job Report container and interface object

    """
    def __init__(self, name = None):
        self.name = name
        self.status = None
        self.jobSpecId = None
        self.workflowSpecId = None
        self.files = []
        self.errors = []
        self.exitCode = 0
        self.messages = []


    def wasSuccess(self):
        """
        _wasSuccess_

        Generate a boolean expression from this report to indicate if
        it comes from a successful job or not.

        This method will return True if:

        exitCode == 0 AND status = "Success"

        Otherwise it will return false

        """
        return (self.exitCode == 0) and (self.status == "Success") 
    

    def newMessage(self):
        """
        _newMessage_

        generate a new Message instance and return it, the instance
        will be an empty message instance that is added to the list of
        messages in this object

        """
        newMessage = Message()
        self.messages.append(newMessage)
        return newMessage
        

    def newFile(self):
        """
        _newFile_

        Insert a new file into the Framework Job Report object.
        Use an LFN to insert the file, returns a FwkJobRep.FileInfo
        object by reference that can be populated with extra details of
        the file.

        If LFN exists, returns None. (Should throw exception eventually)
        
        """
        fileInfo = FileInfo()
        self.files.append(fileInfo)
        return fileInfo
    

    def save(self):
        """
        _save_

        Save the Framework Job Report by converting it into
        an XML IMProv Object

        """
        result = IMProvNode("FrameworkJobReport")
        if self.name != None:
            result.attrs['Name'] = self.name
        if self.status != None:
            result.attrs['Status'] = str(self.status)
        if self.jobSpecId != None:
            result.attrs['JobSpecID'] = self.jobSpecId
        if self.workflowSpecId != None:
            result.attrs['WorkflowSpecID'] = self.workflowSpecId

        #  //
        # // Save ExitCode
        #//
        result.addNode(
            IMProvNode("ExitCode",
                       None,
                       Value = str(self.exitCode)
                       )
            )
        
        
        #  //
        # // Save Messages
        #//
        for message in self.messages:
            result.addNode(message.save())

        #  //
        # // Save Files
        #//
        for fileInfo in self.files:
            result.addNode(fileInfo.save())

        

        return result

    def write(self, filename):
        """
        _write_

        Write the job report to an XML file

        """
        handle = open(filename, 'w')
        handle.write(self.save().makeDOMElement().toprettyxml())
        handle.close()
        return
    

    def __str__(self):
        """strin representation of instance"""
        return str(self.save())
        
        
