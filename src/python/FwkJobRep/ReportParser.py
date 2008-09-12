#!/usr/bin/env python
"""
_ReportParser_

Read an XML Job Report into memory and create the appropriate FwkJobReport
instances and supporting objects.


"""

from xml.sax.handler import ContentHandler
from xml.sax import make_parser

from FwkJobRep.FwkJobReport import FwkJobReport



class FwkJobRepHandler(ContentHandler):
    """
    _FwkJobRepHandler_

    SAX Content Handler implementation to build 
    instances of FwkJobReport and populate it

    Multiple job reports in a file are supported, since the plan
    is to concatenate them at the end of a job from multiple outputs

    instances of FwkJobReport are stored in self.results as a list
    
    """
    def __init__(self):
        ContentHandler.__init__(self)
        #  //
        # // State containers used during parsing
        #//
        self.currentReport = FwkJobReport()
        self.currentMessage = None
        self.currentFile = None
        self.currentDict = None
        self._CharCache = ""

        #  //
        # // container for results
        #//
        self.results = []


        #  //
        # // Response methods to start of elements based on element name
        #//
        self._StartResponses = {
            "FrameworkJobReport" : self.newReport,
            "Report" : self.newMessage,
            "File" : self.newFile,
            "Dataset" : self.newDataset,
            "Item" : self.newItem,
            "ExitCode" : self.exitCode,
            }

        #  //
        # // Response methods to end of elements based on element name
        #//
        self._EndResponses = {
            "FrameworkJobReport" : self.endReport,
            "Report" : self.endMessage,
            "File" : self.endFile,
            "Dataset" : self.endDataset,
            "Item" : self.endItem,
            "Message" : self.noResponse,
            "ExitCode": self.noResponse,
            }

    def noResponse(self, name, attrs = {}):
        """some elements require no action"""
        pass
        
    def startElement(self, name, attrs):
        """
        _startElement_

        Override ContentHandler.startElement
        Start a new XML Element, call the appropriate response method based
        off the name of the element

        """
        response = self._StartResponses.get(name, self.noResponse)
        response(name, attrs)
        return

    def endElement(self, name):
        """
        _endElement_

        Override ContentHandler.endElement
        End of element, invoke response based on name and
        flush the chardata cache
        """
        response = self._EndResponses.get(name, self.fillDictionary)
        response(name)
        self._CharCache = ""
        return
        
    def characters(self, data):
        """
        _characters_

        Override ContentHandler.characters
        Accumulate character data from an xml element, if required
        the response will pick it up and insert it into the appropriate
        object.
        """
        if len(data.strip()) == 0:
            return
        self._CharCache += str(data).replace("\t", "")
        return

    def inMessage(self):
        """boolean test to see if state is in a message block"""
        return self.currentMessage != None

    def inFile(self):
        """boolean test to see if state is in a file block"""
        return self.currentFile != None
    

    def newReport(self, name, attrs):
        """
        _newReport_

        Handler method for a new FrameworkJobReport

        """
        self.currentReport = FwkJobReport()
        name =  attrs.get("Name", None)
        status = attrs.get("Status", None)
        jobSpec = attrs.get("JobSpecID", None)
        workSpec = attrs.get("WorkflowSpecID", None)
        if name != None:
            self.currentReport.name = str(name)
        if status != None:
            self.currentReport.status = str(status)
        if jobSpec != None:
            self.currentReport.jobSpecId = str(jobSpec)
        if workSpec != None:
            self.currentReport.workflowSpecId = str(workSpec)
        return

    def endReport(self, name):
        """
        _endReport_

        Handler Method for finishing a FrameorkJobReport
        """
        self.results.append(self.currentReport)
        self.currentReport = None
        return

    def newMessage(self, name, attrs):
        """ new verbose message tag"""
        self.currentMessage = self.currentReport.newMessage()
        self.currentDict = self.currentMessage
        return

    def endMessage(self, name):
        """end of verbose message tag"""
        self.currentMessage = None
        self.currentDict = None
        return

    def newFile(self, name, attrs):
        """new File tag encountered"""
        self.currentFile = self.currentReport.newFile()
        self.currentDict = self.currentFile
        
    def endFile(self, name):
        """ end of file tag encountered"""
        self.currentFile = None
        self.currentDict = None


    def newDataset(self, name, attrs):
        """ start of Dataset tag within a File tag"""
        if not self.inFile():
            return
        self.currentDict = self.currentFile.dataset
        return

    def endDataset(self, name):
        """end of Dataset tag"""
        if not self.inFile():
            return
        self.currentDict = self.currentFile

    def newItem(self, name, attrs):
        """ start of Item tag in verbose message, no action to take here"""
        return
        
    def endItem(self, name):
        """end of Item tag in verbose message, accumulate char data for
        current message"""
        if not self.inMessage():
            return
        self.currentMessage['Message'].append(str(self._CharCache))
        return
    
    def exitCode(self, name, attrs):
        """
        handle an ExitCode node, extract the value attr and add it to
        the current report

        """
        if self.currentReport == None:
            return

        value = attrs.get("Value", None)
        if value != None:
            self.currentReport.exitCode = int(value)
        return

        
    def fillDictionary(self, name):
        """
        _fillDictionary_

        Any object requiring population as a dictionary can use this
        handler to populate itself
        """
        if self.currentDict == None:
            return
        self.currentDict[str(name)] = str(self._CharCache)
        return
    

def readJobReport(filename):
    """
    _readJobReport_

    Load an XML FwkJobReport Document into a FwkJobReport instance.
    return the FwkJobReport instances.

    Instantiate a new ContentHandler and run it over the file producing
    a list of FwkJobReport instances
    
    """
    handler = FwkJobRepHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    parser.parse(filename)
    #print handler.results[0]
    return handler.results

