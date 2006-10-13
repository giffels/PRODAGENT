#!/usr/bin/env python
"""
_FwkJobReport_

Toplevel object for representing a Framework Job Report and
manipulating the bits and pieces of it.


"""

from FwkJobRep.FileInfo import FileInfo

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
        self.jobType = None
        self.workflowSpecId = None
        self.files = []
        self.inputFiles = []
        self.errors = []
        self.skippedEvents = []
        self.skippedFiles = []
        self.exitCode = 0
        self.siteDetails = {}
        self.timing = {}
        self.storageStatistics = None

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
    


    def newFile(self):
        """
        _newFile_

        Insert a new file into the Framework Job Report object.
        returns a FwkJobRep.FileInfo
        object by reference that can be populated with extra details of
        the file.
        
        """
        fileInfo = FileInfo()
        self.files.append(fileInfo)
        return fileInfo

    def newInputFile(self):
        """
        _newInputFile_

        Insert an new Input File into this job report and return the
        corresponding FileInfo instance so that it can be populated

        """
        fileInfo = FileInfo()
        fileInfo.isInput = True
        self.inputFiles.append(fileInfo)
        return fileInfo

    def addSkippedEvent(self, runNumber, eventNumber):
        """
        _addSkippedEvent_

        Add a skipped event record run/event number pair
        """
        self.skippedEvents.append(
            {"Run" : runNumber, "Event" : eventNumber}
            )

        return

    def addSkippedFile(self, pfn, lfn):
        """
        _addSkippedFile_

        Add a skipped file record to this report
        """
        self.skippedFiles.append(
            { "Pfn" : pfn, "Lfn" : lfn}
            )
        return
        

    def addError(self, status, errType):
        """
        _addError_

        Add a new Error dictionary to this report, return it to be populated

        """
        newError = {"ExitStatus" : status,
                    "Type" : errType,
                    "Description": ""}
        self.errors.append(newError)
        return newError
    

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
        if self.jobType != None:
            result.attrs['JobType'] = self.jobType

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
        # // Save Site details
        #//
        for key, value in self.siteDetails.items():
            siteDetail = IMProvNode("SiteDetail", None,
                                    Parameter = key,
                                    Value = str(value))
        
            result.addNode(siteDetail)
        
        #  //
        # // Save Files
        #//
        for fileInfo in self.files:
            result.addNode(fileInfo.save())

        #  //
        # // Save Input Files
        #//
        for fileInfo in self.inputFiles:
            result.addNode(fileInfo.save())

        #  //
        # // Save Skipped Events
        #//
        for skipped in self.skippedEvents:
            result.addNode(IMProvNode("SkippedEvent", None,
                                      Run = skipped['Run'],
                                      Event = skipped['Event']))
        #  //
        # // Save Skipped Files
        #//
        for skipped in self.skippedFiles:
            result.addNode(IMProvNode("SkippedFile", None,
                                      Pfn = skipped['Pfn'],
                                      Lfn = skipped['Lfn']))
        
        #  //
        # // Save Errors
        #//
        for error in self.errors:
            result.addNode(
                IMProvNode("FrameworkError", error['Description'],
                           ExitStatus = error['ExitStatus'],
                           Type = error['Type'])
                )

        #  //
        # // Save Timing Info
        #//
        timing = IMProvNode("TimingService")
        result.addNode(timing)
        for key, value in self.timing.items():
            timing.addNode(IMProvNode(key, None, Value=str(value) ))

        #  //
        # // Save Storage Statistics
        #//
        if self.storageStatistics != None:
            result.addNode(
                IMProvNode("StorageStatistics", self.storageStatistics))
            
        
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
        
        
