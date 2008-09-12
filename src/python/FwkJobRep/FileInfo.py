#!/usr/bin/env python
"""
_FileInfo_

Container object for file information.
Contains information about a single file as a dictionary

"""

from IMProv.IMProvNode import IMProvNode

class FileInfo(dict):
    """
    _FileInfo_

    Dictionary based container for holding details about a
    file.
    Majority of keys are key:string single value, however a
    few need to be list based

    """
    
    def __init__(self):
        dict.__init__(self)
        self.setdefault("LFN", None)
        self.setdefault("PFN", None)
        self.setdefault("GUID", None)
        self.setdefault("Size", None)
        self.setdefault("Checksum", None)
        self.setdefault("TotalEvents", None)

        #  //
        # // Input files is a list of LFNs each of which
        #//  should have an associated event range.
        self.inputFiles = {}

        #  //
        # // Dataset is a dictionary and will have the same key
        #//  structure as the MCPayloads.DatasetInfo object
        self.dataset = {}
        

        
    def addInputFile(self, lfn, firstEvent, lastEvent):
        """
        _addInputFile_

        Add an input file LFN and event range used as input to produce the
        file described by this instance.

        NOTE: May need to allow multiple ranges per file later on for skimming
        etc. However care must be taken to ensure we dont end up with event
        lists, since these will be potentially huge.

        """
        self.inputFiles[lfn] = {"FirstEvent" : firstEvent,
                                "LastEvent" : lastEvent}
        return
    
        

    def save(self):
        """
        _save_

        Return an improvNode structure containing details
        of this object so it can be saved to a file

        """
        improvNode = IMProvNode("File")
        #  //
        # // General keys
        #//
        for key, val in self.items():
            if val == None:
                continue
            node = IMProvNode(str(key), str(val))
            improvNode.addNode(node)

        #  //
        # // Inputs
        #//
        inputs = IMProvNode("Input")
        improvNode.addNode(inputs)
        for lfn, ranges in self.inputFiles.items():
            inpNode = IMProvNode("InputFile")
            inpNode.addNode(IMProvNode("LFN", str(lfn)))
            inpNode.addNode(IMProvNode("FirstEvent",
                                       str(ranges['FirstEvent'])))
            inpNode.addNode(IMProvNode("LastEvent", str(ranges['LastEvent'])))
            inputs.addNode(inpNode)
            
            

        #  //
        # // Dataset info
        #//
        dataset = IMProvNode("Dataset")
        improvNode.addNode(dataset)
        for key, val in self.dataset.items():
            dataset.addNode(IMProvNode(key, str(val)))

            
        return improvNode

    
