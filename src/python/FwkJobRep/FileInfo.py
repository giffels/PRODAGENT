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
        self.setdefault("EventsRead", None)

        #  //
        # // Is this an input or output file?
        #//
        self.isInput = False

        #  //
        # //  open/closed state
        #//
        self.state = "closed"

        #  //
        # // Output files is a list of input files which contain
        #//  the LFN and PFN of all contributing inputs
        self.inputFiles = []

        #  //
        # // List of Branch names
        #//
        self.branches = []

        #  //
        # // List of Runs
        #//
        self.runs = []

        #  //
        # // Dataset is a dictionary and will have the same key
        #//  structure as the MCPayloads.DatasetInfo object
        self.dataset = {}
        
        
        
    def addInputFile(self, pfn, lfn):
        """
        _addInputFile_

        Add an input file LFN and event range used as input to produce the
        file described by this instance.

        NOTE: May need to allow multiple ranges per file later on for skimming
        etc. However care must be taken to ensure we dont end up with event
        lists, since these will be potentially huge.

        """
        self.inputFiles.append({"PFN" : pfn,
                                "LFN" : lfn})
        return
    
        

    def save(self):
        """
        _save_

        Return an improvNode structure containing details
        of this object so it can be saved to a file

        """
        if self.isInput:
            improvNode = IMProvNode("InputFile")
        else:
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
        # // State
        #//
        improvNode.addNode(IMProvNode("State", None, Value = self.state))

        #  //
        # // Inputs
        #//
        if not self.isInput:
            inputs = IMProvNode("Inputs")
            improvNode.addNode(inputs)
            for inputFile in self.inputFiles:
                inpNode = IMProvNode("Input")
                for key, value in inputFile.items():
                    inpNode.addNode(IMProvNode(key, value))
                inputs.addNode(inpNode)

        #  //
        # // Branches
        #//
        branches = IMProvNode("Branches")
        improvNode.addNode(branches)
        for branch in self.branches:
            branches.addNode(IMProvNode("Branch", branch))

        #  //
        # // Runs
        #//
        runs = IMProvNode("Runs")
        improvNode.addNode(runs)
        for run in self.runs:
            runs.addNode(IMProvNode("Run", run))

        #  //
        # // Dataset info
        #//
        if not self.isInput:
            dataset = IMProvNode("Dataset")
            improvNode.addNode(dataset)
            for key, val in self.dataset.items():
                dataset.addNode(IMProvNode(key, str(val)))

            
        return improvNode

    
