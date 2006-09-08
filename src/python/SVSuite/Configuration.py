#!/usr/bin/env python
"""
_Configuration_

Configuration object that describes a SVSuite Task.
Can be saved/loaded as XML and edited via python API

"""

import os


#  //
# // XML tools
#//
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery


from SVSuite.SVSuiteError import SVSuiteError


class Configuration:
    """
    _Configuration_

    Config object to define what happends in an SVSuite Task
    Data objects are public member data to keep editing simple.


    """
    def __init__(self):
        #  //
        # // List of input reference files to be staged in.
        #//
        self.stageIn = []

        #  //
        # // SW Setup command.
        #//  command to be invoked to provide a SCRAM runtime env.
        self.swSetupCommand = None
        self.swVersion = None
        self.jobId = None
        
        #  //
        # // data dir for input reference data
        #//
        self.svSuiteDataDir = None
        self.svSuiteOutputDir = None
        self.svSuiteBinDir = None
        self.svSuiteInputDir = None
        
        #  //
        # // list os SVSuite Tools to be run
        #//
        self.tools = []

        
        #  //
        # // Controls for how the task/tools will be executed
        #//
        self.zipOutput = False
        self.writeJobReport = False
        self.doStageIn = False
        

    #  //
    # // Persistency methods. Most users wont need to know about 
    #//  the content of any of these.        
    def save(self):
        """
        _save_

        Convert this object into an XML Node structure so that it can
        be saved

        """
        result = IMProvNode(self.__class__.__name__)
        for item in self.stageIn:
            result.addNode(IMProvNode("StageIn", item))

        if self.swSetupCommand != None:
            result.addNode(IMProvNode("SWSetupCommand",
                                      self.swSetupCommand.strip()))

        if self.swVersion != None:
            result.addNode(IMProvNode(
                "SWVersion", None, Value = self.swVersion)
                           )
        if self.jobId != None:
            result.addNode(IMProvNode(
                "JobID", None, Value = self.jobId)
                           )                  
        if self.svSuiteDataDir != None:
            result.addNode(IMProvNode("SVSUITE_DATA_DIR", None,
                                      Value = self.svSuiteDataDir))
        if self.svSuiteOutputDir != None:
            result.addNode(IMProvNode("SVSUITE_OUTPUT_DIR", None,
                                      Value = self.svSuiteOutputDir))
        if self.svSuiteBinDir != None:
            result.addNode(IMProvNode("SVSUITE_BIN_DIR", None,
                                      Value = self.svSuiteBinDir))
        if self.svSuiteInputDir != None:
            result.addNode(IMProvNode("SVSUITE_INPUT_DIR", None,
                                      Value = self.svSuiteInputDir))

        result.addNode(IMProvNode("ZipOutput", None, Value = self.zipOutput))
        result.addNode(IMProvNode("WriteJobReport", None, Value = self.writeJobReport))
        result.addNode(IMProvNode("DoStageIn", None, Value = self.doStageIn))
        
        for tool in self.tools:
            result.addNode(IMProvNode("SVSuiteTool", None, Value = tool))
            
        return result


    def load(self, improvNode):
        """
        _load_

        Extract data and populate this instance from the XML Node structure
        provided

        """
        stageInQ = IMProvQuery("Configuration/StageIn[text()]")
        for item in stageInQ(improvNode):
            self.stageIn.append(item.strip())

        swQ = IMProvQuery("Configuration/SWSetupCommand[text()]")
        swResult = swQ(improvNode)
        if len(swResult) > 0:
            self.swSetupCommand = swResult[0].strip()

        versQ = IMProvQuery("Configuration/SWVersion[attribute(\"Value\")]")
        versResult = versQ(improvNode)
            
        if len(versResult) > 0:
            self.swVersion = versResult[0]

        idQ = IMProvQuery("Configuration/JobID[attribute(\"Value\")]")
        idResult = idQ(improvNode)
        if len(idResult) > 0:
            self.jobId = str(versResult[0])
            
            

        attrs = {
            "SVSUITE_DATA_DIR" : "svSuiteDataDir",
            "SVSUITE_OUTPUT_DIR" : "svSuiteOutputDir",
            "SVSUITE_BIN_DIR": "svSuiteBinDir",
            "SVSUITE_INPUT_DIR" : "svSuiteInputDir",
            }
        
        for node in attrs.keys():
            query = IMProvQuery(
                "Configuration/%s[attribute(\"Value\")]" % node)
            result = query(improvNode)
            if len(result) > 0:
                setattr(self, attrs[node],  str(result[0]))

        controls = {
            "ZipOutput" : "zipOutput",
            "WriteJobReport" : "writeJobReport",
            "DoStageIn" : "doStageIn",
            }
        for control in controls.keys():
            query = IMProvQuery(
                "Configuration/%s[attribute(\"Value\")]" % control)
            result = query(improvNode)
            if len(result) > 0:
                resValue = result[0]
                if resValue in ("True", "1", "true", "TRUE", 1):
                    setattr(self, controls[control],  True)
                else:
                    setattr(self, controls[control],  False)
        toolQ = IMProvQuery(
            "Configuration/SVSuiteTool[attribute(\"Value\")]")
        for item in toolQ(improvNode):
            self.tools.append(item)
            
        return


    def write(self, filename):
        """
        _write_

        Save this object to the filename provided

        """
        doc = IMProvDoc("SVSuite")
        doc.addNode(self.save())
        handle = open(filename, 'w')
        handle.write(doc.makeDOMDocument().toprettyxml())
        handle.close()
        return


    def read(self, filename):
        """
        _read_

        Read the file provided and load the contents into this
        object

        """
        try:
            improvNode = loadIMProvFile(filename)
        except Exception, ex:
            msg = "Unable to read SVSuite configuration file :\n"
            msg += "%s\n" % filename
            msg += str(ex)
            raise SVSuiteError(msg, ClassInstance = self,
                               Filename = filename)

        self.load(improvNode)
        return
        
    def __str__(self):
        """string rep of this instance for debug/printing"""

        return str(self.save())
        
        
