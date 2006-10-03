#!/usr/bin/env python
"""
_SVSuiteSpec_

Util for defining a bunch of SVSuite tests in an XML File


"""


from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile

class SVSuiteSpec(dict):
    """
    _SVSuiteSpec_

    Serialisable object that defines the set of jobs required for
    an SVSuite Task
    
    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Name", None)
        self.setdefault("Type", None)  # Production/Skimming
        self.setdefault("NumberOfEvents", None)
        self.setdefault("NumberOfJobs", None)
        self.setdefault("InputDataset", None)
        self.setdefault("Configuration", None)
        self.setdefault("SaveCMSRunOutput", True)
        self.setdefault("SplitType", None)
        self.setdefault("SplitSize", None)
        self.referenceLfns = []
        self.tools = []
        
        self._Booleans = ["SaveCMSRunOutput"]


    def save(self):
        """
        _save_

        Serialise this instance into IMProvNodes

        """
        result = IMProvNode("SVSuiteSpec", None,
                            Name = self['Name'], Type = self['Type'])

        for key, value in self.items():
            if key in ("Name", "Type"):
                continue
            if value == None:
                continue
            node = IMProvNode("Parameter", None,
                              Name = key, Value = str(value))
            result.addNode(node)

        for item in self.referenceLfns:
            result.addNode(IMProvNode("ReferenceLFN", item))
        for item in self.tools:
            result.addNode(IMProvNode("Tool", None, Name = item))

        return result


    def load(self, improvNode):
        """
        _load_

        de-serialize from IMProvNode instance

        """
        self['Name'] = improvNode.attrs.get("Name", None)
        self['Type'] = improvNode.attrs.get("Type", None)
        
        paramQ = IMProvQuery("SVSuiteSpec/Parameter")
        reflfnQ = IMProvQuery("SVSuiteSpec/ReferenceLFN")
        toolQ = IMProvQuery("SVSuiteSpec/Tool")
                           
        params = paramQ(improvNode)
        reflfns = reflfnQ(improvNode)
        tools = toolQ(improvNode)

        for node in params:
            name = node.attrs.get("Name", None)
            val = node.attrs.get("Value", None)
            if name == None:
                continue
            if name in self._Booleans:
                val = self.convertToBool(val)

            self[name] = val

        for reflfn in reflfns:
            self.referenceLfns.append(str(reflfn.chardata))
            
        for tool in tools:
            toolName = tool.attrs.get("Name", None)
            if toolName == None:
                continue
            self.tools.append(str(toolName))

        return

    def __str__(self):
        return str(self.save())
        

        
    
    
    def convertToBool(self, value):
        """
        _convertToBool_

        Util method to convert a boolean value from string to
        bool type

        """
        if value in (1, "1", True, "True"):
            return True
        return False


def saveSpecFile(filename, *specs):
    """
    _saveSpecFile_

    Write the list of spec instances to the filename provided

    """
    doc = IMProvDoc("SVSuite")
    for spec in specs:
        doc.addNode(spec.save())
    handle = open(filename, 'w')
    handle.write(doc.makeDOMDocument().toprettyxml())
    handle.close()
    return

def loadSpecFile(filename):
    """
    _loadSpecFile_

    Read the Spec XML File and return a list of SVSuiteSpec instances

    """
    doc = loadIMProvFile(filename)
    query = IMProvQuery("SVSuite/SVSuiteSpec")
    specs = query(doc)

    results = []
    for spec in specs:
        newSpec = SVSuiteSpec()
        newSpec.load(spec)
        results.append(newSpec)

    return results
        
    
