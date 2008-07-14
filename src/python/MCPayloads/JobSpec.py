#!/usr/bin/env python
"""
_JobSpec_

Container class for a tree of JobSpecNodes representing a
concrete job definition

"""

import time

from MCPayloads.JobSpecNode import JobSpecNode
import MCPayloads.DatasetTools as DatasetTools
import MCPayloads.AppVersionTools as AppVersionTools

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery


class JobSpec:
    """
    _JobSpec_

    JobSpecNode tree toplevel container

    """
    def __init__(self):
        self.payload = JobSpecNode()
        self.parameters = {}
        self.parameters.setdefault("JobName", "Job-%s" % time.time())

        
    def setJobName(self, jobName):
        """
        set the name for this job

        """
        self.parameters['JobName'] = jobName
        updateJobName(self.payload, jobName)
        return
      
    def makeIMProv(self):
        """
        _makeIMProv_

        Serialise the WorkflowSpec instance into an XML IMProv structure

        """
        node = IMProvNode("JobSpec")
        for key, val in self.parameters.items():
            paramNode = IMProvNode("Parameter", str(val), Name = str(key))
            node.addNode(paramNode)
        payload = IMProvNode("Payload")
        payload.addNode(self.payload.makeIMProv())
        node.addNode(payload)
        return node
    

    def save(self, filename):
        """
        _save_

        Save this workflow spec into a file using the name provided

        """
        handle = open(filename, 'w')
        handle.write(self.makeIMProv().makeDOMElement().toprettyxml())
        handle.close()
        return  
    

    def load(self, filename):
        """
        _load_

        Load a saved JobSpec object and install its information
        into this instance

        """
        node = loadIMProvFile(filename)
        paramQ = IMProvQuery("/JobSpec/Parameter")
        payloadQ = IMProvQuery("/JobSpec/Payload/JobSpecNode")

        #  //
        # // Extract Params
        #//
        paramNodes = paramQ(node)
        for item in paramNodes:
            paramName = item.attrs.get("Name", None)
            if paramName == None:
                continue
            paramValue = str(item.chardata)
            self.parameters[str(paramName)] = paramValue


        #  //
        # // Extract Payload Nodes
        #//
        payload = payloadQ(node)[0]
        self.payload = JobSpecNode()
        self.payload.populate(payload)
        return

    #  //
    # // Accessor methods for retrieving dataset information
    #//
    def outputDatasets(self):
        """
        _outputDatasets_

        returns a list of MCPayload.DatasetInfo objects (essentially
        just dictionaries) containing all of the output datasets
        in all nodes of this JobSpec including details of output modules
        (Catalog etc) if there is a matching output module for each dataset

        Note that this method overrides the outputDatsets method in
        PayloadNode. It returns the same information as that method but
        augmented with output module details from the configuration

        """
        return DatasetTools.getOutputDatasetDetailsFromTree(self.payload)

    
    def listApplicationVersions(self):
        """
        _listApplicationVersions_

        Traverse all of this JobSpec instances nodes and retrieve the
        Application versions from each node and return a list of all
        of the unique Version strings

        Eg if there are three nodes all using CMSSW_X_X_X, then there
        will be a single entry for that version
        
        """
        return AppVersionTools.getApplicationVersions(self.payload)
        

def updateJobName(jobSpecNode, jobNameVal):
    """
    _updateJobName_

    Propagate JobName down to all JobSpec nodes in the tree

    """
    jobSpecNode.jobName = jobNameVal
    for child in jobSpecNode.children:
        updateJobName(child, jobNameVal)
    return

