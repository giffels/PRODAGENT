#!/usr/bin/env python
"""
_FileCrossCheck_


Tools for cross checking file lists in merge sensor, DBS etc

"""


import ProdAgent.WorkflowEntities.Utilities as WEUtils
import ProdAgent.WorkflowEntities.Workflow as WEWorkflow

from MergeSensor.MergeSensorDB import MergeSensorDB
from MergeSensor.MergeSensorError import MergeSensorError, \
                                        InvalidDataTier, \
                                        InvalidDataset, \
                                        DatasetNotInDatabase




class MergeSensorCrossCheck:
    """
    _MergeSensorCrossCheck_

    Object representing a cross check for the data in
    the merge sensor for a particular workflow

    Provide an API for extracting the dataset information
    from the merge sensor
    
    """
    def __init__(self, datasetName):
        self.dataset = datasetName
        self.mergeDB = MergeSensorDB()
        self.datasetInfo = self.mergeDB.getDatasetInfo(self.dataset)
        self.datasetId = self.mergeDB.getDatasetId(self.dataset)


    def __del__(self):
        self.mergeDB.closeDatabaseConnection()
        

    def getFiles(self):
        """
        _getFiles_

        Extract a list of files

        """
        filelist = self.mergeDB.getFileList(self.datasetId)
        return [ x['name'] for x in filelist]
        
        

    def getPendingUnmergedFiles(self):
        """
        _getPendingUnmergedFiles_

        Get a list of unmerged files waiting to be merged

        """
        filelist = self.mergeDB.getUnmergedFileList(self.datasetId)
        return [ x['name'] for x in filelist]
        
    def getFileMap(self):
        """
        _getFileMap_

        Get mapping of unmerged LFN to merged LFN or None

        """
        return self.mergeDB.getDatasetFileMap(self.datasetId)
    
    
        
