#!/usr/bin/env python


import logging
from MergeSensor.MergeSensorDB import MergeSensorDB
from MergeSensor.MergeSensorError import MergeSensorError, \
                                        InvalidDataTier, \
                                        InvalidDataset, \
                                        DatasetNotInDatabase, \
                                        DuplicateLFNError

from ProdCommon.FwkJobRep.ReportParser import readJobReport





class ReportHandler:

    def __init__(self, repFile):
        self.mergeDB = MergeSensorDB()
        self.reportFile = repFile
        self.datasetIds = {}
        [ self.datasetIds.__setitem__(x, self.mergeDB.getDatasetId(x))
          for x in self.mergeDB.getDatasetList() ]
        
        self.insertedLFNs = []
        self.duplicateLFNs = []
        self.unknownDatasets = []
        
    def __call__(self):
        """
        _operator()_

        """
        reps = readJobReport(self.reportFile)
        for rep in reps:
            self.handleReport(rep)

    def summarise(self):
        """
        _summarise_

        debug friendly statement of what was done

        """
        msg = "Report Handled: %s\n " % self.reportFile
        if len(self.unknownDatasets) > 0:
            msg += "Unknown Datasets:\n"
            for ds in self.unknownDatasets:
                msg += "=> %s\n" % ds
        if len(self.duplicateLFNs) > 0:
            msg += "Duplicate LFNS:\n"
            for lfn in self.duplicateLFNs:
                msg += "=> %s\n" % lfn
        msg += "Inserted LFNs:\n"
        for lfn in self.insertedLFNs:
            msg += "=> %s\n" % lfn

        return msg
                

    def handleReport(self, report):
        """
        _handleReport_
        
        
        """
        for ofile in report.files:
            datasets = set([ x.name() for x in ofile.dataset ])
            for dataset in datasets:
                if dataset not in self.datasetIds.keys():
                    self.unknownDatasets.append(dataset)
                    continue
                datasetId = self.datasetIds[dataset]
                runsList = [ {'RunNumber' : x } for x in ofile.runs ]
                try:
                    self.mergeDB.addFile(
                        datasetId, ofile['LFN'],
                        ofile['SEName'],
                        {
                        'FileSize' : ofile['Size'],
                        'NumberOfEvents' : ofile['TotalEvents'],
                        'RunsList' : runsList
                        })
                    self.mergeDB.commit()
                    self.insertedLFNs.append(ofile['LFN'])
                except DuplicateLFNError, ex:
                    msg = "Not registering duplicate unmerged file:\n"
                    msg += "%s\n" % ofile['LFN']
                    msg += str(ex)
                    logging.warning(msg)
                    self.mergeDB.rollback()
                    self.duplicateLFNs.append(ofile['LFN'])
                    continue
                

                
                



if __name__ == '__main__':

    reps = [
        "/home/evansde/work/PRODAGENT/work/JobCreator/RelValMinBias-170pre12/1012/FrameworkJobReport.xml",
        "/home/evansde/work/PRODAGENT/work/JobCreator/RelValMinBias-170pre12/5079/FrameworkJobReport.xml",
        
        
        ]

    for rep in reps:
        handler = ReportHandler(rep)
        handler()
        

        


