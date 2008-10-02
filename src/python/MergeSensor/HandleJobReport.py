#!/usr/bin/env python
#pylint: disable-msg=C0103

"""
_HandleJobReport_
This module contains processing and merge framework job report handlers. It takes and instance of FwkJobReport and
process it...
"""

import logging
import os
from MergeSensor.MergeSensorDB.Interface.MergeSensorDB import MergeSensorDB
from MergeSensor.MergeSensorError import DuplicateLFNError
from ProdAgent.WorkflowEntities import File

class HandleJobReport :
    """
    _HandleJobReport_
    """
    def __init__ (self, reportInstance, reportFile, maxInputAccessFailures, enableMergeHandling = False):
        """
        _init_
        Initialization function
        """

        #// Merge DB Instance
        self.mergeDB = MergeSensorDB()

        self.reportFile = reportFile

        #// Fetch all the dataset list from Merge DB
        self.datasetIds = {}
        [ self.datasetIds.__setitem__(x, self.mergeDB.getDatasetId(x))
          for x in self.mergeDB.getDatasetList() ]

        #// Attributes to hold respective information
        self.insertedLFNs = []
        self.duplicateLFNs = []
        self.unknownDatasets = []
        self.removedLFNs = []
        self.unremovedLFNs = []
        self.enableMergeHandling = enableMergeHandling
        self.maxInputAccessFailures = maxInputAccessFailures
        self.reportInstance = reportInstance
     #   self.reportFile = reportFile
        self.jobName = None
        self.mergedLfn = None


        return   #// END init

    def __call__ (self):
        """
        Making that class callable
        """
        result = None
        #// Read job report file from the jobreport location provided
        result  = self.initialiseReport()
        if result is False:
           return None



        try:
          #// Handle Processing jobs's report
          if self.jobName.find ('merge') == -1:
             self.handleProcessingJobReport()
             return self.jobName

          #// Handle Merge Job Report. Insert & update Merge DB

          if self.enableMergeHandling:
              self.handleMergeJobReport ()

          return self.jobName

        except Exception, ex:

             msg = "Failed to handle job report from processing job:\n"
             msg += "%s\n" % self.reportFile
             msg += str(ex)
             logging.error(msg)
             return self.jobName




    def initialiseReport(self):
        """
        _initialiseReport_

        Function that read job and loads job report from the job report file provided
        """

        # get job name from first report (should be only one)
        try:

            self.jobName = self.reportInstance.jobSpecId

        # if cannot be done, signal error
        except Exception, msg:

            logging.error("Cannot process JobSuccess event for %s: %s" \
                          % (self.reportFile, msg))
            return False

        # get output file (should be only one)
        try:
            outputFiles = self.reportInstance.files
            for outputFile in outputFiles:
                self.mergedLfn = outputFile['LFN']

        # if cannot be done, signal error
        except Exception, msg:

            logging.error("Cannot process JobSuccess event for %s: %s" \
                            % (self.reportFile, msg))
            return False

        return True  #//END initialiseReport

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

        if len(self.removedLFNs) > 0:
            msg += "Removed LFNs:\n"
            for lfn in self.removedLFNs:
                msg += "=> %s\n" % lfn

        if len(self.unremovedLFNs) > 0:
            msg += "Unremoved LFNs:\n"
            for lfn in self.unremovedLFNs:
                msg += "=> %s\n" % lfn


        return msg   #//END summarise


    def handleProcessingJobReport (self):
        """
        _handleProcessingJobReport_
        Funtion that actually registers the respective job report information in the Merge DB and update the job status as required in MergeDB


        """
        logging.info("Handling processing job: %s" % self.jobName)

        logging.info('handle report...')

        #// update romoved file status
        removedFiles = self.reportInstance.removedFiles.keys()
        if len(removedFiles) > 0:
            self.mergeDB.removedState(*removedFiles)
            self.mergeDB.commit()
            self.removedLFNs.extend(removedFiles)

        #//update unremoved file status
        unremovedFiles = self.reportInstance.unremovedFiles.keys()
        if len(unremovedFiles) > 0:
            self.mergeDB.unremovedState(*unremovedFiles)
            self.mergeDB.commit()
            self.unremovedLFNs.extend(unremovedFiles)



        #// Read output files from the job report
        for ofile in self.reportInstance.files:

            #// gets the associated dataset info
            datasets = set([ x.name() for x in ofile.dataset ])
            for dataset in datasets:
                if dataset not in self.datasetIds.keys():
                    self.unknownDatasets.append(dataset)
                    continue
                datasetId = self.datasetIds[dataset]


                logging.info('output lumi Run Info')
                logging.info(ofile.runs)
                logging.info(ofile.getLumiSections())


                try:

                    #// passing the LFN, SEName and lumisections info to MergeSensorDB

                    inputId = self.mergeDB.addFile(
                        datasetId, ofile['LFN'],
                        ofile['SEName'],
                        {
                        'FileSize' : ofile['Size'],
                        'NumberOfEvents' : ofile['TotalEvents']

                        })

                    #// Adding output lumi info
                    outputLumi = []


                    self.mergeDB.addLumiInfo (ofile.getLumiSections(), inputId)

                    #// Adding input lumi info

                    for ifile in self.reportInstance.inputFiles:
                        self.mergeDB.addInputLumiInfo(ifile.getLumiSections(), inputId)
                        logging.info('input lumi Run Info')
                        logging.info(ifile.runs)
                        logging.info(ifile.getLumiSections())

                    self.mergeDB.commit()
                    self.insertedLFNs.append(ofile['LFN'])
                    logging.info('All merge data inserted successfully')

                except DuplicateLFNError, ex:
                    msg = "Not registering duplicate unmerged file:\n"
                    msg += "%s\n" % ofile['LFN']
                    msg += str(ex)
                    logging.warning(msg)
                    self.mergeDB.rollback()
                    self.duplicateLFNs.append(ofile['LFN'])
                    continue


        return   #// End handleProcessingJobReport



    def handleMergeJobReport (self):
        """
        _handleMergeJobReport_

        """

        logging.info("Handling Merge job: %s" % self.jobName)



        # get skipped files
        skippedFiles = [aFile['Lfn'] for aFile in self.reportInstance.skippedFiles]

        # open a DB connection
        database = MergeSensorDB()

        # start a transaction
        database.startTransaction()

        # get job information
        try:
            jobInfo = database.getJobInfo(self.jobName)

        # cannot get it!
        except Exception, msg:
            logging.error("Cannot process JobSuccess event for job %s: %s" \
                  % (self.jobName, msg))
            database.closeDatabaseConnection()
            return

        # check that job exists
        if jobInfo is None:
            logging.error("Job %s does not exists." % self.jobName)
            database.closeDatabaseConnection()
            return

        # check status
        if jobInfo['status'] != 'undermerge':
            logging.error("Cannot process JobSuccess event for job %s: %s" \
                  % (self.jobName, "the job is not currently running"))
            database.closeDatabaseConnection()
            return

        # get dataset id
        datasetId = database.getDatasetId(jobInfo['datasetName'])

        # update input files status
        finishedFiles = []
        unFinishedFiles = []

        for fileName in jobInfo['inputFiles']:

            if fileName not in skippedFiles:

                # set non skipped input files as 'merged'
                database.updateInputFile(datasetId, fileName, status="merged")

                # add to the list of finished files
                finishedFiles.append(fileName)

            else:

                # increment failure counter for skipped input files
                newStatus = database.updateInputFile( \
                       datasetId, fileName, \
                       status = "unmerged", \
                       maxAttempts = int(self.maxInputAccessFailures))

                # add invalid files to list of non finished files
                if newStatus == 'invalid':
                   unFinishedFiles.append(fileName)

        # mark output file as 'merged'
        database.updateOutputFile(datasetId, jobName=self.jobName, \
                                  status='merged', lfn = self.mergedLfn)

        # commit changes
        database.commit()

        # notify the PM
        File.merged(finishedFiles)
        if len(unFinishedFiles) > 0:
            File.merged(unFinishedFiles, True)

        # log messages
        logging.info("Job %s finished succesfully, file information updated." \
                     % self.jobName)

        if len(skippedFiles) > 0:
            logging.info("*** Warning: the files: " + str(skippedFiles) + \
                         " were skipped")

        # close connection
        database.closeDatabaseConnection()

        return #// End handleMergeJobReport



