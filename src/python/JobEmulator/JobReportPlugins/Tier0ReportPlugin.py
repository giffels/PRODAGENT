#!/usr/bin/env python
"""
_Tier0ReportPlugin_

Plugin for the Job Emulator to generate job reports
upon job completion.  Successful reports are marked
as success while failure are marked a middleware
failures.

"""
__revision__ = "$Id: Tier0ReportPlugin.py,v 1.3 2008/08/05 15:47:18 sryu Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "sfoukes, sryu"

import logging

from random import randrange
from random import random
from random import gauss
from random import choice

from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetDetails
from ProdCommon.MCPayloads.MergeTools import getSizeBasedMergeDatasetsFromNode
from ProdCommon.MCPayloads.UUID import makeUUID

from JobEmulator.JobReportPlugins.JobReportPluginInterface \
     import JobReportPluginInterface
from JobEmulator.Registry import registerPlugin


class Tier0ReportPlugin(JobReportPluginInterface):
    """
    _EumlatorReportPlugin_

    Plugin for the Job Emulator to generate job reports
    upon job completion.  Successful reports are marked
    as success while failure are marked a middleware
    failures.

    """
    
    
    def createSuccessReport(self, jobSpecLoaded, workerNodeInfo, 
                            reportFilePath):
        """
        _createSuccessReport_

        Create a job report representing the successful completion
        of a job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.

        """
        jobSpecPayload, newReport = \
                self.__fwkJobReportCommon(jobSpecLoaded, workerNodeInfo)
        newReport.exitCode = 0
        newReport.status = "Success"
        
        # parse newReport.jobSpecId (it should contain Job Name
        # "Repack-Run%s-%s", "RepackMerge-Run%s-%s", "PromptReco-Run%s-%s"
        
        specIDParts = newReport.jobSpecId.split('-')
        
        tier0JobType = None
        if len(specIDParts) != 3:
            logging.debug("JobReport jobSpecID not in correct format for tier 0: %s" %
                          newReport.jobSpecId)
        else:
            # Job type should be one of "Repack", "RepackMerge", "PromptReco"
            tier0JobType = specIDParts[0].strip()
        
         
        if "jobId" in jobSpecLoaded.parameters.keys():
            newReport.jobSpecId = jobSpecLoaded.parameters["jobId"]

        # Create a list of datasets from the JobSpec
        # then associate file to these later on
        datasets = getOutputDatasetDetails(jobSpecPayload)
        datasets.extend(getSizeBasedMergeDatasetsFromNode(jobSpecPayload))
        outModules = jobSpecPayload.cfgInterface.outputModules

        inputFiles = jobSpecPayload.cfgInterface.inputFiles

        for dataset in datasets:
            modName = dataset.get('OutputModuleName', None)

            if outModules.has_key(modName):
                dataset['LFNBase'] = outModules[modName].get('LFNBase', None)
                self.setDefaultForNoneValue('LFNBase', dataset['LFNBase'])
                dataset['MergedLFNBase'] = \
                                outModules[modName].get('MergedLFNBase', None)

        datasetMap = {}
        for dataset in datasets:
            datasetMap[dataset['OutputModuleName']] = dataset

        for outName, outMod in \
                jobSpecPayload.cfgInterface.outputModules.items():

            theFile = newReport.newFile()
            guid = makeUUID()
            
            theFile['GUID'] = guid
            theFile['ModuleLabel'] = outName
            
            theFile.runs.append(jobSpecLoaded.parameters["RunNumber"])
            #check if the maxEvents['output'] iE    s set if not set totalEvent using maxEvents['input']
            totalEvent = jobSpecPayload.cfgInterface.maxEvents['output']
            if totalEvent == None:
                totalEvent = jobSpecPayload.cfgInterface.maxEvents['input']

            # if there is no input and output, print out error message and set default to 1000
            totalEvent = self.setDefaultForNoneValue(
                                           "maxEvent['input' and 'output']",
                                            totalEvent,
                                            100)

            try:
                totalEvent = int(totalEvent)
            except ValueError, ex:
                logging.error("totalEvent is not a number. \n%s" % ex)

            if (random() > self.avgEventProcessingRate):
                # Gauss distribution of totalEvent.
                meanEvent = int(totalEvent * 0.7)
                stdDev = totalEvent * 0.15
                tempTotalEvent = int(gauss(meanEvent,stdDev))
                if tempTotalEvent <= 0 :
                    totalEvent = 1
                elif tempTotalEvent >= totalEvent:
                    totalEvent = totalEvent - 1
                else:
                    totalEvent = tempTotalEvent

            #logging.debug("---------- Total Event ----------: %s \n" % totalEvent)
            theFile['TotalEvents'] = totalEvent

            theFile['SEName'] = workerNodeInfo['se-name']
            theFile['CEname'] = workerNodeInfo['ce-name']
            theFile['Catalog'] = outMod['catalog']
            theFile['OutputModuleClass'] = "PoolOutputModule"

            theFile.addChecksum("cksum", randrange(1000000, 10000000))
            theFile.branches.extend(["fakeBranch_%d-%s.Rec" % (num, guid)
                                  for num in range(randrange(5,20))])
            #theFile.load(theFile.save())

            [ theFile.addInputFile("fakefile:%s" % x , "%s" % x )
              for x in inputFiles ]

            
            if datasetMap.has_key(outName):
                datasetForFile = theFile.newDataset()
                datasetForFile.update(datasetMap[outName])

                # basic measurement is byte (minumum 4MB, max 4GB)
            
            # default value for the file size
            # it should be overridden if the primary dataset exist.
            # for the all other 
            theFile['Size'] = 4000000 * randrange(1, 1000) #random size
            theFile['MergedBySize'] = choice(["True", "False"])
            # setting up default LFN
            if outMod.has_key("LFNBase"):
                theFile['LFN'] = "%s%s.root" % (outMod['LFNBase'], guid)
            else:
                theFile['LFN'] = "/some/madeup/path/%s.root" % guid
                
            self.setDefaultForNoneValue('LFNBase', theFile['LFN'])
            
            if tier0JobType == "Repack":
                # parse dataset name set the size according to the threshold
                if len(theFile.dataset) == 0:
                    continue
            
                datasetNameParts = theFile.dataset[0]["PrimaryDataset"].split('_')
                # need to add sanity check
                if self.thresholdForMerge > int(datasetNameParts[2]):
                    theFile['Size'] = 500000000 #(500 MG)
                    theFile['MergedBySize'] = "False"
                else :
                    theFile['Size'] = 4000000000  #(4 G)
                    theFile['MergedBySize'] = "True"
                    #override LFN fro Merged file
                    theFile['LFN'] = "%s%s.root" % (outMod['MergedLFNBase'], guid)
                    
            elif tier0JobType == "RepackMerge":
                theFile['Size'] = 4000000000  #(4 G)
                theFile['MergedBySize'] = "True"
                    
            elif tier0JobType == "PromptReco": 
                theFile['Size'] = 2000000000  #(2 G)
            else :
                theFile['Size'] = 4000000 * randrange(1, 1000) #random size        
            
            theFile['PFN'] ="fakefile:%s" % theFile['LFN']
            
        newReport.write(reportFilePath)

        return newReport

    def createFailureReport(self, jobSpecLoaded, workerNodeInfo, reportFilePath):
        """
        _createFailureReport_

        Create a job report representing the failure of a job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.

        """
        jobSpecPayload, newReport = \
                    self.__fwkJobReportCommon(jobSpecLoaded, workerNodeInfo)
        newReport.status = "Fail"
        newReport.exitCode = 1
        err = newReport.addError(1, "RandomEmulatorError")
        errDesc = "Failure in JobEmulator Layer \n"
        err['Description'] = errDesc
        newReport.jobSpecId = jobSpecPayload.jobName

        newReport.write(reportFilePath)

        return newReport

    def __fwkJobReportCommon(self, jobSpecLoaded, workerNodeInfo):
        """
        __fwkJobReportCommon_

        Create a new job report and fill it in with generic
        information that is not dependent on the outcome of
        the job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.

        """
        #workerNodeInfo = RandomAllocationPlugin().allocateJob()

        try:
            jobSpecPayload = jobSpecLoaded.payload

            newReport = FwkJobReport()
            newReport.jobSpecId = jobSpecPayload.jobName
            newReport.jobType = jobSpecPayload.type
            newReport.workflowSpecId = jobSpecPayload.workflow
            newReport.name = jobSpecPayload.name
            #get information from the super class
            newReport.siteDetails['SiteName'] = workerNodeInfo['SiteName']
            #HostName is the same as worker_node name
            newReport.siteDetails['HostName'] = workerNodeInfo['HostName']
            newReport.siteDetails['se-name'] = workerNodeInfo['se-name']
            newReport.siteDetails['ce-name'] = workerNodeInfo['ce-name']
            return jobSpecPayload, newReport

        except Exception, ex:
            #msg = "Unable to Publish Report for %s\n" % jobSpecPayload.jobName
            #msg += "Since It is not known to the JobState System:\n"
            msg = str(ex)
            logging.error(msg)

            raise RuntimeError, msg


registerPlugin(Tier0ReportPlugin, Tier0ReportPlugin.__name__)

if __name__ == "__main__":
    from ProdCommon.MCPayloads.JobSpec import JobSpec
    jobSpec = JobSpec()
    jobSpec.load("/home/sryu")
