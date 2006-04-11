#!/usr/bin/env python

"""
_DBS_Ws_

Class to extract/insert info from DBS
based on web service DBS API
 
"""
import os, string,binascii,exceptions
import dbsWsApi
import dbsException
import dbsApi
from dbsClientDatastructures import DbsPrimaryDataset
from dbsClientDatastructures import DbsApplication
from dbsClientDatastructures import DbsProcessingPath
from dbsClientDatastructures import DbsProcessedDataset
from dbsClientDatastructures import DbsBlock
from dbsClientDatastructures import DbsFile
from dbsClientDatastructures import DbsEventCollection

# ##############
class DBS_Ws:
  """
  _DBS_Ws_

  interface to extract/insert info from DBS
  """
# ##############
  def __init__(self, instannce):
    """
     Construct api object.
    """
    ## define wsdl URL location: nolonger needed with DBS API tag vs20060320
    #DBSConfig=os.path.join(DBSAPILocation,"DbsDatasetService.wsdl.xml")

    ## web service API
    #old self.api = dbsWsApi.DbsWsApi(wsdlUrl=DBSConfig)
    self.api = dbsWsApi.DbsWsApi()
    ## set log level
    #self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_ALL_)
    self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_INFO_)
    #self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_QUIET_)

# ##############
  def insertPrimaryDataset(self, PrimDatasetName): 
    """
     create a primary dataset with DBS API createPrimaryDataset
    """

    # Define Primary dataset object: 
    primdataset = DbsPrimaryDataset(name=PrimDatasetName)
    # Create Primary dataset
    primaryDatasetId=0
    print " primaryDatasetId  = self.api.createPrimaryDataset(primdataset) "
    primaryDatasetId = self.api.createPrimaryDataset(primdataset)

    return primaryDatasetId

# ##############
  def insertProcessedDataset(self, datasetinfo, datatier, applicationfamily):
    """
     create a processed dataset with DBS API createProcessedDataset + empty fileblock
    """
    ## Define the PSetHash from: EdmConfigHash <  Gen-Sim.cfg
    print " - PSetHash %s"%datasetinfo['PSetHash']
    PSet=datasetinfo['PSetHash']

    app = DbsApplication(
           family=applicationfamily,
           executable=datasetinfo['ApplicationName'],
           version=datasetinfo['ApplicationVersion'],
           #old configConditionsVersion ="Testabcd",
           parameterSet=PSet)

    processingPath = DbsProcessingPath(
           dataTier=datatier,
           ## PROVENANCE: adding parent proccessing path:
           #parentPath=parentprocessingPath
           application=app)

    ## Define the ProcessedDataset object
    datasetPath="/%s/%s/%s"%(datasetinfo['PrimaryDataset'],datatier,datasetinfo['ProcessedDataset'])
    #print "Defining %s"%datasetPath
    dataset = DbsProcessedDataset(
            primaryDatasetName=datasetinfo['PrimaryDataset'],
            isDatasetOpen="y",
            processedDatasetName=datasetinfo['ProcessedDataset'],
            #old datasetName=datasetinfo['ProcessedDataset'],
            processingPath=processingPath)

    # Create Processed dataset
    procDatasetId=0
    print " createProcessedDataset for %s"%datasetPath
    procDatasetId = self.api.createProcessedDataset(dataset)

    ## Define FileBlock object: empty block, to be updated when inserting files
    block = DbsBlock(
        #blockName="dummyname",      
        #old processedDatasetName=ProcDatasetName,
        blockStatusName="Dummy Block Status",
        numberOfBytes=0,
        numberOfFiles=0
    )

    # Create FileBlock associated to the dataset: create the block upfront since 1 fileblock= 1 dataset 
    datasetPath="/"+dataset._primaryDatasetName+"/"+datatier+"/"+dataset._processedDatasetName

    #Enforce 1-1 mapping between fileblock and adataset: check that if a fileblock already exists the fileblock creation is skipped => create the fileblock only if fileBlockList is empty
    fileBlockList = self.api.getDatasetFileBlocks(datasetPath)
    if fileBlockList == None:
      #print "creating the fileblock since there are no fileblock for this dataset"
      print " createFileBlock for dataset %s"%datasetPath
      fbId = self.api.createFileBlock(datasetPath, block)

    return procDatasetId

# ##############
  def insertFiletoBlock(self,fileinfo,fileblock):
    """
    """
    #print "Insering event collection/file with:"
    #print "  LFN %s"%fileinfo['LFN']
    #print "  GUID %s"%fileinfo['GUID']
    #print "  Checksum %s"%fileinfo['Checksum']
    #print "  Size %s"%fileinfo['Size']

    fileBlockId = fileblock._blockId

    outfile = DbsFile(logicalFileName=fileinfo['LFN'],
              fileStatus = "file dummy status",
              guid =fileinfo['GUID'],
              #old checkSum=fileinfo['Checksum'],
              fileType="dummy",
              fileBlockId=fileBlockId,
              fileSize=int(fileinfo['Size']))
    fList=[outfile]
    return fList

# ##############
  def setEVCollection(self,fileinfo,fList,datasetPath):
    """
    """
    collectionIndex=self.getEVCIndex(fileinfo['LFN'])
    ## event collections (1 event collection = 1 file) : NO PROVENANCE
    ec = DbsEventCollection(
              collectionName=fileinfo['LFN'],
              numberOfEvents=int(fileinfo['TotalEvents']),
              collectionIndex=collectionIndex,
              #new: datasetPathName
              datasetPathName=datasetPath,
              fileList=fList)

    ecList = [ec]

    return ecList

# ##############
  def getEVCIndex(self,LFN):
    """
     get an integer from the LFN
    """
    index=binascii.crc32(LFN)
    return index

# ##############
  def insertEVCtoDataset(self, datasetPath, evcList ): 
    """
     insert an event collection/file to a dataset with DBS API insertEventCollections
    """    
    # insert evc to the dataset
    print " inserting event collections"
    self.api.insertEventCollections(evcList)

# ##############
  def getDatasetContents(self,dbspath):
    """
     query DBS to get fileblocks and event collections  with DBS API getDatasetContents
    """
    fileBlockList = self.api.getDatasetContents(dbspath)
    if fileBlockList != None:
     for fileBlock in fileBlockList:
       ## get the event collections for each block
       print "Fileblock "+fileBlock._blockName
       eventCollectionList = fileBlock._eventCollectionList
       nevts=0
       for eventCollection in eventCollectionList:
          print "  - eventcollection: "+eventCollection._collectionName+" nb.ofevts: %i"%eventCollection._numberOfEvents
          #print "  %s" % eventCollection
          nevts=nevts+eventCollection._numberOfEvents

    return fileBlockList

# ##############
  def getDatasetFileBlocks(self,dbspath):
    """
     query DBS to get fileblocks 
    """
    #try:
    #   fileBlockList = self.api.getDatasetFileBlocks(dbspath)
    #except dbsException.DbsException, ex:
    #  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
    fileBlockList = self.api.getDatasetFileBlocks(dbspath)

    return fileBlockList

##############################################################################
# Unit testing.
                                                                                
if __name__ == "__main__":
  try:

        dbsinfo = DBS_Ws("")

        ## create primary dataset
        primarydataset="Test_PrimDataset1"
        PrimaryDatasetId=dbsinfo.insertPrimaryDataset(primarydataset)

        ## define application + processing path
        app = dbsApplication.DbsApplication(
        family="Test_simulation",
        executable="Test_cmsRun",
        version="Test_CMSSW_X_Y_Z",
        configConditionsVersion ="Testabcd",
        parameterSet="Test_psetdummy",
        outputTypeName="Test_Hit",
        inputTypeName="idummy")
                                                                                
        processingPath = dbsProcessingPath.DbsProcessingPath(
        dataTier="Test_Hit",
        application=app)

        ## create processed dataset ( + empty fileblock associated to it)
        processeddataset="Test_ProcDataset1"
        ProcessedDatasetId=dbsinfo.insertProcessedDataset(processingPath,primarydataset,processeddataset)

        print "Processed dataset Id "
        print  ProcessedDatasetId

        ## add event collection ....

  except dbsException.DbsException, ex:
    print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
  print "Done"


