#!/usr/bin/env python

"""
_DBS_

Class to extract/insert info from DBS
based on CGI DBS API
 
"""
import os,string,exceptions
import dbsApi
from dbsCgiApi import DbsCgiApi, DbsCgiObjectExists
from dbsException import DbsException
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock
from dbsEventCollection import DbsEventCollection
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsProcessedDataset import DbsProcessedDataset
from dbsProcessing import DbsProcessing
from dbsApi import DbsApi, DbsApiException, InvalidDataTier, DBS_LOG_LEVEL_ALL_

# ##############
class DBS:
  """
  _DBS_

  interface to extract/insert info from DBS
  """
# ##############
  def __init__(self, dbinstance):
    """
     Construct api object.
    """
    ## cgi service API
    DEFAULT_URL = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/prodquery"
    args = {}
    args['instance']=dbinstance

    self.api = DbsCgiApi(DEFAULT_URL, args)
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
    primdataset = DbsPrimaryDataset(datasetName = PrimDatasetName) 
    # Create Primary dataset
    print " createPrimaryDataset %s"%PrimDatasetName
    try:
      self.api.createPrimaryDataset(primdataset)
    except DbsCgiObjectExists, ex:
      #print "Object existed already, passing"
      pass

    return

# ##############
  def insertProcessedDataset(self, datasetinfo, datatier, applicationfamily):
    """
     create a processed dataset with DBS API createProcessedDataset + empty fileblock
    """

    # Create processing
    processing=self.insertProcessing(datasetinfo, datatier, applicationfamily)

    # Create Processed datasets : one for each datatier
    primdataset = DbsPrimaryDataset(datasetName = datasetinfo['PrimaryDataset'])
    procdatasetName = datasetinfo['ProcessedDataset']
    tiers=string.split(datatier,'-')
    for tier in tiers:
       dataset=DbsProcessedDataset (primaryDataset=primdataset,
                                 datasetName=procdatasetName,
                                 dataTier=tier)
       try:
         print " createProcessedDataset %s for datatier %s"%(procdatasetName,tier)
         self.api.createProcessedDataset(dataset)
       except DbsCgiObjectExists, ex:
         #print "Object existed already, passing"
         pass

       # Create File Block (in the datatier loop for now)
       datasetPath="/%s/%s/%s"%(dataset.getPrimaryDataset().getDatasetName(),dataset.getDataTier(),dataset.getDatasetName())
       #print " datasetPath %s"%datasetPath
       self.insertFileBlock(datasetPath, processing)

    return 

# ##############
  def insertProcessing(self, datasetinfo, datatier, applicationfamily):
    """
     create a processing
    """

    PSet=datasetinfo['PSetHash']
    #PSetcont=datasetinfo['PSet']
    PSetcont=PSet # add PSet content or endcoded config later on
    ApplicationName=datasetinfo['ApplicationName']
    ApplicationVersion=datasetinfo['ApplicationVersion']
    procdatasetName=datasetinfo['ProcessedDataset']
                                                                                                    
    primdataset = DbsPrimaryDataset(datasetName = datasetinfo['PrimaryDataset'])
                                                                                                    
    processing = DbsProcessing (primaryDataset = primdataset,
                              processingName = procdatasetName,
                              applicationConfig = {
                                'application' : { 'executable' : ApplicationName,
                                                  'version' : ApplicationVersion,
                                                  'family' : applicationfamily },
                                'parameterSet' : { 'hash' : PSet,
                                                   'content' : PSetcont }})
    try:
      print " createProcessing "
      self.api.createProcessing(processing)
    except DbsCgiObjectExists, ex:
      #print "Object existed already, passing"
      pass

    return processing

# ##############
  def insertFileBlock(self, datasetPath, processing):
    """
      Create FileBlock associated to the dataset: create the block upfront since 1 fileblock= 1 dataset
    """
    #Enforce 1-1 mapping between fileblock and adataset: check that if a fileblock already exists the fileblock creation is skipped => create the fileblock only if fileBlockList is empty
    fileBlockList = self.api.getDatasetFileBlocks(datasetPath)
    if len(fileBlockList) == 0:
      #print "creating the fileblock since there are no fileblock for this dataset"
       block = DbsFileBlock (processing = processing)
       print " createFileBlock "
       try:
         self.api.createFileBlock(block)
       except DbsCgiObjectExists, ex:
         #print "Object existed already, passing"
         pass
    return

# ##############
  def insertFiletoBlock(self, fileinfo,fileblock):
    """
      insert files
    """

    #print "Insering file with:"
    #print "  LFN %s"%fileinfo['LFN']
    #print "  GUID %s"%fileinfo['GUID']
    #print "  Checksum %s"%fileinfo['Checksum']
    #print "  Size %s"%fileinfo['Size']

    outfile = DbsFile (logicalFileName=fileinfo['LFN'], 
                       fileSize=int(fileinfo['Size']),
                       checkSum="cksum:%s"%fileinfo['Checksum'],
                       guid=fileinfo['GUID'], 
                       fileType="EVD")
    fList=[outfile]
    print " insert files to block %s"%fileblock.getBlockName()
    try:
      self.api.insertFiles(fileblock, fList)
    except DbsCgiObjectExists, ex:
      #print "Object existed already, passing"
      pass
    except DbsCgiNoObject, ex:
      return

    return fList

# ##############
  def setEVCollection(self,fileinfo,fList,datasetPath): 
    """
     insert an event collection to a dataset with DBS API insertEventCollections
    """

    # event collection
    fileLFN=fileinfo['LFN']
    nameLFN=fileLFN.replace('.root','')
    events=int(fileinfo['TotalEvents'])
    #tier=dataset.getDataTier()
    tier=string.split(datasetPath,'/')[2]
    #print "tier %s"%tier

    parentList=[]
    ## parentage inserted by hand
    if (tier == "SIM") or (tier == "DIGI"):
        if tier == "SIM": parent_tier="GEN"
        if tier == "DIGI": parent_tier="SIM"
        parentname="%s_%s"%(parent_tier,nameLFN)
        parent_ec = DbsEventCollection (collectionName=parentname, 
                                       numberOfEvents=events, 
                                       fileList=fList)
        parentList=[{ 'parent' : parent_ec, 'type' : parent_tier }]

    name="%s_%s"%(tier,nameLFN)
    print " evc name : %s"%name
    ec = DbsEventCollection (collectionName=name, 
                             numberOfEvents=events,
                             fileList=fList,
                             parentageList=parentList)

    evcList = [ec]   
    return evcList

# ##############
  def insertEVCtoDataset(self,datasetPath,evcList):
    """
    """
    # dataset
    dataset= DbsProcessedDataset(datasetPath=datasetPath)

    print " insert EventCollections for dataset %s"%datasetPath
    try:
     self.api.insertEventCollections(dataset, evcList)
    except DbsCgiObjectExists, ex:
      #print "Object existed already, passing"
      pass

# ##############
  def getDatasetContents(self,dbspath):
    """
     query DBS to get fileblocks and event collections  with DBS API getDatasetContents
    """
#    try:
#      fileBlockList = self.api.getDatasetContents(dbspath)
#    except DbsException, ex:
#      print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
#      return []
## catch the exception in DBSComponent.py
    fileBlockList = self.api.getDatasetContents(dbspath)

    for fileBlock in fileBlockList:
      print "File block name: %s" % (fileBlock.getBlockName())
      for eventCollection in fileBlock.getEventCollectionList():
         print "  - eventcollection: %s nb.ofevts: %d " \
        % (eventCollection.getCollectionName(), eventCollection.getNumberOfEvents())
         nevts=nevts+eventCollection.getNumberOfEvents()

    return fileBlockList

# ##############
  def getDatasetFileBlocks(self,dbspath):
    """
     query DBS to get fileblocks 
    """
    fileBlockList = self.api.getDatasetFileBlocks(dbspath)

    return fileBlockList

##############################################################################
# Unit testing.
                                                                                
if __name__ == "__main__":
  try:

        dbsinfo = DBS("Dev/fanfani")

        datasetinfo={}
        ## create primary dataset
        datasetinfo['PrimaryDataset']="Test_PrimDataset1"

        ## define processing 
        dbsinfo.insertPrimaryDataset(datasetinfo['PrimaryDataset'])
        ## create processed dataset ( + empty fileblock associated to it)
        datasetinfo['PSetHash']="Test_psetdummy"
        datasetinfo['ApplicationName']="Test_simulation"
        datasetinfo['ApplicationVersion']="Test_CMSSW_X_Y_Z"
        datasetinfo['ProcessedDataset']="Test_ProcDataset1"
        datasetinfo['PrimaryDataset']="Test_PrimDataset1"
        applicationfamily='Test_simulation' 
        datatier='Test_datatier'
        dbsinfo.insertProcessedDataset(datasetinfo, datatier, applicationfamily)

        ## add event collection ....

  except DbsException, ex:
      print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
  #except :
  #  print "Caught exception "
  print "Done"


