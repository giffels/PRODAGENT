#!/usr/bin/env python

"""
_DBS_

Class to extract/insert info from DBS
based on CGI DBS API
 
"""
import os,string,exceptions
import dbsApi
from dbsCgiApi import DbsCgiApi, DbsCgiObjectExists, DbsCgiNoObject
from dbsException import DbsException
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock
from dbsEventCollection import DbsEventCollection
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsProcessedDataset import DbsProcessedDataset
from dbsProcessing import DbsProcessing
from dbsApi import DbsApi, DbsApiException, InvalidDataTier

import logging
# ##############
class DBS:
  """
  _DBS_

  interface to extract/insert info from DBS
  """
# ##############
  def __init__(self, dbsurl , dbinstance):
    """
     Construct api object.
    """
    ## cgi service API
    args = {}
    args['instance']=dbinstance

    self.api = DbsCgiApi(dbsurl , args)
    logging.debug(" DBS URL: %s DBSAddress: %s "%(dbsurl , dbinstance))
    
    ## set log level : log not supported
    #self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_ALL_)
    #self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_INFO_)
    #self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_QUIET_)

# ##############
  def insertPrimaryDataset(self, PrimDatasetName): 
    """
     create a primary dataset with DBS API createPrimaryDataset
    """

    # Define Primary dataset object:
    primdataset = DbsPrimaryDataset(datasetName = PrimDatasetName) 
    # Create Primary dataset
    logging.debug(" createPrimaryDataset %s"%PrimDatasetName)
    print " createPrimaryDataset %s"%PrimDatasetName
    #logging.debug(" createPrimaryDataset %s"%PrimDatasetName)
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
         logging.debug("createProcessedDataset %s for datatier %s"%(procdatasetName,tier))
         print " createProcessedDataset %s for datatier %s"%(procdatasetName,tier)
         self.api.createProcessedDataset(dataset)
       except DbsCgiObjectExists, ex:
         #print "Object existed already, passing"
         pass

       # Create File Block (in the datatier loop for now)

#AF - skip creation of empty fileblock
#AF       datasetPath="/%s/%s/%s"%(dataset.get('primaryDataset')['datasetName'],dataset.get('dataTier'),dataset.get('datasetName'))
#AF       logging.debug(" datasetPath %s"%datasetPath)
#AF       self.insertFileBlock(datasetPath, processing)

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
      logging.debug(" createProcessing ")
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
       logging.debug(" createFileBlock ")
       print " createFileBlock "
       try:
         self.api.createFileBlock(block)
       except DbsCgiObjectExists, ex:
         #print "Object existed already, passing"
         pass
    return

# ##############
  def addFileBlock(self, datasetinfo, datasetPath):
    """
      Create FileBlock with the same processing alredy stored when New Dataset was created
    """
    ##

    #primdataset = DbsPrimaryDataset(datasetName = datasetPath.split('/')[1])
    #procdatasetName=datasetPath.split('/')[3]

    PSet=datasetinfo['PSetHash']
    #PSetcont=datasetinfo['PSet']
    PSetcont=PSet # add PSet content or endcoded config later on
    ApplicationName=datasetinfo['ApplicationName']
    ApplicationVersion=datasetinfo['ApplicationVersion']
    procdatasetName=datasetinfo['ProcessedDataset']
    applicationfamily=datasetinfo['OutputModuleName']                                                                                                            
    primdataset = DbsPrimaryDataset(datasetName = datasetinfo['PrimaryDataset'])
                                                                                                                
    processing = DbsProcessing (primaryDataset = primdataset,
                              processingName = procdatasetName,
                              applicationConfig = {
                                'application' : { 'executable' : ApplicationName,
                                                  'version' : ApplicationVersion,
                                                  'family' : applicationfamily },
                                'parameterSet' : { 'hash' : PSet,
                                                   'content' : PSetcont }})


    ## add another block associated with that processing
    block = DbsFileBlock (processing = processing)
    try:
           self.api.createFileBlock(block)
    except DbsCgiObjectExists, ex:
           print "Object existed already, passing"
           pass
    return block


# ##############
  def insertFiletoBlock(self, fileinfo,fileblock):
    """
      insert files
    """

    #print "Insering file with:"
    #print "  LFN %s"%fileinfo['LFN']
    #print "  GUID %s"%fileinfo['GUID']
    logging.debug("  GUID %s"%fileinfo['GUID'])
    #print "  Size %s"%fileinfo['Size']
 
    ## checksum
    cksumalgo='cksum'
    checksum=fileinfo.checksums[cksumalgo].split(' ')[0]
    checkSum="%s:%s"%(cksumalgo,checksum)
    logging.debug("  checkSum %s"%checkSum )

    if fileinfo['GUID']:
      outfile = DbsFile (logicalFileName=fileinfo['LFN'], 
                       fileSize=long(fileinfo['Size']),
                       checkSum="%s"%checkSum,
                       guid=fileinfo['GUID'], 
                       fileType="EVD")
    else: # do not insert GUID if it's not there
       outfile = DbsFile (logicalFileName=fileinfo['LFN'],
                       fileSize=long(fileinfo['Size']),
                       checkSum="%s"%checkSum,
                       fileType="EVD")


    fList=[outfile]
    logging.debug(" insert files to block %s"%fileblock.get('blockName'))
    #print " insert files to block %s"%fileblock.get('blockName')
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
    #nameLFN=os.path.basename(fileLFN)
    #nameLFN=nameLFN.replace('.root','')
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
                                       numberOfEvents=long(events), 
                                       fileList=fList)
        parentList=[{ 'parent' : parent_ec, 'type' : parent_tier }]

    name="%s_%s"%(tier,nameLFN)
    logging.debug(" evc name : %s"%name)

    ec = DbsEventCollection (collectionName=name, 
                             numberOfEvents=long(events),
                             fileList=fList,
                             parentageList=parentList)
    evcList = [ec]   
    return evcList

# ##############
  def insertEVCtoDataset(self,datasetPath,evcList):
    """
    """
    # dataset
    logging.debug("insert EventCollections for dataset %s"%datasetPath)
    dataset= DbsProcessedDataset (datasetPathName=datasetPath)
    #print " insert EventCollections for dataset %s"%datasetPath
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
      logging.debug("File block name: %s" % (fileBlock.get('blockName')))
      print "File block name: %s" % (fileBlock.get('blockName'))
      for eventCollection in fileBlock.get('eventCollectionList'):
         logging.debug("  - eventcollection: %s nb.ofevts: %d "% (eventCollection.get('collectionName'), eventCollection.get('numberOfEvents')))
         print "  - eventcollection: %s nb.ofevts: %d " \
        % (eventCollection.get('collectionName'), eventCollection.get('numberOfEvents'))
         nevts=nevts+eventCollection.get('numberOfEvents')

    return fileBlockList

# ##############
  def getDatasetFileBlocks(self,dbspath):
    """
     query DBS to get fileblocks 
    """
    fileBlockList = self.api.getDatasetFileBlocks(dbspath)

    return fileBlockList

# ##############
  def closeBlock(self, fileblockName):
    """
     close a fileblock
    """
    dbsblock = DbsFileBlock (blockName = fileblockName)
    try:
       self.api.closeFileBlock(dbsblock)
    except DbsCgiObjectExists, ex:
       logging.debug("Failed to close FileBlock %s"%fileblockName)
       pass                                                                        

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


