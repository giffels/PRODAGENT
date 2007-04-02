#!/usr/bin/env python
"""
_DBSInjectReport_
                                                                                
Command line tool to inject a "JobSuccess" event into DBS reading a FrameworkJobReport file.

"""
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsFileBlock import DbsFileBlock
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx

import string,sys,os,getopt,time


usage="\n Usage: python closeDBSFileBlock.py <options> \n Options: \n --DBSURL=<URL> \t\t\t DBS URL \n --block=<fileblock> \t\t\t close this fileblock \n --blockFileList=<filewithblocklist> \t close all fileblocks listed in this file \n --datasetPath=/<primarydataset>/<procdataset>/<datatier>  close all fileblocks for this dataset"
valid = ['DBSURL=','block=','blockFileList=','datasetPath=','valid']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

url = None
block = None
blockFileList = None
datasetPath = None

for opt, arg in opts:
    if opt == "--block":
        block = arg
    if opt == "--blockFileList":
        blockFileList = arg
    if opt == "--datasetPath":
        datasetPath = arg
    if opt == "--DBSURL":
        url = arg

if url == None:
    print "--DBSURL option not provided. For example : \n --DBSURL=http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet"
    print usage
    sys.exit(1)

if (block == None) and (blockFileList == None) and (datasetPath == None):
    print "\n either --block or --blockFileList or --datasetPath option has to be provided"
    print usage
    sys.exit(1)
if (block != None) and (blockFileList != None) and (datasetPath != None):
    print "\n options --block or --blockFileList or --datasetPath are mutually exclusive"
    print usage
    sys.exit(1)

print ">>>>> DBS URL : %s "%(url,)

import logging
logging.disable(logging.INFO)
#  //
# // Get API to DBS
#//
args = {'url' : url }
dbsapi = DbsApi(args)
dbsreader = DBSReader(url)

#  //
# // Close FileBlock method
#//
def closeDBSFileBlock(ablock):   
  print "Closing block %s"%ablock
  dbsblock = DbsFileBlock( Name = ablock)
  dbsapi.closeBlock(dbsblock)

### --block option: close single block
if (block != None):
  closeDBSFileBlock(block)

## --blockFileList option: close list of blocks from a file
if (blockFileList != None) :
 expand_blockFileList=os.path.expandvars(os.path.expanduser(blockFileList))
 if not os.path.exists(expand_blockFileList):
    print "File not found: %s" % expand_blockFileList
    sys.exit(1)

 blocklist_file = open(expand_blockFileList,'r')
 for line in blocklist_file.readlines():
   block=line.strip()
   closeDBSFileBlock(block)
 blocklist_file.close()

## --datasetPath: close all blocks for a dataset
if (datasetPath != None):

  #  //
  # // Get list of datasets
  #//
     primds=datasetPath.split('/')[1]
     procds=datasetPath.split('/')[2]
     tier=datasetPath.split('/')[3]
     #print " matchProcessedDatasets(%s,%s,%s)"%(primds,tier,procds)
     datasets=dbsreader.matchProcessedDatasets(primds,tier,procds)

     for dataset in datasets:
#  //
# // Get list of blocks for the dataset 
#//
      matchPath=False
      for datasetpath in dataset.get('PathList'):
       if (datasetpath == datasetPath) :
         matchPath=True
         print "===== dataset %s"%datasetpath
         blocks=dbsreader.getFileBlocksInfo(datasetpath)
         fileBlockList = dbsreader.getFileBlocksInfo(datasetpath)
         for fileBlock in fileBlockList:
           block=fileBlock.get('Name')
           closeDBSFileBlock(block)
       if not matchPath:
           print "WARNING: the provided datasetPath=%s should match exactly one of the paths %s"%(datasetPath,dataset.get('PathList'))

