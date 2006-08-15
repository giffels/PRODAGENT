
#!/usr/bin/env python

import dbsCgiApi
import os,sys,getopt

#   //
#  // Get DBS instance to use
# //
usage="\n Usage: python InspectDBS.py <options> \n Options: \n --instance=<MCLocal/Writer> \t\t DBS database instance \n --url=<URL> \t\t DBS URL \n --full \t\t\t\t enable printing of file list \n --datasetPath=/primarydataset/datatier/procdataset \t\t optional dataset path to refine the search\n --help \t\t\t\t print this help \n"
valid = ['instance=','url=','full','datasetPath=','help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

# default URL
url = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
# default instance to "MCLocal/Writer"
dbinstance = "MCLocal/Writer"
full=False         
datasetPath = None                                                                                                 
for opt, arg in opts:
    if opt == "--instance":
        dbinstance = arg
    if opt == "--url":
        url = arg
    if opt == "--full":
        full = True
    if opt == "--datasetPath":
        datasetPath = arg
    if opt == "--help":
        print usage
        sys.exit(1)

print ">>>>> DBS URL : %s DBS : %s"%(url,dbinstance) 


#  //
# // Get API to DBS containing datasets
#//
## database instance 
args = {'instance' : dbinstance}

api = dbsCgiApi.DbsCgiApi(url, args)

#  //
# // Get list of datasets
#//
try:
   if datasetPath:
     datasets = api.listProcessedDatasets(datasetPath)
   else:
     datasets = api.listProcessedDatasets("/*/*/*")
except dbsCgiApi.DbsCgiToolError , ex:
  print "%s: %s " %(ex.getClassName(),ex.getErrorMessage())
  print "exiting..."
  sys.exit(1)

#  //
# // Look at contents of dataset
#//
for dataset in datasets:
     print "-----------------------------------"
     print "Dataset: %s"%dataset.get('datasetPathName')
     for block in api.getDatasetContents(dataset.get('datasetPathName')):
         print "File block name/id: %s/%d"%(block.get('blockName'),block.get('objectId'))
         print "Number of event collections: %s"%len(block.get('eventCollectionList'))
#  //
# // Look at contents in term single files
#//
     if full:
       for block in api.getDatasetFileBlocks (dataset.get('datasetPathName')):
         print "--------- info about files --------"
         print "File block name: ", block.get('blockName')
         print "Number of files: ", block.get('numberOfFiles')
         print "Number of Bytes: ", block.get('numberOfBytes')
         for file in block.get('fileList'):
             print "  LFN: ", file.get('logicalFileName')

