
#!/usr/bin/env python

import dbsCgiApi
import os,sys,getopt

#   //
#  // Get DBS instance to use
# //
usage="\n Usage: python InspectDBS.py <options> \n Options: \n --instance=<MCLocal/Writer> \t\t DBS database instance \n --full \t\t\t\t enable printing of file list \n --help \t\t\t\t print this help \n"
valid = ['instance=','full','help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

# default to "MCLocal/Writer"
dbinstance = "MCLocal/Writer"
full=False                                                                                                          
for opt, arg in opts:
    if opt == "--instance":
        dbinstance = arg
    if opt == "--full":
        full = True
    if opt == "--help":
        print usage
        sys.exit(1)

print ">>>>> DBS instance : %s"%dbinstance 

#if dbinstance == None:
#    print "--dbinstance option not provided. For example : --dbinstance MCLocal/Writer"
#    print usage
#    sys.exit(1)


#  //
# // Get API to DBS containing datasets
#//
url = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/prodquery"
## database instance 
args = {'instance' : dbinstance}

api = dbsCgiApi.DbsCgiApi(url, args)

#  //
# // Get list of datasets
#//
try:
  datasets = api.listDatasets("/*/*/*")
except dbsCgiApi.DbsCgiToolError , ex:
  print "%s: %s " %(ex.getClassName(),ex.getErrorMessage())
  print "exiting..."
  sys.exit(1)

#  //
# // Look at contents of dataset
#//
for dataset in datasets:
     print "-----------------------------------"
     print "Dataset: %s"%dataset.getDatasetPath()
     for block in api.getDatasetContents(dataset.getDatasetPath()):
         print "File block name/id: %s/%d"%(block.getBlockName(),block.getObjectId())
         print "Number of event collections: %s"%len(block.getEventCollectionList())
#  //
# // Look at contents in term single files
#//
     if full:
       for block in api.getDatasetFileBlocks (dataset.getDatasetPath()):
         print "--------- info about files --------"
         print "File block name: ", block.getBlockName()
         print "Number of files: ", block.getNumberOfFiles()
         print "Number of Bytes: ", block.getNumberOfBytes()
         for file in block.getFileList():
             print "  LFN: ", file.getLogicalFileName()

