
#!/usr/bin/env python

#
import dbsCgiApi
from dbsException import DbsException
#
import dlsClient
from dlsDataObjects import *
#
import os,sys,getopt

#   //
#  // Get DBS instance to use
# //
usage="\n Usage: python InspectDBSDLS.py <options> \n Options: \n --datasetPath=/primarydataset/datatier/procdataset \t\t dataset path \n --DBSAddress=<MCLocal/Writer> \t\t DBS database instance \n --DLSAddress=<lfc-cms-test.cern.ch/grid/cms/DLS/MCLocal_Test>\t\t DLS instance \n --DLSType=<DLS_TYPE_LFC> \t\t DLS type \n --help \t\t\t\t print this help \n"
valid = ['DBSAddress=','DLSAddress=','DLSType=','datasetPath=','help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

dbinstance = None
dlsendpoint = None
dlstype = None
dataset = None

for opt, arg in opts:
    if opt == "--DBSAddress":
        dbinstance = arg
    if opt == "--DLSAddress":
        dlsendpoint = arg
    if opt == "--DLSType":
        dlstype = arg
    if opt == "--datasetPath":
        dataset = arg
    if opt == "--help":
        print usage
        sys.exit(1)

if dataset == None:
    print "--datasetPath option not provided. For example : --datasetPath /primarydataset/datatier/processeddataset"
    print usage
    sys.exit(1)
if dbinstance == None:
    print "--DBSAddress option not provided. For example : --DBSAddress MCLocal/Writer"
    print usage
    sys.exit(1)
if dlstype == None:
   print "--DLSType option not provided. For example : --DLSType DLS_TYPE_LFC "
   print usage
   sys.exit(1)
if dlsendpoint == None:
    print "--DLSAddress option not provided. For example : --DLSAddress lfc-cms-test.cern.ch/grid/cms/DLS/MCLocal_Test"
    print usage
    sys.exit(1)


print ">>>>> DBS instance : %s"%dbinstance
print ">>>>> DLS instance : %s"%dlsendpoint

#  //
# // Get API to DBS
#//
url = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/prodquery"
## database instance 
args = {'instance' : dbinstance}
dbsapi = dbsCgiApi.DbsCgiApi(url, args)

#  //
# // Get API to DLS
#//
try:
  dlsapi = dlsClient.getDlsApi(dls_type=dlstype,dls_endpoint=dlsendpoint)
except dlsApi.DlsApiError, inst:
  msg = "Error when binding the DLS interface: " + str(inst)
  print msg
  sys.exit(1)

#  //
# // Get list of blocks for the dataset and their location
#//
try:
  fileBlockList = dbsapi.getDatasetFileBlocks(dataset)
except DbsException, ex:
  print "DbsException for DBS API getDatasetFileBlocks(%s): %s %s" %(dataset,ex.getClassName(), ex.getErrorMessage())
  sys.exit(1)

for fileBlock in fileBlockList:
        entryList=[]
        try:
         entryList=dlsapi.getLocations(fileBlock.get('blockName'))
        except dlsApi.DlsApiError, inst:
          msg = "Error in the DLS query: %s." % str(inst)
          print msg
          if "DLS Server don't respond" in msg:
            print msg
            sys.exit(1)
            raise RuntimeError, msg
        SEList=[]
        for entry in entryList:
         for loc in entry.locations:
          SEList.append(str(loc.host))
         print "-----------------------------------------------"
         print "File block name: ", fileBlock.get('blockName')
         print "Number of files: ", fileBlock.get('numberOfFiles')
         print "Number of Bytes: ", fileBlock.get('numberOfBytes')
         print "File block %s is located at: %s"%(fileBlock.get('blockName'),SEList)                                                                                                   
