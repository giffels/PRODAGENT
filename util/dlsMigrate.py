
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
usage="\n Purpose of the script: \n " + \
" - get the fileblocks (closed, by default) in DBS for a given dataset \n " + \
" - migrate those fileblock locations from an input DLS to a destination DLS \n " + \
" \n Usage: python dlsMigrate.py <options> " + \
" \n Options:  \n " + \
" --datasetPath=/primarydataset/datatier/procdataset \t dataset path (wildcard are accepted) \n " + \
" --DBSAddress=<MCGlobal/Writer> \t\t\t DBS database instance \n " + \
" --DBSURL=<URL> \t\t\t\t\t DBS URL \n " + \
" --InputDLSAddress=<lfc-cms-test.cern.ch/grid/cms/DLS/MCLocal_Test> input DLS instance \n " + \
" --InputDLSType=<DLS_TYPE_LFC> \t\t\t\t DLS type \n " + \
" --OutputDLSAddress=<lfc-cms-test.cern.ch/grid/cms/DLS/LFC>\t DLS output instance \n " + \
" --OutputDLSType=<DLS_TYPE_LFC> \t\t\t\t DLS type \n " + \
" --skip-blockcheck migrate all the blocks, regardless of their status (by default only closed blocks are migrated) \n " + \
" --help \t\t\t\t\t print this help \n\n " + \
" For example: \n  python dlsMigrate.py --DBSAddress=MCGlobal/Writer --InputDLSAddress=prod-lfc-cms-central.cern.ch/grid/cms/DLS/MCLocal_Test --InputDLSType DLS_TYPE_LFC --OutputDLSAddress=prod-lfc-cms-central.cern.ch/grid/cms/DLS/LFC --datasetPath=/CSA06-103-os-EWKSoup0-0/RECOSIM/CMSSW_1_0_4-hg_HiggsWW_WWFilter-1161045561 \n"
valid = ['DBSAddress=','DBSURL=','InputDLSAddress=','InputDLSType=','OutputDLSAddress=','OutputDLSType=','datasetPath=','skip-blockcheck','help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

url = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
dbinstance = None
inputdlsendpoint = None
outputdlsendpoint = None
inputdlstype = None
outputdlstype = None
datasetPath = None
skipStatusCheck = False

for opt, arg in opts:
    if opt == "--DBSAddress":
        dbinstance = arg
    if opt == "--DBSURL":
        url = arg
    if opt == "--InputDLSAddress":
        inputdlsendpoint = arg
    if opt == "--InputDLSType":
        inputdlstype = arg
    if opt == "--OutputDLSAddress":
        outputdlsendpoint = arg
    if opt == "--OutputDLSType":
        outptdlstype = arg
    if opt == "--datasetPath":
        datasetPath = arg
    if opt == "--skip-blockcheck":
        skipStatusCheck = True
    if opt == "--help":
        print usage
        sys.exit(1)

if datasetPath == None:
    print "--datasetPath option not provided. For example : --datasetPath /primarydataset/datatier/processeddataset"
    print usage
    sys.exit(1)

if dbinstance == None:
    print "--DBSAddress option not provided. For example : --DBSAddress MCGlobal/Writer"
    print usage
    sys.exit(1)
if inputdlstype == None:
   print "--InputDLSType option not provided. For example : --InputDLSType DLS_TYPE_LFC "
   print usage
   sys.exit(1)
if inputdlsendpoint == None:
    print "--InputDLSAddress option not provided. For example : --InputDLSAddress lfc-cms-test.cern.ch/grid/cms/DLS/MCLocal_Test"
    print usage
    sys.exit(1)
if outputdlstype == None:
   print "--OutputDLSType option not provided. Assumed the same as --InputDLSType."
   outputdlstype = inputdlstype
   #print usage
   #sys.exit(1)
if outputdlsendpoint == None:
    print "--OutputDLSAddress option not provided. For example : --OutputDLSAddress lfc-cms-test.cern.ch/grid/cms/DLS/LFC"
    print usage
    sys.exit(1)



print "\n >>> Upload existing block in DBS : %s "%(dbinstance)

#  //
# // Get API to DBS
#//
## database instance 
args = {'instance' : dbinstance}
dbsapi = dbsCgiApi.DbsCgiApi(url, args)

#  //
# //  Local and Global DLS API
#//
print ">>>> From DLS Server endpoint: %s (type: %s) "%(inputdlsendpoint,inputdlstype)
try:
     inDLSapi = dlsClient.getDlsApi(dls_type=inputdlstype,dls_endpoint=inputdlsendpoint)
except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()
                                                                                                                                  
print ">>>> to DLS Server endpoint: %s (type: %s)"%(outputdlsendpoint,outputdlstype)
print ""
try:
     outDLSapi = dlsClient.getDlsApi(dls_type=outputdlstype,dls_endpoint=outputdlsendpoint)
except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()



def UploadBlock(fileblock):
                                                                                                                                  
  #  //
  # // get location of the fileblock in local DLS
  #//
  entryList=[]
  locationList=[]
  try:
     entryList=inDLSapi.getLocations(fileblock)
  except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
     return
  for entry in entryList:
    print "= fileblock: %s in DLS %s "%(entry.fileBlock.name,inputdlsendpoint)
    for loc in entry.locations:
      #print " %s"%loc.host
      locationList.append(DlsLocation(loc.host))
                                                                                                                                  
  #  //
  # // add fileblock in global DLS with locations of the original local fileblock
  #//
  file_block=DlsFileBlock(fileblock)
  entry=DlsEntry(file_block,locationList)
                                                                                                                                  
                                                                                                                                  
  try:
     outDLSapi.add([entry])
  except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg
     return
  print " migrated to DLS %s and located at :"%(outputdlsendpoint,)
  for loc in locationList:
    print "%s"%loc.host
                                                                                                                                  
  return


#  //
# // Get list of datasets
#//
try:
   if datasetPath:
     datasets = dbsapi.listProcessedDatasets(datasetPath)
   else:
     datasets = dbsapi.listProcessedDatasets("/*/*/*")
except dbsCgiApi.DbsCgiToolError , ex:
  print "%s: %s " %(ex.getClassName(),ex.getErrorMessage())
  print "exiting..."
  sys.exit(1)


for dataset in datasets:
#  //
# // Get list of blocks for the dataset and their location
#//
 dataset=dataset.get('datasetPathName')
 print "===== dataset %s"%dataset
 try:
  fileBlockList = dbsapi.getDatasetFileBlocks(dataset)
 except DbsException, ex:
  print "DbsException for DBS API getDatasetFileBlocks(%s): %s %s" %(dataset,ex.getClassName(), ex.getErrorMessage())
  sys.exit(1)


 for fileBlock in fileBlockList:
  #print fileBlock.get('blockName')
  #print fileBlock.get('blockStatus')
  if skipStatusCheck:
     UploadBlock(fileBlock.get('blockName'))
  else:
   if fileBlock.get('blockStatus')=="closed": 
     UploadBlock(fileBlock.get('blockName'))

