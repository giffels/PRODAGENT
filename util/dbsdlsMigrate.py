#!/usr/bin/env python
#

import sys,string,getopt
import xml.sax.handler
import xml.sax
## DBS API
from dbsCgiApi import DbsCgiApi, DbsCgiDatabaseError
from dbsException import DbsException
from dbsApi import DbsApi, DbsApiException, InvalidDataTier
from dbsFileBlock import DbsFileBlock

## DLS API
import warnings
warnings.filterwarnings("ignore","Python C API version mismatch for module _lfc",RuntimeWarning)
import dlsClient
from dlsDataObjects import *

usage="\n Purpose of the script is Migrate a dataset to global DBS/DLS:\n " + \
"   - fetch from input DBS the contents (closed blocks) of the dataset, save it in a xml file and then write the contents of that xml to output DBS (modifying the family name if provided)\n " + \
"   - get the fileblocks in the output DBS for a given dataset \n " + \
"   - migrate those fileblock locations from an input DLS to a destination DLS \n " + \
" \n Usage: python dbsMigrateBlocks.py <options> " + \
" \n Options:  \n " + \
" --datasetPath=/primarydataset/datatier/procdataset \t dataset path \n " + \
" --DBSURL=<URL> \t\t\t\t\t DBS URL \n " + \
" --InputDBSAddress=MCLocal_X/Writer\t input DBS instance \n " + \
" --OutputDBSAddress=MCGlobal/Writer\t output DBS instance \n " + \
" --family=<application family> \t new family to use in the output DBS \n " + \
" --InputDLSAddress=<lfc-cms-test.cern.ch/grid/cms/DLS/MCLocal_Test>\t input DLS instance \n " + \
" --InputDLSType=<DLS_TYPE_LFC> \t\t\t\t DLS type \n " + \
" --OutputDLSAddress=<lfc-cms-test.cern.ch/grid/cms/DLS/LFC>\t output DLS instance \n " + \
" --OutputDLSType=<DLS_TYPE_LFC> \t\t\t\t DLS type \n " + \
" --blockcheck \t\t\t\t upload in DLS only closed block in output DBS \n " + \
" --help \t\t\t\t\t print this help \n\n " + \
" For example: \n  python dbsdlsMigrate.py --InputDBSAddress=MCLocal_4/Writer --OutputDBSAddress=Dev/fanfani --family=Skimming --InputDLSAddress=prod-lfc-cms-central.cern.ch/grid/cms/DLS/MCLocal_4 --InputDLSType DLS_TYPE_LFC --OutputDLSAddress=prod-lfc-cms-central.cern.ch/grid/cms/DLS/MCLocal_Test --datasetPath=/CSA06-103-os-EWKSoup0-0/RECOSIM/CMSSW_1_0_4-hg_HiggsWW_WWFilter-1161045561 \n"

valid = ['DBSURL=','InputDBSAddress=','OutputDBSAddress=','InputDLSAddress=','InputDLSType=','OutputDLSAddress=','OutputDLSType=','datasetPath=','family=','blockcheck','help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)
                                                                                                        
DEFAULT_URL ="http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
inputdbinstance = None
outputdbinstance = None
datasetPath = None
newfamily = None
StatusCheck = False
inputdlsendpoint = None
outputdlsendpoint = None
inputdlstype = None
outputdlstype = None

for opt, arg in opts:
    if opt == "--DBSURL":
        DEFAULT_URL = arg
    if opt == "--InputDBSAddress":
        inputdbinstance = arg
    if opt == "--OutputDBSAddress":
        outputdbinstance = arg
    if opt == "--datasetPath":
        datasetPath = arg
    if opt == "--family":
        newfamily = arg
    if opt == "--blockcheck":
        StatusCheck = True
    if opt == "--InputDLSAddress":
        inputdlsendpoint = arg
    if opt == "--InputDLSType":
        inputdlstype = arg
    if opt == "--OutputDLSAddress":
        outputdlsendpoint = arg
    if opt == "--OutputDLSType":
        outputdlstype = arg
    if opt == "--help":
        print usage
        sys.exit(1)

if datasetPath == None:
    print "--datasetPath option not provided. For example : --datasetPath /primarydataset/datatier/processeddataset"
    print usage
    sys.exit(1)
if inputdbinstance == None:
    print "--InputDBSAddress option not provided. For example : --InputDBSAddress MCGlobal/Writer"
    print usage
    sys.exit(1)
if outputdbinstance == None:
    print "--OutputDBSAddress option not provided. For example : --OutputDBSAddress Dev/fanfani "
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
if outputdlsendpoint == None:
    print "--OutputDLSAddress option not provided. For example : --OutputDLSAddress lfc-cms-test.cern.ch/grid/cms/DLS/LFC"
    print usage
    sys.exit(1)


# ##############################
class Handler(xml.sax.handler.ContentHandler):
  """
   XML handler
  """ 
  def __init__(self):
    self.family = []
    self._Handlers = {
            "processing" : self._Handleprocessing,
            }

  def startElement(self, name, attrs):
    if name != 'processing' : return
    handler = self._Handlers.get(name, None)
    if handler == None:
            return
    handler(attrs)
    return
                                                                                
  def _Handleprocessing(self, attrs):
    self.family.append(attrs.get('family'))
    #print "fam %s"%attrs['family']
    return

# ##############################
def uniquelist(old):
    """
    remove duplicates from a list
    """
    nd={}
    for e in old:
        nd[e]=0
    return nd.keys()

# ##############################
def replacefamily(filename,newfamily):
    """
    parse XML to extract the family and replace it
    """
    parser = xml.sax.make_parser(  )
    handler = Handler(  )
    parser.setContentHandler(handler)
    parser.parse(filename)
    familyList=uniquelist(handler.family)

    f = open(filename,"r")
    xmlcontent = f.read()
    for family in  familyList:
       xmlcontent=string.replace(xmlcontent,'family=\''+family+'\'','family=\''+newfamily+'\'')
    f.close()

    return xmlcontent  

# ##############################
def UploadDBSBlock(i,blocks):
    """
     fetch info from InputDBS and uplaod to OutputDBS , replacing family if needed 
    """
## Fetch
    blockName =  blocks[str(i)]['blockName'].replace('/', '_')
    print "\n o Fetching information for Block %s " %  blocks[str(i)]['blockName']
    xmlinput = api.getDatasetInfo(datasetPath,  blocks[str(i)]['blockName'])
                                                                                                        
    f = open(name +  blockName + ".xml", "w");
    f.write(xmlinput)
    f.close()

    print "o Dataset information fetched from " + inargs['instance'] + " in XML format is saved in " + name +  blockName +  ".xml"
    ## replace family 
    if newfamily != None:
       print "o Replacing family with %s"%newfamily
       xmlinput = replacefamily(name +  blockName + ".xml",newfamily)
       #print xmlinput

## Upload
    try:
      flog =  open(name + blockName + ".log", "w");
      flog.write(api_out.insertDatasetInfo(xmlinput))
      flog.close()
      print "***** DBSupload:  \n The transfer log for " + outargs['instance'] + " in XML format is saved in " + name + blockName + ".log"
      print "\n*****"
    except:
       print "***** DBSupload FAILED"
       return

## close the block in OutputDBS
    dbsblock = DbsFileBlock (blockName = blocks[str(i)]['blockName'])
    api_out.closeFileBlock(dbsblock)
    print "o Closed block %s "%blocks[str(i)]['blockName']

# ##############################
def UploadtoDBS(datasetPath):
  """
  Fetch all closed blocks and upload them to DBS
  """
  try:
        blocks = api.listBlocks(datasetPath)
        for i in blocks.keys():
          if blocks[str(i)]['status']=="closed": # consider only closed blocks
              UploadDBSBlock(i,blocks)

          #if skipStatusCheck:
          #    UploadDBSBlock(i)
          #else:
          #  if blocks[str(i)]['status']=="closed": # consider only closed blocks
          #    UploadDBSBlock(i)
                                                                                                        
  except DbsCgiDatabaseError,e:
    print e
  except InvalidDataTier, ex:
    print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
  except DbsApiException, ex:
    print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
  except DbsException, ex:
    print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

# #############################
def UploadDLSBlock(fileblock):
  """
   migrate fileblock from Input DLS to Output DLS
  """
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
    print "***** DLSupload: \n o fileblock: %s in DLS %s "%(entry.fileBlock.name,inputdlsendpoint)
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

  print "\n*****"
  return

# ###############################
def UploadtoDLS(datasetPath):
 """
 Fetch all blocks inserted in the OutputDBS and migrate the corresponding DLS entries  
 """
 try:
  fileBlockList = api_out.getDatasetFileBlocks(datasetPath) # from Output DBS
 except DbsException, ex:
  print "DbsException for DBS API getDatasetFileBlocks(%s): %s %s" %(dataset,ex.getClassName(), ex.getErrorMessage())
  sys.exit(1)
                                                                                                        
 for fileBlock in fileBlockList:
  #print fileBlock.get('blockName')
  #print fileBlock.get('blockStatus')
  if StatusCheck:
    if fileBlock.get('blockStatus')=="closed":
     UploadDLSBlock(fileBlock.get('blockName'))
  else: # by defaul upload in DLS all the fileblock that are in Output DBS
     UploadDLSBlock(fileBlock.get('blockName'))


#//
#// DBS API : Input DBS and  Ouput DBS
#//

inargs = {'instance' : inputdbinstance}
api = DbsCgiApi(DEFAULT_URL, inargs)
print ">>>> From DBS :  %s "%inputdbinstance 
outargs = {'instance' : outputdbinstance }
api_out = DbsCgiApi(DEFAULT_URL, outargs)
print ">>>> to DBS :  %s "%outputdbinstance

name = inargs['instance'].replace('/','_') + "_" + outargs['instance'].replace('/', '_') + datasetPath.replace('/', '_')


#  //
# //  DLS API: Input and Output DLS 
#//
print ">>>> From DLS : %s (type: %s) "%(inputdlsendpoint,inputdlstype)
try:
     inDLSapi = dlsClient.getDlsApi(dls_type=inputdlstype,dls_endpoint=inputdlsendpoint)
except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()
print ">>>> to DLS : %s (type: %s)"%(outputdlsendpoint,outputdlstype)
print ""
try:
     outDLSapi = dlsClient.getDlsApi(dls_type=outputdlstype,dls_endpoint=outputdlsendpoint)
except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()

# //
# // Upload to DBS
#//
UploadtoDBS(datasetPath)

# //
# // Upload to DLS
# //
UploadtoDLS(datasetPath)



