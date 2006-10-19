#!/usr/bin/env python
"""
_DBSInjectReport_
                                                                                
Command line tool to inject a "JobSuccess" event into DBS reading a FrameworkJobReport file.

"""
import dbsCgiApi
from dbsException import DbsException
from dbsFileBlock import DbsFileBlock

import string,sys,os,getopt,time


usage="\n Usage: python closeDBSFileBlock.py <options> \n Options: \n --DBSAddress=<MCLocal/Writer> \t\t DBS database instance \n --DBSURL=<URL> \t\t DBS URL \n --block=<fileblock> \t\t fileblock \n --blockFileList=<filewithblocklist> \t\t File with the list of fileblocks \n"
valid = ['DBSAddress=','DBSURL=','block=','blockFileList=','valid']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

url = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
dbinstance = None
block = None
blockFileList = None

for opt, arg in opts:
    if opt == "--block":
        block = arg
    if opt == "--blockFileList":
        blockFileList = arg
    if opt == "--DBSAddress":
        dbinstance = arg
    if opt == "--DBSURL":
        url = arg

if dbinstance == None:
    print "--DBSAddress option not provided. For example : --DBSAddress MCLocal/Writer"
    print usage
    sys.exit(1)

if (block == None) and (blockFileList == None) :
    print "\n either --block or --blockFileList option has to be provided"
    print usage
    sys.exit(1)
if (block != None) and (blockFileList != None) :
    print "\n options --block or --blockFileList are mutually exclusive"
    print usage
    sys.exit(1)

print ">>>>> DBS URL : %s DBS Address : %s"%(url,dbinstance)
#  //
# // Get API to DBS
#//
## database instance
args = {'instance' : dbinstance}
dbsapi = dbsCgiApi.DbsCgiApi(url, args)

#  //
# // Close FileBlock
#//
def closeDBSFileBlock(ablock):   
  print "Closing block %s"%ablock
  dbsblock = DbsFileBlock (blockName = ablock)
  dbsapi.closeFileBlock(dbsblock)

if (block != None):
  closeDBSFileBlock(block)

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



