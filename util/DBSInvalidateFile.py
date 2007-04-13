#!/usr/bin/env python
"""
_DBSInvalidateFile_
                                                                                
Command line tool to invalidate a file.

"""

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *

import string,sys,os,getopt,time

usage="\n Usage: python DBSInvalidateFile.py <options> \n Options: \n --DBSURL=<URL> \t\t DBS URL \n --lfn=<LFN> \t\t LFN \n --lfnFileList=<filewithLFNlist> \t\t File with the list of LFNs \n [ valid \t\t option to set files to valid instead of invalid]"
valid = ['DBSURL=','lfn=','lfnFileList=','valid']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

#url= "http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet"
url = None
lfn = None
lfnFileList = None
valid = False

for opt, arg in opts:
    if opt == "--lfn":
        lfn = arg
    if opt == "--lfnFileList":
        lfnFileList = arg
    if opt == "--DBSURL":
        url = arg
    if opt == "--valid":
        valid = True

if url == None:
    print "--DBSURL option not provided. For example :\n --DBSURL http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet"
    print usage
    sys.exit(1)

if (lfn == None) and (lfnFileList == None) :
    print "\n either --lfn or --lfnFileList option has to be provided"
    print usage
    sys.exit(1)
if (lfn != None) and (lfnFileList != None) :
    print "\n options --lfn or --lfnFileList are mutually exclusive"
    print usage
    sys.exit(1)

print ">>>>> DBS URL : %s"%(url)

import logging
logging.disable(logging.INFO)

#  //
# // Get API to DBS
#//
args = {'url' : url , 'level' : 'ERROR'}
dbsapi = DbsApi(args)

#  //
# // Invalidate LFNs
#//
def setLFNstatus(alfn, valid):

  if valid:
    print "Validating LFN %s"%alfn
    dbsapi.updateFileStatus(alfn,"VALID")

  else:
    print "Invalidating LFN %s"%alfn
    dbsapi.updateFileStatus(alfn,"INVALID")

if (lfn != None):
  setLFNstatus(lfn,valid)

if (lfnFileList != None) :
 expand_lfnFileList=os.path.expandvars(os.path.expanduser(lfnFileList))
 if not os.path.exists(expand_lfnFileList):
    print "File not found: %s" % expand_lfnFileList
    sys.exit(1)

 lfnlist_file = open(expand_lfnFileList,'r')
 for line in lfnlist_file.readlines():
   lfn=line.strip()
   setLFNstatus(lfn,valid) 
 lfnlist_file.close()



