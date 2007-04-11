#!/usr/bin/env python
"""
 Send MergeRegistered events 

"""
import sys,os,getopt,time
from DBSInterface.DBSComponent import getGlobalDBSDLSConfig
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError, formatEx
from DBSAPI.dbsApiException import DbsException


usage = "\n Usage: python PhEDExInjectMergeRegistered.py <options> \n Options: \n --datasetPath=</primarydataset/datatier/processeddataset> \n --help \t\t\t\t print this help \n"
valid = [ 'datasetPath=' , 'help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

datasetpath = None
for opt, arg in opts:
    if opt == "--datasetPath":
        datasetpath = arg
    if opt == "--help":
        print usage
        sys.exit(1)
if datasetpath == None:
    print "--datasetPath option not provided. For example : --datasetPath /primarydataset/processeddataset/datatier"
    print usage
    sys.exit(1)


DBSConf=getGlobalDBSDLSConfig()
dbsreader= DBSReader(DBSConf['DBSURL'])
blocks=dbsreader.listFileBlocks(datasetpath)

from MessageService.MessageService import MessageService 
ms = MessageService()

for fileblockName in blocks:
 print "publising Event: PhEDExInjectBlock Payload: %s"%fileblockName
 ms.registerAs("Test")
 ms.publish("PhEDExInjectBlock",fileblockName)
 ms.commit()
