#!/usr/bin/env python
"""
 Send MergeRegistered events 

"""
import sys,os,getopt,time

usage = "\n Usage: python PhEDExInjectMergeRegistered.py <options> \n Options: \n --datasetPath=</primarydataset/datatier/processeddataset> \n --block=</primarydataset/datatier/processeddataset#blockhash> \n --help \t\t\t\t print this help \n"
valid = [ 'datasetPath=' , 'block=' , 'help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

datasetpath = None
block = None
for opt, arg in opts:
    if opt == "--datasetPath":
        datasetpath = arg
    if opt == "--block":
        block = arg
    if opt == "--help":
        print usage
        sys.exit(1)
if datasetpath == None and block == None:
    print "\n Neither --datasetPath nor --block option provided."
    print usage
    sys.exit(1)


from MessageService.MessageService import MessageService 
ms = MessageService()
ms.registerAs("Test")
if datasetpath != None:
    print "publising Event: PhEDExInjectDataset Payload: %s"%datasetpath
    ms.publish("PhEDExInjectDataset",datasetpath)
elif block != None:
    print "publising Event: PhEDExInjectBlock Payload: %s"%block
    ms.publish("PhEDExInjectBlock",block)
ms.commit()
