#!/usr/bin/env python
"""
 Send MergeRegistered events 

"""
import sys,os,getopt,time

usage = "\n Usage: python PhEDExInjectMergeRegistered.py <options> \n Options: \n --datasetPath=</primarydataset/datatier/processeddataset> \n --help \t\t\t\t print this help \n"
valid = [ 'datasetPath=' , 'help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

dataset = None
for opt, arg in opts:
    if opt == "--datasetPath":
        dataset = arg
    if opt == "--help":
        print usage
        sys.exit(1)
if dataset == None:
    print "--datasetPath option not provided. For example : --datasetPath /primarydataset/datatier/processeddataset"
    print usage
    sys.exit(1)

from MessageService.MessageService import MessageService 
ms = MessageService()
ms.registerAs("Test")
ms.publish("MergeRegistered",dataset)
ms.commit()

