#!/usr/bin/env python
"""
Util to test the individual plugins for development purposes

"""

import os
import sys
import getopt
import logging
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

import WorkflowInjector.Plugins
from WorkflowInjector.Registry import retrievePlugin

def usage():
    usage = \
    """
    Usage: TestPlugin.py <opts>
    Test harness for plugin development testing
    Options:
    ComponentDir
    Plugin
    Payload
    """
    print usage

argsDict = {
    "ComponentDir" : os.getcwd(),
    "Plugin" : None,
    "Payload" : None,
    }

valid = ['ComponentDir=', "Plugin=", "Payload="]

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print str(ex)
    usage()
    sys.exit(1)

for opt, arg in opts:
    argsDict[opt.replace('--', '')] = arg


for key, val in argsDict.items():
    if argsDict[key] == None:
        msg = "Error: Parameter %s not set:\n" % key
        msg += "You must provide the --%s=<value> option" % key
        print msg
        sys.exit(1)


pluginInstance = retrievePlugin(argsDict['Plugin'])
pluginInstance.args.update(argsDict)
pluginInstance(argsDict['Payload'])


