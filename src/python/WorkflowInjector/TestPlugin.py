#!/usr/bin/env python
"""
Util to test the individual plugins for development purposes

"""

import os
import sys
import getopt
import logging
import hotshot, hotshot.stats
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

class FakeMessageService:
    def publish(self, event, payload):
        msg = "Call to publish: %s %s from plugin" % (event, payload)
        logging.info(msg)
    def commit(self):
        msg = "Commit called from plugin"
        logging.info(msg)

statsFile = "TestPlugin_%s.prof" % argsDict['Plugin']
prof = hotshot.Profile(statsFile)
prof.start()
pluginInstance = retrievePlugin(argsDict['Plugin'])
pluginInstance.msRef = FakeMessageService()
pluginInstance.args.update(argsDict)
pluginInstance(argsDict['Payload'])
prof.stop()
stats = hotshot.stats.load(statsFile)
stats.strip_dirs()
stats.sort_stats('time', 'calls')
stats.print_stats(10)
