#!/usr/bin/env python
"""
_DiskBot_

Automatically shutdown prodAgent components in case the disk is full

"""

import logging
import os
import subprocess

from AdminControl.Bots.BotInterface import BotInterface
from AdminControl.Registry import registerBot

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails



def sizeUsed(pathname):
    """
    _sizeUsed_

    Get the size used from the path provided by running the df command.
    Return output as a dictionary containing the total size, size used and
    size available
    Units are GB blocks

    """
    output = subprocess.Popen(["df", pathname],
                              stdout = subprocess.PIPE).communicate()[0]
    data = output.split("\n")[1]
    size = data.split()[1]
    used = data.split()[2]
    available = data.split()[3]
    return {
        "size" : (int(size)/1024.)/1024.,
        "used" : (int(used)/1024.)/1024.,
        "available" : (int(available)/1024.)/1024.,
        }


class DiskBot(BotInterface):
    """
    _DiskBot_

    Run a check on the available disk space for the node/partition we are on
    & if there is a problem, trigger a component shutdown

    """


    def __init__(self):
        BotInterface.__init__(self)
        self.skipCycles = 1


    def __call__(self):
        """
        _operator()_

        Invoke the Bot to check disk size

        """
        sizes = sizeUsed(os.getcwd())

        percentUsed = float(sizes['used'])/float(sizes['size']) * 100

        #  //
        # // > 98% full
        #//
        percentageLimit = 98
        percentageTest = percentUsed > percentageLimit
        logging.info("Disk Percentage used %s > %s percent : Test is %s " % (
            percentUsed, percentageLimit, percentageTest))

        #  //
        # // Minimum space check min GB available
        #//
        minimumSpaceLimit = 10
        spaceTest = sizes['available'] < minimumSpaceLimit
        logging.info("Minimum Disk Space: %sGB < %sGB : Test is %s" % (
            sizes['available'], minimumSpaceLimit, spaceTest))

        if percentageTest and spaceTest:
            msg = "Disk Space Critical:\n"
            msg += " - Below %s percentage Space on Device\n" % percentageLimit
            msg += " - Less than %s GB available\n" % minimumSpaceLimit
            msg += " SHUTTING DOWN PRODAGENT DAEMONS"
            logging.warning(msg)
            self.shutdown()
            logging.info("Diskbot: Shutdown procedure completed.")


    def shutdown(self) :
        """
        Shutdown PA components
        """ 
        cfgObject = loadProdAgentConfiguration()
        components = cfgObject.listComponents()
        for component in components:
            logging.info("DiskBot: Checking %s" % component)
            if component.lower().find('admincontrol') > -1 :
                logging.info("Suicide is not allowed. Skipping.")
                continue
            compCfg = cfgObject.getConfig(component)
            compDir = compCfg['ComponentDir']
            compDir = os.path.expandvars(compDir)
            daemonXml = os.path.join(compDir, "Daemon.xml")
            doShutdown = True

            if not os.path.exists(daemonXml):
                doShutdown = False

            daemon = DaemonDetails(daemonXml)
            if not daemon.isAlive():
                doShutdown = False
                logging.info("Component %s is already down." % component)

            if doShutdown :
                logging.info("Component %s is alive, killing it..." % component)
                daemon.killWithPrejudice()
                logging.info("%s has been killed." % component)


registerBot(DiskBot, DiskBot.__name__)
