#!/usr/bin/env python
"""
_UpdateGlobalDatabase_

Daemon that updates global ProdAgent database. It is expected to be
executed as:

  python UpdateGlobalDatabase.py &

The configuration file is MonitoringConfig.xml and it must exist in the
same directory.

Errors and other information are logged in the file UpdateGlobalDatabase.log,
created in the same directory.

Please look for documentation in the ProdAgent monitoring twiki page,
section UpdateGlobalDatabase.

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import sys
import os
import time
import popen2
import signal
from xml.dom import minidom
from shutil import move
import logging
from logging.handlers import RotatingFileHandler

##########################################################################
# execute a command
##########################################################################

def executeCommand(command, timeOut = 60):
    """
    _executeCommand_

     execute a command, waiting at most timeOut seconds for successful
     completation.

    Arguments:

      command -- the command
      timeOut -- the timeout in seconds

    Return:

      the exit code or -1 if did not finish on time.

    """

    startTime = time.time()

    # run command
    job = popen2.Popen4(command)
    output = job.fromchild

    # get exit code (if ready)
    exitCode = job.poll()

    # wait for it to finish
    while exitCode == -1:

        # check timeout
        if (time.time() - startTime) > timeOut:
            logging.critical("Timeout exceded for command")

            # exceeded, kill the process
            try:
                os.kill(job.pid, signal.SIGKILL)

            # oops, cannot kill it
            except OSError:
                logging.critical("Cannot kill process")
            # abandon execution
            return -1

        # wait a second
        time.sleep(1)

        # get exit status
        exitCode = job.poll()

    # log error information if possible
    if exitCode != 0:

        # get all lines from child
        try:
            line = output.readline()
            while line:
                logging.error(str(line))
                line = output.readline()
            output.close()

        # does not work, ignore
        except IOError:
            pass
 
    # return exit code
    return exitCode

##############################################################################
# process configuration
##############################################################################

def getConfiguration():
    """
    _getConfiguration_

    read configuration file

    Arguments:

      none

    Return:

      a dictionary with all parameters

    """

    result = {}

    # build xml document
    try:
        aFile = open('MonitoringConfig.xml', 'r')
    except IOError, msg:
        logging.error("Cannot read configuration file: " + str(msg))
        sys.exit(1)

    try:
        doc = minidom.parse(aFile)
    except Exception, msg:
        logging.error("Cannot process configuration file: " + str(msg))
        sys.exit(1)

    # get all parameters
    parameterList = doc.getElementsByTagName('Parameter')

    # process each one
    result['Parameter'] = {}
    for parameter in parameterList:
        result['Parameter'][parameter.attributes['Name'].value] = \
                                 parameter.attributes['Value'].value

    # get all instances
    prodAgentList = doc.getElementsByTagName('ProdAgentInstance')

    instanceList = []
    for prodAgentInstance in prodAgentList:
        prodAgentName = prodAgentInstance.attributes['Name'].value
        instanceList.append(prodAgentName)

        # get import command
        importCommandNode = \
                prodAgentInstance.getElementsByTagName('ImportCommand')
        importCommand = importCommandNode[0].firstChild.data
        importCommand = importCommand.replace('\t','')
        importCommand = importCommand.strip('\t\n')
        result[prodAgentName] = {'importCommand' : importCommand }

    # add list of ProdAgent instances
    result['instances'] = instanceList

    # sanity checks
    try:
        interval = int(result["Parameter"]["PollInterval"])
        portNr = str(result["Parameter"]["PortNr"])
        socketFileLocation = result["Parameter"]["SocketFileLocation"]
        user = result["Parameter"]["User"]
        password = result["Parameter"]["Password"]
        host = result["Parameter"]["Host"]
    except KeyError,msg:
        logging.error("Parameter not defined in configuration: ", msg)
        sys.exit(1)

    # create mysql command
    if portNr in [None, ""]:
        access = " --socket=" + socketFileLocation
    else:
        access = " --port=" + portNr

    result['mysqlCommand'] = "mysql --user=" + user + \
                        " --password=" + password + \
                        " --host=" + host + \
			access 

    return result

##############################################################################
# get dump file
##############################################################################

def getDumpFile(prodAgent, configuration):
    """
    _getDumpFile_

    get dump file with user specified commands, and add extra
    commands necessary to create the database.

    Arguments:

      prodAgent -- the prodAgent instance
      configuration -- configuration information

    Return:

      none

    It can generate IOError on file errors

    """

    # execute command
    exitCode = executeCommand(configuration[prodAgent]['importCommand'])

    # verify exit code
    if exitCode != 0:
        raise IOError

    # add extra code for database creation and fast insertion
    extraCode = "DROP DATABASE IF EXISTS " + prodAgent + ";\n" + \
                "CREATE DATABASE " + prodAgent + ";\n" + \
                "USE " + prodAgent + ";\n" + \
                "SET FOREIGN_KEY_CHECKS=0;\n\n"

    # add extra code at the beginning of the file
    fileName = prodAgent + ".sql"
    inputFile = file(fileName, "r")
    outputFile = file(fileName + ".temp", "w")
    outputFile.write(extraCode)
    for line in inputFile:
        outputFile.write(line)
    inputFile.close()
    outputFile.close()
    move(fileName + ".temp", fileName)
    
##############################################################################
# store information into database
##############################################################################

def storeInformation(prodAgent, configuration):
    """
    _storeInformation_

    store information on local database

    Arguments:

      prodAgent -- the prodAgent instance
      configuration -- configuration information

    Return:

      none

    It can generate IOError on file errors

    """

    # create insertion command
    command = configuration["mysqlCommand"] + " < " + prodAgent + ".sql"

    # run it
    exitCode = executeCommand(command)

    # verify exit code
    if exitCode != 0:
        raise IOError

##############################################################################
# initialization
##############################################################################

def init():
    """
    _init_

    initialize update task

    Arguments:

      none

    Return:

      none

    """
    # create log handler
    logHandler = RotatingFileHandler("UpdateGlobalDatabase.log", \
                                     "a", 1000000, 3)

     # define log format
    logFormatter = logging.Formatter("%(asctime)s:%(message)s")
    logHandler.setFormatter(logFormatter)
    logging.getLogger().addHandler(logHandler)
    logging.getLogger().setLevel(logging.INFO)

##############################################################################
# Main body
##############################################################################

# initialize process
init()

# get configuration
configuration = getConfiguration()

# get poll interval
pollInterval = int(configuration["Parameter"]["PollInterval"])

# for ever
while True:

    # process each ProdAgent database
    for prodAgent in configuration['instances']:

        logging.info("Processing database for PA " + prodAgent)

        # get file
        try:
            getDumpFile(prodAgent, configuration)
        except IOError:
            logging.error("Cannot get database dump for PA instance: " + \
                  prodAgent + \
                  ", error message should have been displayed before")
            continue

        # insert information into database
        try:
            storeInformation(prodAgent, configuration)
        except IOError:
            logging.error("Cannot insert database dump for PA instance: " + \
                  prodAgent)
            continue

    # wait for next update cycle
    time.sleep(pollInterval)


