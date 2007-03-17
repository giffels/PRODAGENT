#!/usr/bin/env python
"""
_Monitoring_

Component that provides monitoring services

"""

__revision__ = "$Id: MonitoringComponent.py,v 1.1 2007/03/13 12:39:39 ckavka Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os
import time
import popen2
import signal

# Message service import
from MessageService.MessageService import MessageService

# logging
import logging
import ProdAgentCore.LoggingUtils as LoggingUtils

##############################################################################
# MonitoringComponent class
##############################################################################
                                                                                
class MonitoringComponent:
    """
    _MonitoringComponent_

    Component that provides monitoring services for the production agent

    """

    ##########################################################################
    # Monitoring component initialization
    ##########################################################################

    def __init__(self, **args):
        """
        
        Arguments:
        
          args -- all arguments from StartComponent.
          
        Return:
            
          none

        """

        # arguments
        self.args = {}
        self.args.setdefault("Enabled", "no")
        self.args.setdefault("ComponentDir", None)
        self.args.setdefault("ProdAgentName", None)
        self.args.setdefault("PollInterval", 3600)
        self.args.setdefault("ExportCommand", None)
        self.args.setdefault("passwd", None)
        self.args.setdefault("host", None)
        self.args.setdefault("user", None)
        self.args.setdefault("socketFileLocation", None)
        self.args.setdefault("portNr", None)
        self.args.setdefault("dbName", None)
        self.args.setdefault("Logfile", None)

        # update parameters
        self.args.update(args)

        # define log file
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        # create log handler
        LoggingUtils.installLogHandler(self)

        # inital log information
        logging.info("Monitoring starting...") 

        # enabled
        if self.args['Enabled'] in ['yes', 'y', 'YES', 'Y']:
            self.enabled = True
        else:
            self.enabled = False

        # compute poll delay
        delay = int(self.args['PollInterval'])
        if delay < 60:
            delay = 60 # a minimum value

        seconds = str(delay % 60)
        minutes = str((delay / 60) % 60)
        hours = str(delay / 3600)

        self.pollDelay = hours.zfill(2) + ':' + \
                         minutes.zfill(2) + ':' + \
                         seconds.zfill(2)

        # determine output file
        self.outputFile = "ProdAgentStatus.sql"

        # tables to be dumped
        self.tableList = "st_job_attr st_job_fail_attr st_job_failure " + \
                         "st_job_success"

        # build dump command
        if self.args['portNr'] in [None, '']:
            access = " --socket=" + self.args['socketFileLocation']
        else:
            access = " --port=" + str(self.args['portNr'])

        self.dumpCommand = "mysqldump --host=" + self.args['host'] + \
                           " --single-transaction" + \
                           " --password=" + self.args['passwd'] + \
                           " --result-file=" + self.outputFile + \
                           " --quick" + \
                           " --user=" + self.args['user'] + \
                           " --add-drop-table" + \
                           access + \
                           " --databases " + self.args['dbName'] + \
                           " --tables " + self.tableList 

        # export command
        self.exportCommand = self.args['ExportCommand']

        # message service instances
        self.ms = None 
        
    ##########################################################################
    # handle events
    ##########################################################################

    def __call__(self, event, payload):
        """
        _operator()_

        Used as callback to handle events that have been subscribed to

        Arguments:
           
          event -- the event name
          payload -- its arguments
          
        Return:
            
          none
          
        """
        
        logging.debug("Received Event: %s" % event)
        logging.debug("Payload: %s" % payload)

        # start debug event
        if event == "Monitoring:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return

        # stop debug event
        if event == "Monitoring:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        # make database information available for global monitoring
        if event == "Monitoring:DumpInformation":

            # dump information
            self.dumpInformation()

            # generate new dump information cycle
            self.ms.publish('Monitoring:DumpInformation', '', self.pollDelay)
            self.ms.commit()
            return

        # wrong event
        logging.debug("Unexpected event %s, ignored" % event)

    ##########################################################################
    # dump information
    ##########################################################################

    def dumpInformation(self):
        """
        _dumpInformation_

        export DB information for global monitoring

        Arguments:

          none

        Return:

          none
        """

        # compute start time for dump command
        logging.info("dumping database information")

        # run dump command
        exitCode = self.executeCommand(self.dumpCommand, 120)

        # verify exit code
        if exitCode != 0:
            logging.error("Dump process failed. Error code: " + \
            str(exitCode) + \
            ". Error message should have been displayed before. Continuing...")
            return

        # make file available
        exitCode = self.executeCommand(self.exportCommand, 240)

        # verify exit code
        if exitCode != 0:
            logging.error("Export process failed. Error code: " + \
            str(exitCode) + \
            ". Error message should have been displayed before. Continuing...")
            return

        return
 
    ##########################################################################
    # start component execution
    ##########################################################################

    def startComponent(self):
        """
        _startComponent_

         initialization 
        
        Arguments:
        
          none
          
        Return:
        
          none

        """
       
        # create message service instance
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("Monitoring")
        
        # subscribe to messages
        self.ms.subscribeTo("Monitoring:StartDebug")
        self.ms.subscribeTo("Monitoring:EndDebug")
        self.ms.subscribeTo("Monitoring:DumpInformation")

        # generate first polling cycle
        self.ms.remove("Monitoring:DumpInformation")
        if self.enabled:
            self.ms.publish("Monitoring:DumpInformation", "")
            self.ms.commit()

        # wait for messages
        while True:
            
            # get a single message
            messageType, payload = self.ms.get()

            # commit the reception of the message
            self.ms.commit()
            
            # perform task
            self.__call__(messageType, payload)
            
    ##########################################################################
    # execute a command
    ##########################################################################

    def executeCommand(self, command, timeOut):
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

        return exitCode
        
    ##########################################################################
    # get version information
    ##########################################################################

    @classmethod
    def getVersionInfo(cls):
        """
        _getVersionInfo_
        
        return version information 
        """
        
        return "Monitoring: " + __version__ + "\n"
   
