#!/usr/bin/env python
"""
_DQMInjectorComponent_

Create Offline DQM Histogram collection jobs in an automated manner
The component will poll a data source to retrieve a list of files
in a run within a dataset and generate a job spec to pull the DQM 
histograms out of the file and post them to the DQM Server. 
"""

import os
import logging

from ProdCommon.Database import Session
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
import ProdAgentCore.LoggingUtils as LoggingUtils

import DQMutils
from urllib2 import HTTPError

class DQMInjectorComponent:
    """
    _DQMInjectorComponent_
    """
    
    def __init__(self, **args):
        
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args.update(args)
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(
                self.args['ComponentDir'],
                "ComponentLog")
        LoggingUtils.installLogHandler(self)

        self.plugin = None
        self.ms = None
        msg = "DQMInjector Component Started\n"
        msg += " => Plugin: %s\n" % self.plugin
        logging.info(msg)


    def __call__(self, event, payload):
        """
        _operator(message, payload)_

        Respond to messages from the message service

        """
        logging.info("Message=%s Payload=%s" % (event, payload))
        
        if event == "DQMInjector:CollectRun":
            logging.info("event : DQMInjector:CollectRun \npayload : %s" % payload)
            return
                        
        if event == "DQMInjector:Upload":
          url        = 'http://dd:8030/dqm/tier-0/data/put'
          uploadFile = self.args['UploadDir'] + '/' + payload
          producer   = 'ProdSys'
          step       = 'Pass-1'
          try:
            (headers,data) = DQMutils.upload(url      = url,
                                             producer = producer,
                                             step     = step,
                                             file     = uploadFile)
            logging.info('SUCCESS')
            logging.info('Status code: %s'% headers.get("Dqm-Status-Code", "None"))
            logging.info('Message:     %s'% headers.get("Dqm-Status-Message", "None"))
            logging.info('Detail:      %s'% headers.get("Dqm-Status-Detail", "None"))

          except DQMutils.UploadFileError, e:
            logging.info(e.msg)
            
          except HTTPError, e:
            logging.info('ERROR - %s' % e)
            logging.info('Status code: %s' % e.hdrs.get("Dqm-Status-Code", "None"))
            logging.info('Message:     %s' % e.hdrs.get("Dqm-Status-Message", "None"))
            logging.info('Detail:      %s' % e.hdrs.get("Dqm-Status-Detail", "None"))
            logging.info('#####DEBUG##### called by DQMInjectorComponent.py')
            
          return

        return
            
    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component
        
        """                                   
        # create message service
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("DQMInjector")                                                                                
        # subscribe to messages
        self.ms.subscribeTo("DQMInjector:CollectRun")
        self.ms.subscribeTo("DQMInjector:Upload")
        
        self.ms.subscribeTo("DQMInjector:StartDebug")
        self.ms.subscribeTo("DQMInjector:EndDebug")
        
        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            msgtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("DQMInjector: %s, %s" % (msgtype, payload))
            self.__call__(msgtype, payload)
            Session.commit_all()
            Session.close_all()
            

