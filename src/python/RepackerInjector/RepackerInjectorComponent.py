#!/usr/bin/env python
"""
_RepackerInjectorComponent_

Component for generating Repacker JobSpecs

"""



__version__ = "$Revision: 1.8 $"
__revision__ = "$Id: RepackerInjectorComponent.py,v 1.8 2007/06/25 17:12:07 hufnagel Exp $"
__author__ = "kss"


import logging
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
#import MySQLdb
import ConfigDB
import DbsLink
from LumiServerLink import LumiServerLink
import RepackerIterator
from RepackerHelper import RepackerHelper
import os
import sys
import traceback
from ProdCommon.MCPayloads.Tier0WorkflowMaker import Tier0WorkflowMaker
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

from ProdCommon.MCPayloads.JobSpec import JobSpec


"""
!!! Every 'print' statement line ends with '# XXX' comment !!!
(means 'print's will be replaced with 'log()'s)
"""


class RepackerInjectorComponent:
    """
    _RepackerInjectorComponent_

    Query DBS and generate repacker job specs

    """
    def __init__(self, **args):
        #self.workflow_by_ds={}
        self.args = {}
        #  //
        # // Set default args
        #//

        #  //
        # // Override defaults with those from ProdAgentConfig
        #//
        self.args.update(args)
        #print "DB=[%s] h=[%s] p=[%s] u=[%s]" % (self.args["DbsDbName"],self.args["DbsDbHost"],self.args["DbsDbPort"],self.args["DbsDbUser"])
        LoggingUtils.installLogHandler(self)
        msg = "RepackerInjector Started:\n"
        logging.info(msg)
        logging.info("args %s"%str(args))
        logging.info("URL=[%s] level=[%s]" % (self.args["DbsUrl"],self.args["DbsLevel"]))

        # Create repacker helper for generation and modification of workflow and job specs
        self.repacker_helper=RepackerHelper(args)
        if(self.args.has_key('LumiServerUrl')):
            self.lumisrv=LumiServerLink(url=self.args["LumiServerUrl"],level=self.args["DbsLevel"])
        else:
            self.lumisrv=LumiServerLink(url=None,level=self.args["DbsLevel"])


    def __call__(self, message, payload):
        """
        _operator()_

        Define responses to messages

        """
        msg = "Recieved Event: %s " % message
        msg += "Payload: %s" % payload
        logging.debug(msg)

        #  //
        # // All components respond to standard debugging level control
        #//
        if message == "RepackerInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if message == "RepackerInjector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        #  //
        # // Component Specific actions.
        #//

        if message == "RepackerInjector:StartNewRun":
            #  //
            # // On demand action
            #//
            self.doStartNewRun(payload)
            return




    def doStartNewRun(self, payload):
        """
        Expects run number and source dataset name in the payload in the
        form "run_number primary_ds_name processed_ds_name"
        """
        logging.info("StartNewRun(%s)" % payload)
        items=payload.split(" ")
        if(len(items)!=3):
            logging.error("StartNewRun(%s) - bad payload format" % payload)
            return
        run_number = -1
        try:
            run_number =  int(items[0])
        except ValueError:
            logging.error("StartNewRun - bad runnumber [%s]" % items[0])
            return
        primary_ds_name = items[1]
        processed_ds_name = items[2]
        logging.info(
            "RunNumber %d PDS [%s] SDS [%s]" % (
            run_number, primary_ds_name, processed_ds_name)
            )


        #  //
        # // Generate workflow
        #// 
        workflowFile,workflowHash=self.repacker_helper.prepareWorkflow(run_number, primary_ds_name, processed_ds_name)
        
        self.ms.publish("NewWorkflow", workflowFile)
        self.ms.publish("NewDataset",workflowFile)
        self.ms.commit()
        
        #
        # Final solution is not supposed to need run number.
        # The run number is discovered in the DBS query and
        # overridden in the workflow before the job is submitted.
        #

        dbslink = DbsLink.DbsLink(url=self.args["DbsUrl"],
                                  level=self.args["DbsLevel"])

        file_res = dbslink.poll_for_files(primary_ds_name,
                                          processed_ds_name,
                                          run_number)

        for i in file_res:
            lfn,tags,file_lumis=i
            logging.info("Found file %s" % lfn)
            lumisection=file_lumis[0]['LumiSectionNumber']
            lumi_info=self.lumisrv.getLumiInfo(run_number,lumisection)
            #print "LUMIINFO",lumi_info
            res_job_error=self.submit_job(lfn,
                                          tags,
                                          primary_ds_name,
                                          processed_ds_name,
                                          lumi_info,
                                          workflowHash)
            if(not res_job_error):
                dbslink.setFileStatus(lfn, "submitted")
                dbslink.commit()
                logging.info("Submitted job for %s" % lfn)

        dbslink.close()
        return


    def submit_job(self, lfn, tags, pri_ds, pro_ds, lumi_info, ds_key):
        logging.info("Creating job for file [%s] tags %s"%(lfn,str(tags)))
        #
        # FIXME: NewStreamerEventStreamFileReader cannot use LFN
        #        either fix that or resolve PFN here via TFC
        #
        pfn = 'rfio:/?path=/castor/cern.ch/cms' + lfn
        logging.info("Creating job for file [%s] tags %s"%(pfn,str(tags)))

        # Creating job_spec
        job_spec=self.repacker_helper.createJobSpec(ds_key, tags, [pfn], lumi_info)
        
        self.ms.publish("CreateJob",job_spec)
        self.ms.commit()

        logging.info("CreateJob signal sent, js [%s]"%(job_spec,))
        return 0



    def startComponent(self):
        """
        _startComponent_

        Start up the component and define the messages that it subscribes to

        """


        #print "Started"
        # create message service
        self.ms = MessageService()
        # register this component
        self.ms.registerAs("RepackerInjector")

        # subscribe to messages
        self.ms.subscribeTo("RepackerInjector:StartDebug")
        self.ms.subscribeTo("RepackerInjector:EndDebug")

        self.ms.subscribeTo("RepackerInjector:StartNewRun")

        self.ms.commit()

        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("RepackerInjector: %s, %s" % (type, payload))
            #print "Message"
            self.__call__(type, payload)


