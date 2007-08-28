#!/usr/bin/env python
"""
_T0RepackerInjectorComponent_

Component for generating Repacker JobSpecs using T0 package and T0Stat Db

"""



__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: T0RepackerInjectorComponent.py,v 1.2 2007/08/27 17:16:44 kosyakov Exp $"
__author__ = "kss"


import logging
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from T0.RepackMgr.RepackerGenerator import RepackerGenerator
from T0.SplitInjector.RepackerSplitGenerator import SplitJobGenerator
from ProdCommon.Database import DbSession


class T0RepackerInjectorComponent:
    """
    _T0RepackerInjectorComponent_

    """
    def __init__(self, **args):
        #self.workflow_by_ds={}
        self.args = {}
        self.args.update(args)
        self.args['PollInterval']="00:00:15"
        self.args['WorkDir']=self.args['ComponentDir']
        LoggingUtils.installLogHandler(self)
        self.db=DbSession.getSession(self.args)
        msg = "T0RepackerInjector Started:\n"
        logging.info(msg)
        logging.info("args %s"%str(args))
        self.repack_gens={}
        self.current_run=0


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
        if message == "T0RepackerInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if message == "T0RepackerInjector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        if message == "T0RepackerInjector:StartNewRun":
            self.doStartNewRun(payload)
            return
        
        if message == "T0RepackerInjector:PollLoop":
            self.pollLoop()
            return
    




    def doStartNewRun(self, payload):
        """
        Expects run number as a payload
        """
        logging.info("StartNewRun(%s)" % payload)
        try:
            run_number =  int(payload)
        except ValueError:
            logging.error("StartNewRun - bad runnumber [%s]" % payload)
            return
        if(self.current_run==run_number):
            logging.error("Already started run %s" % str(run_number))
            return
        #sjg=SplitJobGenerator(run_number,"Run-%s"%str(run_number),self.args,self.db)

        repack=RepackerGenerator(run_number,"Run-%s"%str(run_number),self.args,self.db)
        split_plugin=SplitJobGenerator()
        repack.registerAlgoPlugin('split',split_plugin)
        #report=repack.pollAndCreateJobs()
        
        self.repack_gens[run_number]=repack
        self.current_run=run_number
        self.ms.publish("NewWorkflow",repack.wf_path)
        self.ms.publish("NewDataset",repack.wf_path)
        self.ms.commit()



    #
    # Create job spec and then publish CreateJob event
    #
    def submit_job(self,job_spec):

        logging.info("Creating job for [%s]"%job_spec)

        self.ms.publish("CreateJob",job_spec)
        self.ms.commit()

        logging.info("CreateJob signal sent, js [%s]"%(job_spec,))
        return 0


    def do_db_poll(self):
        """
         _do_db_poll_
         Calls RepackerGenerator to get list of ready for repacking files
         and to send the list to plugins for processing
         
        """
        logging.info("db_poll")
        #fname,updated_ts,job_name
        repack=self.repack_gens[self.current_run]
        report=repack.pollAndCreateJobs()
        if(not report):
            logging.info("No new files/trigger sections")
            return
        for algo in report.keys():
            fname,updated_ts,job_name=report[algo]
            logging.info(fname+" "+job_name+" "+str(updated_ts.keys()))
            repack.updateTsStatus(updated_ts.keys(),job_name)
            self.submit_job(fname)


    def pollLoop(self):
        #print "pollLoop"
        logging.info("Poll Loop invoked...")

        if(self.current_run):
            self.do_db_poll()
        
        self.ms.publish("T0RepackerInjector:PollLoop", "",self.args['PollInterval'])
        self.ms.commit()
        return



    def startComponent(self):
        #print "Started"
        # create message service
        self.ms = MessageService()
        # register this component
        self.ms.registerAs("T0RepackerInjector")

        # subscribe to messages
        self.ms.subscribeTo("T0RepackerInjector:StartDebug")
        self.ms.subscribeTo("T0RepackerInjector:EndDebug")

        self.ms.subscribeTo("T0RepackerInjector:StartNewRun")
        self.ms.subscribeTo("T0RepackerInjector:PollLoop")

        # generate first polling cycle
        self.ms.remove("T0RepackerInjector:PollLoop")
        self.ms.publish("T0RepackerInjector:PollLoop", "")
        self.ms.commit()

        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("T0RepackerInjector: %s, %s" % (type, payload))
            #print "Message"
            self.__call__(type, payload)


