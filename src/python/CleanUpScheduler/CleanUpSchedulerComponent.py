#!/usr/bin/env python
"""
_CleanUpSchedulerComponent_

ProdAgent Component to schedule cleanup jobs to remove
unmerged files

"""

import logging
import os
import ProdAgentCore.LoggingUtils as LoggingUtils
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session


from MergeSensor.MergeSensorDB.Interface.MergeSensorDB import MergeSensorDB
from MergeSensor.MergeSensorError import MergeSensorError, MergeSensorDBError
from MergeSensor.MergeCrossCheck import MergeSensorCrossCheck
from MergeSensor.MergeCrossCheck import listAllMergeDatasets 
import ProdCommon.MCPayloads.CleanUpTools as CleanUpTools
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError
from DBSAPI.dbsApiException import DbsConnectionError
import time







class CleanUpTasks(dict):
    """
    _CleanUpTasks_

    Helper class to organise cleanup tasks by site.
    Essentially a map of block/Site: list of LFNs 

    """
    def __init__(self):
        dict.__init__(self)

    def addFiles(self, blockOrSite, *lfns):
        """
        _addFiles_

        Add a list of files to be cleaned up at a site

        """
        if not self.has_key(blockOrSite):
            self[blockOrSite] = []
        self[blockOrSite].extend(lfns)
        return
    
    
    


class CleanUpSchedulerComponent:
    """
    _CleanUpSchedulerComponent_


    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
                
        self.args.update(args)
        
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        LoggingUtils.installLogHandler(self)

        # //
        # // check default parameters
        # // 
        self.checkConfig()

        msg = "CleanUpScheduler Started:\n"
        logging.info(msg)
        
        
     
        #cleanup specs directory
        self.args.setdefault("CleanupJobSpecs",os.path.join(self.args['ComponentDir'],'cleanup-specs'))
        
        self.queueMode = False
        self.pollActive = True
        
        if str(self.args['QueueJobMode']).lower() == "true":
            self.queueMode = True  
            
        if str(self.args['PollActive']).lower() == "false":
            self.pollActive = False 

        cycleDelay = int(self.args['CleanUpInterval'])
        
        seconds = str(cycleDelay % 60)
        minutes = str((cycleDelay / 60) % 60)
        hours = str(cycleDelay / 3600)

        self.cleanUpDelay = hours.zfill(2) + ':' + minutes.zfill(2) + ':' + seconds.zfill(2)
        logging.debug("cleanUpInterval")
        logging.debug(self.cleanUpDelay)
        
        self.lfnLimit = int(self.args['LFNLimit'])
        
        #dbs reader parameters
        self.dbsReader = None
        self.delayDBS = 100
        
        self.mergeDB = MergeSensorDB()
         

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        msg = "Recieved Event: %s" % event
        msg += "Payload: %s" % payload
        logging.debug(msg)

        if event == "CleanUp:Cycle":
            self.cycle()
            return
        elif event == "CleanUp:CycleOnce":
            self.cycleOnce()
            return
        
        elif event == "CleanUpScheduler:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "CleanUpScheduler:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return


    def checkConfig(self):
        """
        _checkConfig_
        Checks whether the default configuration found in ProdAgentConfig. If not then it assigns the default values
        
        """
        
        if not self.args.has_key('PollActive'):
           self.args['PollActive'] = False
        if not self.args.has_key('QueueJobMode'):
           self.args['QueueJobMode'] = True
        if not self.args.has_key('CleanUpInterval'): 
           self.args['CleanUpInterval'] = 360000
        if not self.args.has_key('LFNLimit'):
           self.args['LFNLimit'] = 10
  
        

        return



    def cycle(self):
        """
        _cycle_

        Run a polling cycle and publish the next cycle triggering event

        """
        
        self.cycleOnce()
                
        #Generate new cleanup cycle after specified cleanup interval

        self.ms.publish("CleanUp:Cycle","Cleanup cycle called",self.cleanUpDelay)
        self.ms.commit()
        return

    def connectToDBS (self):
        """
        _connectToDBS_
      
        Getting dbs connection
        Arguments:
              None
        Return:
              None 

        """
           
        connected = False
        while not connected:

          try:
               
              self.dbsReader = DBSReader(self.args['DBSURL'])
          except (DBSReaderError,DbsConnectionError), ex:
              logging.error("Failed to connect to DBS (Exception: %s) Trying again in %s seconds" % (str(ex),self.delayDBS))
              time.sleep(self.delayDBS)
          else:
              connected = True 


    def cycleOnce(self):
        """
        _cycleOnce_

        Do a single update

        """
        try:
            # //
            # // Check LFN Threshold set by user
            # //
            if self.lfnLimit<=0:
                   logging.info("No Job Generated, LFNLimit set less than or equal to 0")
                   return
    
            # //
            # //Connect to DBS
            # //
            self.connectToDBS()
            
            #  //
            # // file block names in MergeSensor Db are actually the SENames
            #//
            siteTasks = CleanUpTasks()
                
                   
            try:
               # //
               # // Retrive list of Datasets currently being watched by MergeSensor
               # //
               listAllMergeDataset = listAllMergeDatasets()
            except Exception, ex:
               logging.error("DBSReader Error: LIST DATASET failed, Exception: %s " % ex) 
               return  
            for unmergedDataset in listAllMergeDataset:
                               
                mergedDataset = unmergedDataset.replace("-unmerged", "")                      
                mergeXCheck = MergeSensorCrossCheck(unmergedDataset)            
                unmergedToMergeFiles = mergeXCheck.getFileMap()                    
                
                #  //
                # // List of all merged LFNs known to merge sensor
                #//
                mergedLFNs = [ v for v in unmergedToMergeFiles.values() 
                               if v != None ]
               
                  
    
                #  //
                # // List of all merged LFNS that are known to DBS
                #//
                goodMerges = None
                try:
                 goodMerges = self.dbsReader.crossCheck(mergedDataset, *mergedLFNs)
                except Exception, ex:
                 logging.error("DBSReader crosscheck for goodmerges failed Exception: %s" % ex)
                 continue  
                          
    
                doneUnmergedFiles = []
                doneMergedFiles = []
                for unmerged, merged in unmergedToMergeFiles.items():
                    if merged in goodMerges:
                        doneUnmergedFiles.append(unmerged)
                        doneMergedFiles.append(merged)
                                               
    
                #  //
                # // Get a list of block/site names for each LFN
                #//
                blockMap = mergeXCheck.getBlocksMap()           
                i=0             
                for lfn in doneUnmergedFiles:
                    i=i+1
                    siteTasks.addFiles(blockMap[lfn], lfn)          
               
              
            #  //For each site in siteTasks, generate a cleanup job spec with 
            # // the list of files to be cleaned, then publish the
            #//  cleanup workflow, job spec etc
            
            # //
            # //generate cleanup workflow spec
            # //
            
             
            cleanUpWFs = CleanUpTools.createCleanupWorkflowSpec()    
    
            if (len(siteTasks) != 0):
     
                wfspec=os.path.join(self.args['CleanupJobSpecs'],cleanUpWFs.payload.workflow + '-workflow.xml' )
                cleanUpWFs.save(wfspec)
    
            # //
            # //Publishing newworkflow event
            # //
                self.ms.publish("NewWorkflow", wfspec)
                self.ms.commit() 
    
            # //
            # //Generating cleanup jobspecs with list of lfns to be deleted
            # //
    
            cleanUpJobSpec = [] 
    
            # //
            # //generate cleanup jobspec having inputfiles lfns
            # //
            for x in siteTasks.keys():        
    
                   njobs = len(siteTasks[x])/self.lfnLimit
                    
                   if (len(siteTasks[x])%self.lfnLimit) > 0 :               
                    njobs = njobs + 1
                   
                   ref=0
                   
                   for i in range (0,njobs):
                                      
                     lfns = siteTasks[x][ref:ref+self.lfnLimit]
                     logging.debug("Cleaning: %s" % str(lfns)) 
                     cleanUpJobSpec.append(CleanUpTools.createCleanupJobSpec(cleanUpWFs,x,*lfns))
                     self.mergeDB.removingState(*lfns)                                    
                     self.mergeDB.commit()
    
                     ref=ref+self.lfnLimit 
                        
            
              
            for i in range (0,len(cleanUpJobSpec)):
              
              jobspec=os.path.join(self.args['CleanupJobSpecs'],cleanUpJobSpec[i].parameters["JobName"] + ".xml") 
              cleanUpJobSpec[i].save(jobspec)
              logging.debug('JobSpec Saved')
    
              # //
              # //publishing jobspec
              # //
              self.publishCreateJob(jobspec)
        
        except (MergeSensorDBError, MergeSensorError, StandardError), ex:
            logging.error("Error during poll: %s" % str(ex))
            return
   
        return

    def publishCreateJob(self,cleanupSpecURL):
        """
        _publishCreateJob_

        Publish create job event with cleanupSpecURL provided as PAYLOAD

        Arguments:
         cleanupSpecURL -- cleanup specification file name
  
        Return:
         None

        """

        if self.queueMode:
           self.ms.publish("QueueJob",cleanupSpecURL)
        else:
           self.ms.publish("CreateJob",cleanupSpecURL)

        self.ms.commit()

        return

    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        # //
        # //create directory if necessary
        # //
        if not os.path.exists(self.args["CleanupJobSpecs"]):
          os.makedirs(self.args["CleanupJobSpecs"])

        # //
        # //create message service
        # //
        self.ms  = MessageService()
        # register
        self.ms.registerAs("CleanUpScheduler")

        # subscribe to messages
        self.ms.subscribeTo("CleanUp:Cycle")
        self.ms.subscribeTo("CleanUp:CycleOnce")
        self.ms.subscribeTo("CleanUpScheduler:StartDebug")
        self.ms.subscribeTo("CleanUpScheduler:EndDebug")

        ## Debug level
        self.ms.publish("RequestInjector:StartDebug","none")
        self.ms.publish("JobCreator:StartDebug","none")
        self.ms.publish("JobSubmitter:StartDebug","none")
        self.ms.commit()
  
        # //
        # // generate first cleanup cycle
        # //
        
        if self.pollActive:
          self.ms.remove("CleanUp:Cycle")
          self.ms.publish("CleanUp:Cycle", "")
          self.ms.commit()

 
        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            
            type, payload = self.ms.get()              
            self.ms.commit()            
            logging.debug("CleanUpScheduler: %s, %s" % (type, payload))
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()

        
