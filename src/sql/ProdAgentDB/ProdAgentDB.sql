/* This should be the default setting for InnoDB tables */   
SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;
/* Do not commit after every transaction*/
SET AUTOCOMMIT = 0;

/*
 ***********************JOB STATE TABLES****************************

-js_JobType and js_JobSpec are represented as one table
-tr_FlagInstance and tr_TriggerInstance are represented as one table

   Action
    | 1
    |
    | *   *      1 
tr_Trigger---------js_JobSpec
                        | 1
                        |
                        | *
                  js_JobInstance

There is no relation between triggers and
js_JobInstance as this deals with 
job submission which is a seperate component and has its
own states managed by external components.
 */

/* 
A JobSpec table contains information related to the the request (not 
the actual jobs being run):
-JobSpecID: Assigned by the ProdAgent Manager
-JobType: Assigned by the ProdAgent Manager (e.g. merge job, generation job,...)
-MaxRetries: Number of times the error handler should resubmit a job 
for this job spec.
-Retries: Number of times the error handler has already handled a failure.
-State: The state a job class can be in. Below a diagram of the state 
order (which needs to be enforced by an database access layer.

register--->create-->inProgress-->finished

inProgress hides the complex state of many jobs failing an being re submitted.

-CacheDir Location: Even when you resubmit, you will still use the same 
tarfile generated which is located in the CacheDir
-MaxRacers: The maximum number of (the same) jobs we can submit 
simultaneously (usually you only submit one at a time).
-Racers: number of jobs running. During creation that contains all the 
necessary files to run the job.
*/
CREATE TABLE js_JobSpec(
   JobSpecID VARCHAR(255) NOT NULL,
   JobType VARCHAR(255) NOT NULL,
   MaxRetries INT NOT NULL,
   MaxRacers INT NOT NULL,
   Retries INT NOT NULL,
   Racers INT NOT NULL,
   Time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   State ENUM("register","create","inProgress","finished") 
       NOT NULL,
   CacheDirLocation VARCHAR(255) NULL,
   PRIMARY KEY (JobSpecID)
   ) TYPE=InnoDB;


/*
A job instance is a job submission based on the job spec.  There can be 
multiple job instance running at the same time associated to a job class.

-Location: the site to which the job was submitted
-JobReportLocation: where the jobs report is stored.

*/
CREATE TABLE js_JobInstance(
   JobSpecID VARCHAR(255) NOT NULL,
   JobInstanceID VARCHAR(255) ,
   Location VARCHAR(255) ,
   /* Once a job failed a job report can be generated and 
      the url of this send to the error handler. The error
      handler stores this job report locally and registers
      the location of it in the JobReportLocation variable */
   JobReportLocation VARCHAR(255),
   Time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   /* Not every MySQL version supports cascade or foreign keys */
   CONSTRAINT `0_1` FOREIGN KEY(JobSpecID) 
       REFERENCES js_JobSpec(JobSpecID) 
       ON DELETE CASCADE,
   INDEX(JobSpecID),
   UNIQUE(JobInstanceID)
   ) TYPE=InnoDB;

/*
The Job(spec) state is defined by:
(1): sequential messages and actions of the components 
(2): states as defined in the js_JobSpec to deal with failures and 
retries
(3): an augmentation of (1) where parallel process perform actions
and need to be synchronized when they all finished. 

To address (3) we keep track of a set of flags associated to a 
jobSpecId, which if all set to finished, trigger a certain action.
We do not use the MySQL triggers as the actions are external
to MySQL.
*/

CREATE TABLE tr_Trigger(
   JobSpecID VARCHAR(255) NOT NULL,
   TriggerID VARCHAR(255) NOT NULL,
   FlagID VARCHAR(255) NOT NULL,
   FlagValue ENUM("null","start","finished") NOT NULL,
   Time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   /* Not every MySQL version supports cascade or foreign keys */
   CONSTRAINT `0_2` FOREIGN KEY(JobSpecID) 
       REFERENCES js_JobSpec(JobSpecID) 
       ON DELETE CASCADE,
   UNIQUE(JobSpecID,TriggerID,FlagID),
   INDEX(TriggerID)
   ) TYPE=InnoDB;

CREATE TABLE tr_Action(
   JobSpecID VARCHAR(255) NOT NULL,
   TriggerID VARCHAR(255) NOT NULL,
   /* Action name associated to this trigger. This name
   is associated to some python code in an action registery
   */
   ActionName VARCHAR(255) NOT NULL,
   UNIQUE(JobSpecID,TriggerID,ActionName)
   ) TYPE=InnoDB;


   
/*************************************************************************
Message service tables.

See full specification and documentation at:

http://cms-service-mcproto.web.cern.ch/cms-service-mcproto/Doc/
**************************************************************************/

/*
A ms_type table stores information on message types.

Fields:

 typeid   id
 name     message name
*/

CREATE TABLE `ms_type` (
  `typeid` int(11) NOT NULL auto_increment,
  `name` varchar(255) NOT NULL default '',
  PRIMARY KEY `typeid` (`typeid`),
  UNIQUE (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*
A ms_process table stores information on components.
                                                                                
Fields:
                                                                                
 procid   id
 name     component name
 host     host name
 pid      process id in host name
*/

CREATE TABLE `ms_process` (
  `procid` int(11) NOT NULL auto_increment,
  `name` varchar(255) NOT NULL default '',
  `host` varchar(255) NOT NULL default '',
  `pid` int(11) NOT NULL default '0',
  PRIMARY KEY `procid` (`procid`),
  UNIQUE (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*
A ms_history table stores information on the complete message history.
                                                                                
Fields:
                                                                                
 messageid   id
 type        message type id
 #source      source component id
 dest        target component id
 payload     message payload
 time        time stamp
*/

CREATE TABLE `ms_history` (
  `messageid` int(11) NOT NULL auto_increment,
  `type` int(11) NOT NULL default '0',
  `source` int(11) NOT NULL default '0',
  `dest` int(11) NOT NULL default '0',
  `payload` text NOT NULL,
  `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `delay` varchar(50) NOT NULL default '00:00:00',

  PRIMARY KEY `messageid` (`messageid`),
  FOREIGN KEY(`type`) references `ms_type`(`typeid`),
  FOREIGN KEY(`source`) references `ms_process`(`procid`),
  FOREIGN KEY(`dest`) references `ms_process`(`procid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*
A ms_message table stores information on the messages to be delivered.
                                                                                
Fields:
                                                                                
 messageid   id
 type        message type id
 #source      source component id
 dest        target component id
 payload     message payload
 time        time stamp
*/

CREATE TABLE `ms_message` (
  `messageid` int(11) NOT NULL auto_increment,
  `type` int(11) NOT NULL default '0',
  `source` int(11) NOT NULL default '0',
  `dest` int(11) NOT NULL default '0',
  `payload` text NOT NULL,
  `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `delay` varchar(50) NOT NULL default '00:00:00',

  PRIMARY KEY `messageid` (`messageid`),
  FOREIGN KEY(`type`) references `ms_type`(`typeid`),
  FOREIGN KEY(`source`) references `ms_process`(`procid`),
  FOREIGN KEY(`dest`) references `ms_process`(`procid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*
A ms_subscription table stores information on the message subscriptions.
                                                                                
Fields:

 subid   id
 procid  component id
 typeid  message type id
*/

CREATE TABLE `ms_subscription` (
  `subid` int(11) NOT NULL auto_increment,
  `procid` int(11) NOT NULL default '0',
  `typeid` int(11) NOT NULL default '0',
  KEY `subid` (`subid`),
  UNIQUE (`procid`,`typeid`),
  FOREIGN KEY(`procid`) references `ms_process`(`procid`),
  FOREIGN KEY(`typeid`) references `ms_type`(`typeid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*
 * =====================StatTracker DB Tables================== 
 */

/*
 * Table to record Job Success information	
 *
 */
CREATE TABLE st_job_success (
   job_index INT NOT NULL AUTO_INCREMENT,
   job_spec_id VARCHAR(255) NOT NULL,
   workflow_spec_id VARCHAR(255) NOT NULL,
   exit_code INT,
   task_name VARCHAR(255),		
   status VARCHAR(255),
   site_name VARCHAR(255),
   host_name VARCHAR(255),
   se_name VARCHAR(255),
   events_read INT DEFAULT 0,
   events_written INT DEFAULT 0,
   PRIMARY KEY (job_index)

) TYPE=InnoDB;

/*
 * Table to store list based attributes of a successful Job
 *
 */
CREATE TABLE st_job_attr (
   attr_index INT NOT NULL AUTO_INCREMENT,
   job_index INT NOT NULL,
   
   attr_class ENUM("run_numbers", "output_files", "output_datasets", "input_files"),
   
   attr_value BLOB,

   FOREIGN KEY(job_index)
     REFERENCES st_job_success(job_index)
       ON DELETE CASCADE,
 
   PRIMARY KEY (attr_index)
) TYPE=InnoDB;


/*
 * Table to store failed job information
 *
 */
CREATE TABLE st_job_failure (
   job_index INT NOT NULL AUTO_INCREMENT,
   job_spec_id VARCHAR(255) NOT NULL,
   workflow_spec_id VARCHAR(255) NOT NULL,
   exit_code INT,
   task_name VARCHAR(255),		
   status VARCHAR(255),
   site_name VARCHAR(255),
   host_name VARCHAR(255),
   se_name VARCHAR(255),
   error_type VARCHAR(255),
   error_code INT,
   error_desc BLOB,

   PRIMARY KEY (job_index)
) TYPE=InnoDB;


/*
 * =====================End StatTracker DB Tables================== 
 */
