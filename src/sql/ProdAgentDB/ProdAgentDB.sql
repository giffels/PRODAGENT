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
   job_type VARCHAR(255),
   time TIMESTAMP NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   PRIMARY KEY (job_index)

) TYPE=InnoDB;

/*
 * Table to store list based attributes of a successful Job
 *
 */
CREATE TABLE st_job_attr (
   attr_index INT NOT NULL AUTO_INCREMENT,
   job_index INT NOT NULL,
   attr_name VARCHAR(255),
   attr_class ENUM("run_numbers", "output_files", "output_datasets", "input_files", 'timing'),
   
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
   job_type VARCHAR(255),
   time TIMESTAMP NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   PRIMARY KEY (job_index)
) TYPE=InnoDB;

/*
 * Table to store list based attributes of a failed Job
 *
 */
CREATE TABLE st_job_fail_attr (
   attr_index INT NOT NULL AUTO_INCREMENT,
   job_index INT NOT NULL,
   attr_name VARCHAR(255),
   attr_class ENUM("timing" ,"run_numbers"),
   attr_value BLOB,
   FOREIGN KEY(job_index)
     REFERENCES st_job_failure(job_index)
       ON DELETE CASCADE,
 
   PRIMARY KEY (attr_index)
) TYPE=InnoDB;


/*
 * =====================End StatTracker DB Tables================== 
 */

/*
A merge_dataset table stores information on datasets watched by the
MergeSensor component.

Fields:

 id            dataset internal id
 prim          primary dataset name
 tier          datatier name
 processed     processed dataset name
 pollTier      datatier used for polling DBS in multiple datatier datasets
 psethash      psethash used in newdataset event specification
 status        status (opened or closed)
 started       watched started on
 updated       last information updated
 version       CMSSW version
 workflow      workflow name
 mergedlfnbase LFN base for merge files
 category      category (preproduction, etc)
 timeStamp     workflow time stamp
 sequence      output sequence number
*/

CREATE TABLE merge_dataset
  (
    id int NOT NULL auto_increment,
    prim varchar(255) NOT NULL default '',
    tier varchar(255) NOT NULL default '',
    processed varchar(255) NOT NULL default '',
    polltier varchar(255) NOT NULL default '',
    psethash varchar(255) NOT NULL default '',
    status enum("open", "closed") default 'open',
    started timestamp NOT NULL default '0000-00-00 00:00:00',
    updated timestamp NOT NULL default CURRENT_TIMESTAMP
                      on update CURRENT_TIMESTAMP,
    version varchar(255) NOT NULL default '',
    workflow varchar(255) NOT NULL default '',
    mergedlfnbase varchar(255) NOT NULL default '',
    category varchar(255) NOT NULL default '',
    timestamp varchar(255) NOT NULL default '',
    sequence int NOT NULL default '0',

    primary key(id),
    unique(prim,tier,processed)
  )
 TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
A merge_outputfile table stores information on merged files in currently
watched datasets.

Fields:

 id       internal output file id
 name     file name
 instance creation instance number
 status   status (merged or to be done again)
 dataset  dataset id

*/

CREATE TABLE merge_outputfile
  (
    id int NOT NULL auto_increment,
    name text NOT NULL default '',
    instance int NOT NULL default '1',
    status enum("merged", "do_it_again") default "merged",
    mergejob varchar(255) NOT NULL default '',
    dataset int NOT NULL default '0',

    PRIMARY KEY(id),

    FOREIGN KEY(dataset) references merge_dataset(id) ON DELETE CASCADE
  )
 TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
A merge_fileblock table stores information on file blocks of
currently watched datasets.

Fields:

  id   internal file block id
  name block name
*/

CREATE TABLE merge_fileblock
  (
    id int NOT NULL auto_increment,
    name varchar(255) NOT NULL default '',

    PRIMARY KEY(id)
  )
  TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
A merge_inputfile table stores information on input files in currently
watched datasets.

Fields:

 id         internal input file id
 name       file name
 block      block name as returned by DBS
 status     status (merged or blocked)
 dataset    dataset id
 mergedfile associated output merged file id

*/

CREATE TABLE merge_inputfile
  (
    id int NOT NULL auto_increment,
    name text NOT NULL default '',
    block int NOT NULL default '0',
    status enum("unmerged", "merged", "invalid") default "unmerged",
    dataset int NOT NULL default '0',
    mergedfile int default NULL,
    filesize int NOT NULL default '0',

    PRIMARY KEY(id),

    FOREIGN KEY(dataset) references merge_dataset(id) ON DELETE CASCADE,
    FOREIGN KEY(mergedfile) references merge_outputfile(id) ON DELETE CASCADE,
    FOREIGN KEY(block) references merge_fileblock(id) ON DELETE CASCADE
  )
 TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
A merge_control table stores information on the status of the
MergeSensor component.

Fields:

 id            internal id
 status        running or stopped
 mergedjobs    number of merge jobs generated so far
 limited       is there a limit on jobs generation? (yes or no)
 remainingjobs number of jobs that still can be generated
*/

CREATE TABLE merge_control
  (
    id int NOT NULL auto_increment,
    status enum("running", "stopped") default "running",
    mergedjobs int NOT NULL default '0',
    limited enum("no", "yes") default "no",
    remainingjobs int NOT NULL default '0',

    PRIMARY KEY(id)
  )
 TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
 * =====================Start DatasetInjector Tables===============
 */
CREATE TABLE di_job_owner(
  owner_name VARCHAR(255),
  owner_index INT NOT NULL AUTO_INCREMENT,
  PRIMARY KEY(owner_index),
  UNIQUE( owner_name)
) TYPE=InnoDB;


CREATE TABLE di_job_queue(
  job_id INT NOT NULL AUTO_INCREMENT,
  owner_index INT NOT NULL,
  time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP 
     ON UPDATE CURRENT_TIMESTAMP,
  
  fileblock VARCHAR(255),
  se_names BLOB,
  lfns BLOB,
  max_events INT DEFAULT NULL,
  skip_events INT DEFAULT NULL,
  status ENUM ("new", "used") DEFAULT "new",
  
  PRIMARY KEY (job_id),
  FOREIGN KEY (owner_index)
     REFERENCES di_job_owner(owner_index)
       ON DELETE CASCADE

) TYPE=InnoDB;


CREATE INDEX di_job_index USING BTREE ON di_job_queue (owner_index);

/*
 * ======================End Dataset Injector tables===============
 */

/*
 * ======================Start Robust Service call tables===============
 */

CREATE TABLE ws_last_call
  (
    component_id          varchar(150)    not null,
    id                    int(11)        auto_increment,
    tag                   varchar(150)   not null default '0',
    service_call          varchar(255)   not null,
    server_url            varchar(255)   not null,
    service_parameters    mediumtext     not null,
    call_state            ENUM('call_placed','result_retrieved'),
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,

    index(id),
    unique(component_id,service_call,server_url)

  ) Type=InnoDB;

CREATE TABLE ws_queue
   (
    component_id          varchar(150)    not null,
    handler_id            varchar(150)    not null,
    server_url            varchar(150)    not null,
    state                 varchar(150)    not null,
    parameters            mediumtext      not null,
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP
   ) Type=InnoDB;


/*
 * ======================End Robust Service call tables===============
 */

/*
 * ======================Start PA interface tables===============
 */

CREATE TABLE pm_state
   (
    id                    varchar(150)    not null,
    state                 varchar(150)    not null,
    parameters            mediumtext      not null,
    primary key(id)
   ) Type=InnoDB;

CREATE TABLE pm_request
   (
    id                    varchar(150)    not null,
    url                   varchar(150)    not null,
    priority              int(11)         not null
   ) Type=InnoDB;

CREATE TABLE pm_allocation
   (
    id                    varchar(150)    not null,
    request_id            varchar(150)    not null,
    catagory              varchar(150)    not null,
    state                 varchar(150)    not null
   ) Type=InnoDB;

CREATE TABLE pm_job
   (
    id                    varchar(150)    not null,
    request_id            varchar(150)    not null,
    url                   varchar(150)    not null,
    downloaded            int(11)         not null default 0,
    catagory              varchar(150)    not null
   ) Type=InnoDB;



/*
 * ======================End PA interface tables===============
 */
