/* This should be the default setting for InnoDB tables */   
SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;
/* Do not commit after every transaction*/
SET AUTOCOMMIT = 0;


/*
 *CREATE TABLE js_JobSpec(
 *  JobSpecID VARCHAR(255) NOT NULL,
 *  JobType VARCHAR(255) NOT NULL,
 *  MaxRetries INT NOT NULL,
 *  MaxRacers INT NOT NULL,
 *  Retries INT NOT NULL,
 *  Racers INT NOT NULL,
 *  Time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
 *  State ENUM("register","create","inProgress","finished") 
 *      NOT NULL,
 *  WorkflowID VARCHAR(255) default ' ',
 *  CacheDirLocation VARCHAR(255) NULL,
 *  PRIMARY KEY (JobSpecID)
 *  ) TYPE=InnoDB;
 */

/*
A job instance is a job submission based on the job spec.  There can be 
multiple job instance running at the same time associated to a job class.

-Location: the site to which the job was submitted
-JobReportLocation: where the jobs report is stored.

*/
/*
 *CREATE TABLE js_JobInstance(
 *  JobSpecID VARCHAR(255) NOT NULL,
 *  JobInstanceID VARCHAR(255) ,
 *  Location VARCHAR(255) ,
 *  Once a job failed a job report can be generated and 
 *     the url of this send to the error handler. The error
 *     handler stores this job report locally and registers
 *     the location of it in the JobReportLocation variable 
 *  JobReportLocation VARCHAR(255),
 *  Time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
 *   Not every MySQL version supports cascade or foreign keys
 *  CONSTRAINT `0_1` FOREIGN KEY(JobSpecID) 
 *      REFERENCES we_Job(JobS) 
 *      ON DELETE CASCADE,
 *  INDEX(JobSpecID),
 *  UNIQUE(JobInstanceID)
 *  ) TYPE=InnoDB;
 */

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
    secondarytiers varchar(255) NOT NULL default '',
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
 lfn      LFN of the merge output file
 instance creation instance number
 status   output file status status
 dataset  dataset id
 failures failures counter

*/

CREATE TABLE merge_outputfile
  (
    id int NOT NULL auto_increment,
    name text NOT NULL default '',
    lfn text NOT NULL default '',
    instance int NOT NULL default '1',
    status enum("merged", "do_it_again", "failed", "undermerge")
           default "undermerge",
    mergejob varchar(255) NOT NULL default '',
    dataset int NOT NULL default '0',
    failures int NOT NULL default '0',

    PRIMARY KEY(id),
    KEY (mergejob),
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
 eventcount number of events
 block      block name as returned by DBS
 status     input file status
 dataset    dataset id
 mergedfile associated output merged file id
 filesize   input file size
 run        run number list
 failures   access failures counter
 instance   number of merge jobs created for it

*/

CREATE TABLE merge_inputfile
  (
    id int NOT NULL auto_increment,
    name text NOT NULL default '',
    guid VARCHAR(100) NOT NULL,
    eventcount int NOT NULL default '0',
    block int NOT NULL default '0',
    status enum("unmerged", "undermerge", "merged", "invalid", 
	"removing", "removed") default "unmerged",
    dataset int NOT NULL default '0',
    mergedfile int default NULL,
    filesize int NOT NULL default '0',
    failures int NOT NULL default '0',
    instance int NOT NULL default '0',
    PRIMARY KEY(id),
    UNIQUE(guid, dataset),
    FOREIGN KEY(dataset) references merge_dataset(id) ON DELETE CASCADE,
    FOREIGN KEY(mergedfile) references merge_outputfile(id) ON DELETE CASCADE,
    FOREIGN KEY(block) references merge_fileblock(id) ON DELETE CASCADE
  )
 TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
A merge_workflow table stores information on workflows files associated
to currently watched datasets.

Fields:

  id      internal workflow id
  name    workflow name
  dataset dataset id

*/

CREATE TABLE merge_workflow
  (
    id int NOT NULL auto_increment,
    name varchar(255) NOT NULL default '',
    dataset int NOT NULL default '0',
    PRIMARY KEY(id),
    INDEX name(name),
    FOREIGN KEY(dataset) references merge_dataset(id) ON DELETE CASCADE
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
    id                    int(11)        auto_increment,
    component_id          varchar(150)    not null,
    handler_id            varchar(150)    not null,
    server_url            varchar(150)    not null,
    state                 varchar(150)    not null,
    parameters            mediumtext      not null,
    `delay`               varchar(50) NOT NULL default '00:00:00',
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    index(id)
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


CREATE TABLE pm_cooloff
   (
    url                   varchar(150)    not null,
    `delay`               varchar(50) NOT NULL default '00:00:00',
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    primary key(url)
   ) Type=InnoDB;

/*
 * ======================End PA interface tables===============
 */

/*
 * Resource Control tables backend for ResourcMonitor and JobQueue components
 */	
CREATE TABLE rc_site(

 site_index INT(11) NOT NULL AUTO_INCREMENT,
 site_name VARCHAR(255) NOT NULL,
 se_name VARCHAR(255) NOT NULL,
 ce_name VARCHAR(255) NOT NULL,
 is_active ENUM("true", "false") DEFAULT "true",
 PRIMARY KEY(site_index),
 UNIQUE(site_name)
) TYPE = InnoDB;



CREATE TABLE rc_site_threshold(
  site_index INT(11) NOT NULL,
  threshold_name VARCHAR(255) NOT NULL,
  threshold_value INT(11) DEFAULT 0,
  UNIQUE (threshold_name, site_index),
  FOREIGN KEY (site_index) REFERENCES rc_site(site_index)
     ON DELETE CASCADE
) TYPE = InnoDB;


CREATE TABLE rc_site_attr(
 site_index INT(11) NOT NULL,
  attr_name VARCHAR(255) NOT NULL,
  attr_value VARCHAR(255) DEFAULT "",
  UNIQUE (attr_name, site_index),
  FOREIGN KEY (site_index) REFERENCES rc_site(site_index)
    ON DELETE CASCADE
) TYPE = InnoDB;




/*
 * ======================Start JobQueue tables===============
 */

CREATE TABLE jq_queue(
   job_index INT NOT NULL AUTO_INCREMENT,
   job_spec_id VARCHAR(255) NOT NULL,
   job_spec_file TEXT NOT NULL,
   job_type VARCHAR(255) NOT NULL,
   workflow_id VARCHAR(255) NOT NULL, 
   priority INT DEFAULT 0,
   workflow_priority INT DEFAULT 0,
   status ENUM ("new", "held", "released") DEFAULT "new",
   time TIMESTAMP NOT NULL default NOW(),
   released_site INT DEFAULT NULL,
   UNIQUE(job_spec_id),
   PRIMARY KEY(job_index),
   FOREIGN KEY (released_site) REFERENCES rc_site(site_index)
     ON DELETE CASCADE
) TYPE=InnoDB;


CREATE TABLE jq_site(
   job_index INT NOT NULL,
   site_index INT,
   FOREIGN KEY (job_index) REFERENCES jq_queue(job_index)
     ON DELETE CASCADE,
   FOREIGN KEY (site_index) REFERENCES rc_site(site_index)
) TYPE=InnoDB;


CREATE INDEX jq_job_index USING BTREE ON jq_queue (job_index);
CREATE INDEX jq_workflow_index USING BTREE ON jq_queue (workflow_id);


/*
 * ======================End JobQueue tables===============
 */

/*
 * ======================Start CondorTracker tables===============
 */

CREATE TABLE ct_job(
  job_index INT NOT NULL AUTO_INCREMENT,
  job_spec_id VARCHAR(255) NOT NULL,
  job_state ENUM("submitted", "running", "complete", "failed") DEFAULT "submitted",
  job_killed ENUM("true", "false") DEFAULT "false",
  time timestamp NOT NULL default CURRENT_TIMESTAMP
     ON UPDATE CURRENT_TIMESTAMP,	
  UNIQUE (job_spec_id),
  PRIMARY KEY(job_index),
  INDEX(job_spec_id)
  
) TYPE=InnoDB;


CREATE TABLE ct_job_attr(
   job_index INT,
   attr_index INT NOT NULL AUTO_INCREMENT,
   attr_name VARCHAR(255),
   attr_value BLOB,
   PRIMARY KEY (attr_index),
   FOREIGN KEY (job_index) REFERENCES ct_job(job_index)
    ON DELETE CASCADE

) TYPE=InnoDB;

/*  
 * WorkflowEntities
 */

/*
CREATE TABLE we_Job_State(
   status               varchar(40),
   primary key(status)
   ) TYPE=InnoDB;

INSERT INTO we_Job_State(status) VALUES('register') ON DUPLICATE KEY UPDATE status='register';
INSERT INTO we_Job_State(status) VALUES('released') ON DUPLICATE KEY UPDATE status='released';
INSERT INTO we_Job_State(status) VALUES('create') ON DUPLICATE KEY UPDATE status='create';
INSERT INTO we_Job_State(status) VALUES('submit') ON DUPLICATE KEY UPDATE status='submit';
INSERT INTO we_Job_State(status) VALUES('inProgress') ON DUPLICATE KEY UPDATE status='inProgress';
INSERT INTO we_Job_State(status) VALUES('finished') ON DUPLICATE KEY UPDATE status='finished';
INSERT INTO we_Job_State(status) VALUES('reallyFinished') ON DUPLICATE KEY UPDATE status='reallyFinished';
INSERT INTO we_Job_State(status) VALUES('failed') ON DUPLICATE KEY UPDATE status='failed';
*/

CREATE TABLE we_Job(
   allocation_id        varchar(255),
   bulk_id              varchar(255),
   cache_dir            varchar(255),
   events_processed     int             default 0,
   events_allocated     int             default 0,
   id                   varchar(255)    not null,
   job_spec_file        text,
   job_type             varchar(150)    not null,
   max_retries          int             default 1,
   max_racers           int             default 1,
   owner                varchar(150)    default 'no owner',
   retries              int             default 0,
   racers               int             default 0,
/*   status               varchar(40), */
   status   enum("register","released","create","submit","inProgress","finished","reallyFinished","failed") default 'register',
/*   CONSTRAINT `we_Job1` FOREIGN KEY(status) REFERENCES we_Job_State(status), */
   Time timestamp                       default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   workflow_id          varchar(150),
   index(workflow_id),
   primary key(id),
   index(allocation_id)
   ) TYPE=InnoDB;

CREATE TABLE we_File(
   events_processed      int             default 0,
   id                    varchar(255),
   job_id                varchar(255),
   index(job_id),
   CONSTRAINT `we_file1` FOREIGN KEY(job_id) 
       REFERENCES we_Job(id) 
       ON DELETE CASCADE,
   primary key(id)
   ) TYPE=InnoDB;


CREATE TABLE we_Allocation
   (
    allocation_spec_file varchar(255),
    id                   varchar(255)    not null,
    events_allocated     int             default 0,
    events_missed        int             default 0,
    events_missed_cumul  int             default 0,
    events_processed     int             default 0,
    details              mediumtext,
    prod_mgr_url         varchar(255)    not null,
    workflow_id          varchar(255)    not null,
    primary key(id),
    index(workflow_id)
   ) Type=InnoDB;

CREATE TABLE we_Workflow
   (
    events_processed      int             default 0,
    done                  enum("true","false") default 'false',
    id                    varchar(255)    not null,
    owner                 varchar(150)    default 'no owner',
    priority              int(11)         not null,
    prod_mgr_url          varchar(255)    not null,
    run_number_count      int(11)         not null,
    workflow_spec_file    text            not null,
    max_sites             int              default NULL,
    workflow_type          enum("event", "file") default 'event',
    primary key(id),
    index(priority)
   ) Type=InnoDB;

/*
 * Link Workflows to associated sites in rc_site table	
 *	
 */
CREATE TABLE we_workflow_site_assoc (
   workflow_id  varchar(255) not null,
   site_index INT,
   FOREIGN KEY (workflow_id) REFERENCES we_Workflow(id)
     ON DELETE CASCADE,
   FOREIGN KEY (site_index) REFERENCES rc_site(site_index),
   UNIQUE (workflow_id, site_index)

) Type=InnoDB;


/*
 ************************Trigger****************************
 *
 *
 *  Action
 *   | 1
 *   |
 *   | *   *      1 
 * tr_Trigger---------we_Job
 *               
 */

CREATE TABLE tr_Trigger(
   JobSpecID VARCHAR(255) NOT NULL,
   TriggerID VARCHAR(255) NOT NULL,
   FlagID VARCHAR(255) NOT NULL,
   FlagValue ENUM("null","start","finished") NOT NULL,
   Time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
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



/*
 *	ProdMon tables
 */
 
CREATE TABLE prodmon_Resource (
       resource_id INT NOT NULL AUTO_INCREMENT,
       rc_site_id INT DEFAULT NULL,
       site_name VARCHAR(255) NOT NULL,
       ce_hostname VARCHAR(255),
       se_hostname VARCHAR(255),
       PRIMARY KEY (resource_id),
       FOREIGN KEY (rc_site_id) REFERENCES rc_site(site_index)
       	ON DELETE CASCADE
) TYPE = InnoDB;

CREATE TABLE prodmon_Workflow (
       workflow_id INT NOT NULL AUTO_INCREMENT,
       workflow_name VARCHAR(255) NOT NULL,
       request_id INTEGER,
       app_version VARCHAR(255),
       PRIMARY KEY (workflow_id)
)TYPE=InnoDB;

CREATE TABLE prodmon_Job_errors (
       error_id INT NOT NULL AUTO_INCREMENT,
       error_type VARCHAR(255) NOT NULL,
       UNIQUE(error_type),
       PRIMARY KEY (error_id)
)TYPE=InnoDB;

CREATE TABLE prodmon_Datasets (
       dataset_id INT NOT NULL AUTO_INCREMENT,
       dataset_name VARCHAR(255) NOT NULL,
       UNIQUE(dataset_name),
       PRIMARY KEY (dataset_id)
)TYPE=InnoDB;

CREATE TABLE prodmon_LFN (
       file_id INT NOT NULL AUTO_INCREMENT,
       file_name VARCHAR(255) NOT NULL,
       UNIQUE(file_name),
       PRIMARY KEY (file_id)
)TYPE=InnoDB;

CREATE TABLE prodmon_Job (
       job_id INT NOT NULL AUTO_INCREMENT,
       workflow_id INT NOT NULL,
       job_spec_id VARCHAR(255) NOT NULL,
       type VARCHAR(255) NOT NULL,
       PRIMARY KEY (job_id),
       INDEX (workflow_id, job_spec_id),
       FOREIGN KEY (workflow_id) REFERENCES prodmon_Workflow(workflow_id)
       	ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_Job_instance (
       instance_id INT NOT NULL AUTO_INCREMENT,
       job_id INT NOT NULL,
       resource_id INT NOT NULL,
       dashboard_id VARCHAR(255),
       worker_node VARCHAR(255) NOT NULL,
       exit_code INT,
       error_id INT,
       error_message BLOB,
       start_time INT,
       end_time INT,
       exported BOOLEAN DEFAULT FALSE NOT NULL,
       insert_time TIMESTAMP NOT NULL default CURRENT_TIMESTAMP,
       PRIMARY KEY (instance_id),
       INDEX (job_id, resource_id, error_id, exported, insert_time),
       FOREIGN KEY (job_id) REFERENCES prodmon_Job (job_id)
       	ON DELETE CASCADE,
       FOREIGN KEY (resource_id) REFERENCES prodmon_Resource (resource_id)
       	ON DELETE CASCADE,
       FOREIGN KEY (error_id) REFERENCES prodmon_Job_errors (error_id)
       	ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_Job_step (
       step_id INT NOT NULL AUTO_INCREMENT,
       instance_id INT NOT NULL,
       exit_code INT,
       evts_read INT NOT NULL,
       evts_written INT NOT NULL,
       error_id INT,
       error_message BLOB,
       start_time INT,
       end_time INT,
       PRIMARY KEY (step_id),
       INDEX (step_id, instance_id, error_id),
       FOREIGN KEY (instance_id) REFERENCES prodmon_Job_instance (instance_id)
       	ON DELETE CASCADE,
       FOREIGN KEY (error_id) REFERENCES prodmon_Job_errors (error_id)
       	ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_input_datasets_map (
       workflow_id INT NOT NULL,
       dataset_id INT NOT NULL,
       INDEX (workflow_id, dataset_id),
       FOREIGN KEY (workflow_id) REFERENCES prodmon_Workflow (workflow_id)
       	ON DELETE CASCADE,
       FOREIGN KEY (dataset_id) REFERENCES prodmon_Datasets (dataset_id)
       	ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_output_datasets_map (
       workflow_id INT NOT NULL,
       dataset_id INT NOT NULL,
       INDEX (workflow_id, dataset_id),
       FOREIGN KEY (workflow_id) REFERENCES prodmon_Workflow (workflow_id)
       	ON DELETE CASCADE,
       FOREIGN KEY (dataset_id) REFERENCES prodmon_Datasets (dataset_id)
        ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_input_LFN_map (
       step_id INT NOT NULL,
       file_id INT NOT NULL,
       INDEX (step_id, file_id),
       FOREIGN KEY (step_id) REFERENCES prodmon_Job_step (step_id)
       	ON DELETE CASCADE,
       FOREIGN KEY (file_id) REFERENCES prodmon_LFN (file_id)
        ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_output_LFN_map (
       step_id INT NOT NULL,
       file_id INT NOT NULL,
       INDEX (step_id, file_id),
       FOREIGN KEY (step_id) REFERENCES prodmon_Job_step (step_id)
        ON DELETE CASCADE,
       FOREIGN KEY (file_id) REFERENCES prodmon_LFN (file_id)
        ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_Job_timing (
       timing_id INT NOT NULL AUTO_INCREMENT,
       step_id INT NOT NULL,
       timing_type VARCHAR(255) NOT NULL,
       value INT NOT NULL,
       PRIMARY KEY (timing_id),
       INDEX (step_id),
       FOREIGN KEY (step_id) REFERENCES prodmon_Job_step (step_id)
        ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_node_properties (
       node_id INT NOT NULL AUTO_INCREMENT,
       cpu_speed DOUBLE NOT NULL,
       cpu_description VARCHAR(255),
       number_cpu INT NOT NULL,
       number_core INT NOT NULL,
       memory DOUBLE NOT NULL,
       PRIMARY KEY (node_id)
)TYPE=InnoDB;

CREATE TABLE prodmon_node_map (
	   instance_id INT NOT NULL,
       node_id INT NOT NULL,
       INDEX (node_id, instance_id),
       FOREIGN KEY (node_id) REFERENCES prodmon_node_properties (node_id)
         ON DELETE CASCADE,
       FOREIGN KEY (instance_id) REFERENCES prodmon_Job_instance (instance_id)
         ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_performance_summary (
       step_id INT NOT NULL,
       metric_class VARCHAR(255) NOT NULL,
       metric_name VARCHAR(255) NOT NULL,
       metric_value VARCHAR(255) NOT NULL,
       INDEX (step_id, metric_class, metric_name),
       FOREIGN KEY (step_id) REFERENCES prodmon_Job_step (step_id)
         ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_performance_modules (
       step_id INT NOT NULL,
       module_name VARCHAR(255) NOT NULL,
       metric_class VARCHAR(255) NOT NULL,
       metric_name VARCHAR(255) NOT NULL,
       metric_value VARCHAR(255) NOT NULL,
       INDEX (step_id, module_name, metric_class, metric_name),
       FOREIGN KEY (step_id) REFERENCES prodmon_Job_step (step_id)
         ON DELETE CASCADE
)TYPE=InnoDB;


CREATE TABLE log_input (
       id INT NOT NULL AUTO_INCREMENT,
       lfn TEXT,
       workflow VARCHAR(255) NOT NULL,
       se_name VARCHAR(255) NOT NULL,
       status enum("new", "inprogress", "done", "failed") default "new",
       insert_time TIMESTAMP NOT NULL default CURRENT_TIMESTAMP,
       INDEX (id, status)
)TYPE=InnoDB;


/*
ALERT DB TABLES
*/

CREATE TABLE alert_current (
       id int(11) not null auto_increment,
       type varchar(30) not null,
       component varchar(30) not null,
       message text not null,
       time timestamp default 0,
       primary key (id));

CREATE TABLE alert_history (
       id int(11) not null auto_increment,
       type varchar(30) not null,
       component varchar(30) not null,
       message text not null,
       generationtime timestamp default 0,
       historytime timestamp default 0,
       primary key (id));



/*
Lumi Info tables
*/
create table merge_input_lumi 

       (run int(11) not null, 
       lumi int(11) not null, 
       file_id int(11) not null, 
       foreign key(file_id) references merge_inputfile(id) on delete cascade)  TYPE = InnoDB default charset=latin1;


create table merge_lumi

       (run int(11) not null,
       lumi int(11) not null,
       file_id int(11) not null,
       foreign key(file_id) references merge_inputfile(id) on delete cascade)  TYPE = InnoDB default charset=latin1;


