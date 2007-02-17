/*
 * ======================Start JobSpec tables===============
 */

ALTER TABLE js_JobSpec ADD WorkflowID VARCHAR(255) default ' ' AFTER State;

/*
 * ======================Start Merge tables===============
 */

alter table merge_inputfile add eventcount int not null default '0' after name;

/*
 * ======================Start PM interface tables===============
 */
DROP TABLE IF EXISTS pm_state;
CREATE TABLE pm_state
   (
    id                    varchar(150)    not null,
    state                 varchar(150)    not null,
    parameters            mediumtext      not null,
    primary key(id)
   ) Type=InnoDB;

DROP TABLE IF EXISTS pm_request;
CREATE TABLE pm_request
   (
    id                    varchar(150)    not null,
    url                   varchar(150)    not null,
    priority              int(11)         not null,
    request_type          enum("event", "file") default 'event',
    retrieved_workflow    varchar(250)    default 'false',
    done                  enum("true","false") default 'false',
    primary key(id,url)
   ) Type=InnoDB;

DROP TABLE IF EXISTS pm_job;
CREATE TABLE pm_job
   (
    id                    varchar(150)    not null,
    request_id            varchar(150)    not null,
    job_spec_url          varchar(255)    not null,
    job_spec_location     varchar(255)    not null default 'None',
    server_url            varchar(255)    not null,
    downloaded            int(11)         not null default 0,
    job_details           mediumtext      ,
    catagory              varchar(150)    not null,
    primary key(id)
   ) Type=InnoDB;

DROP TABLE IF EXISTS pm_job_cut;
CREATE TABLE pm_job_cut
   (
    id                    varchar(150)    not null,
    job_id                varchar(150)    not null,
    job_cut_spec_location varchar(255)    not null default 'None',
    status                enum("running", "finished") default 'running',
    events_processed      int             default 0             
   ) Type=InnoDB;


DROP TABLE IF EXISTS pm_cooloff;
CREATE TABLE pm_cooloff
   (
    url                   varchar(150)    not null,
    `delay`               varchar(50) NOT NULL default '00:00:00',
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    primary key(url)
   ) Type=InnoDB;


/*
 * Migration tables for jobstate (not being used yet
 */

CREATE TABLE we_Job(
   allocation_id        varchar(150),
   cache_dir            varchar(255),
   events_processed     int             default 0,
   id                   varchar(150)    not null,
   job_spec_file        varchar(150),
   job_type             varchar(150)    not null,
   max_retries          int             default 1,
   max_racers           int             default 1,
   retries              int             default 0,
   racers               int             default 0,
   status enum('register','create','in_progress','finished_processing','finished') default 'register',
   Time timestamp                       default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   workflow_id          varchar(150),
   primary key(id),
   index(allocation_id),
   index(workflow_id)
   ) TYPE=InnoDB;

CREATE TABLE we_File(
   events_processed      int             default 0,
   id                    varchar(255),
   job_id                varchar(255),
   primary key(id)
   ) TYPE=InnoDB;


CREATE TABLE we_Allocation
   (
    id                    varchar(150)    not null,
    events_processed     int             default 0,
    details               mediumtext,
    prod_mgr_url          varchar(255)    not null,
    workflow_id           varchar(150)    not null,
    primary key(id),
    index(workflow_id)
   ) Type=InnoDB;

CREATE TABLE we_Workflow
   (
    events_processed     int             default 0,
    id                    varchar(150)    not null,
    priority              int(11)         not null,
    prod_mgr_url          varchar(150)    not null,
    workflow_spec_file    varchar(255)   default 'not_downloaded',
    workflow_type          enum("event", "file") default 'event',
    primary key(id),
    index(priority)
   ) Type=InnoDB;

