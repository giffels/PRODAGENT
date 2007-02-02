/*
 * ======================Start JobSpec tables===============
 */

ALTER TABLE js_JobSpec ADD WorkflowID VARCHAR(255) NOT NULL AFTER State;

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

