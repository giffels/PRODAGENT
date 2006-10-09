


ALTER TABLE di_job_queue ADD status ENUM ("new", "used") DEFAULT "new";

/*
 * ======================Start Robust Service call tables===============
 */

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



