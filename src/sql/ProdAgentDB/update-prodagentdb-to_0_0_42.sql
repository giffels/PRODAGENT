/*
 * To use:
 *
 * Start a MySQL interactive session with same account/privs as ProdAgent
 * database administrator settings
 * use ProdAgentDB;
 * source update-prodagentdb-to-version.sql
 *
 */


/*
 * Introducing a new table for robust service interaction
 */

CREATE TABLE ws_last_call
  (
    component_id          varchar(150)    not null,
    tag                   varchar(150)   not null default '0',
    service_call          varchar(255)   not null,
    server_url            varchar(255)   not null,
    service_parameters    mediumtext     not null,
    call_state            ENUM('call_placed','result_retrieved'),
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,


    unique(component_id,service_call,server_url)

  ) Type=InnoDB;

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


/* Added field mergejob to table merge_outputfile */

ALTER TABLE merge_outputfile ADD mergejob varchar(255) NOT NULL default '' AFTER status;

