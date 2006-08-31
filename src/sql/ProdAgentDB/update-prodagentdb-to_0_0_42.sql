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
    service_call          varchar(255)   not null,
    server_url            varchar(255)   not null,
    service_parameters    mediumtext     not null,
    call_state            ENUM('call_placed','result_retrieved'),
    log_time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,


    unique(component_id,service_call,server_url)

  ) Type=InnoDB;





