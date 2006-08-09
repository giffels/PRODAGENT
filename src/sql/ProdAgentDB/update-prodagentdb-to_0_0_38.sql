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
 * Update to st_job_attr to contain timing details from job reports
 */
ALTER TABLE st_job_attr ADD attr_name VARCHAR(255);
ALTER TABLE st_job_attr MODIFY attr_class enum('run_numbers','output_files','output_datasets','input_files', 'timing');


/*
 * Update to add timestamps to st_job_failure and st_job_success tables
 * that get filled on insert
 */
ALTER TABLE st_job_success ADD time TIMESTAMP NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP;
ALTER TABLE st_job_failure ADD time TIMESTAMP NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP;

/*
 * Update to include job_type in st_job_success and st_job_failure tables
 */
ALTER TABLE st_job_failure ADD job_type VARCHAR(255);
ALTER TABLE st_job_success ADD job_type VARCHAR(255);

/*
 * Update to fix bug in trigger tables
 * removed the cascade function for the actions.
 */
ALTER TABLE tr_Action DROP FOREIGN KEY 0_3;

