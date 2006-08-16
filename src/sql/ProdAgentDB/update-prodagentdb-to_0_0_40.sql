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

/*
 * Update to add timestamps to st_job_failure and st_job_success tables
 * that get filled on insert
 */

/*
 * extension of the message service with delay functionality of messages
 *
 */
ALTER TABLE ms_message ADD delay VARCHAR(50);
ALTER TABLE ms_history ADD delay VARCHAR(50);

/*
 * Update to include job_type in st_job_success and st_job_failure tables
 */

/*
 * Update to fix bug in trigger tables
 * removed the cascade function for the actions.
 */

