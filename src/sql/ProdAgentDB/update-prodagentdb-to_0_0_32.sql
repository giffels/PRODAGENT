/*
 * To use:  
 *
 * Start a MySQL interactive session with same account/privs as ProdAgent 
 * database administrator settings
 * use ProdAgentDB;
 * source update-stat-tracker.sql
 *      
 */


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
