

CREATE TABLE jq_queue(
   job_index INT NOT NULL AUTO_INCREMENT,
   job_spec_id VARCHAR(255) NOT NULL,
   job_spec_file VARCHAR(255) NOT NULL,
   job_type VARCHAR(255) NOT NULL,
   workflow_id VARCHAR(255) NOT NULL, 
   sites BLOB,
   priority INT DEFAULT 0,
   time TIMESTAMP NOT NULL default NOW(),
   
   UNIQUE(job_spec_id),
   PRIMARY KEY(job_index)

) TYPE=InnoDB;

