

CREATE TABLE ct_job(
  job_index INT NOT NULL AUTO_INCREMENT,
  job_spec_id VARCHAR(255) NOT NULL,
  job_state ENUM("submitted", "running", "killed", "complete", "failed") DEFAULT "submitted",
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
