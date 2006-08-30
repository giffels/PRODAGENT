

CREATE TABLE di_job_owner(
  owner_name VARCHAR(255),
  owner_index INT NOT NULL AUTO_INCREMENT,
  PRIMARY KEY(owner_index),
  UNIQUE( owner_name)
) TYPE=InnoDB;


CREATE TABLE di_job_queue(
  job_id INT NOT NULL AUTO_INCREMENT,
  owner_index INT NOT NULL,
  time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP 
     ON UPDATE CURRENT_TIMESTAMP,

  
  fileblock VARCHAR(255),
  se_names BLOB,
  lfns BLOB,
  max_events INT DEFAULT NULL,
  skip_events INT DEFAULT NULL,
    
  
  PRIMARY KEY (job_id),
  FOREIGN KEY (owner_index)
     REFERENCES di_job_owner(owner_index)
       ON DELETE CASCADE

) TYPE=InnoDB;


CREATE INDEX di_job_index USING BTREE ON di_job_queue (owner_index);
