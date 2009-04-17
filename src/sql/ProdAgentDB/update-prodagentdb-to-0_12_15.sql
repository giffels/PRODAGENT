ALTER TABLE rc_site MODIFY ce_name VARCHAR(255);

CREATE TABLE prodmon_Job_step (
       step_id INT NOT NULL AUTO_INCREMENT,
       instance_id INT NOT NULL,
       exit_code INT,
       evts_read INT NOT NULL,
       evts_written INT NOT NULL,
       error_id INT,
       error_message BLOB,
       start_time INT,
       end_time INT,
       PRIMARY KEY (step_id),
       INDEX (step_id, instance_id, error_id),
       FOREIGN KEY (instance_id) REFERENCES prodmon_Job_instance (instance_id)
       	ON DELETE CASCADE,
       FOREIGN KEY (error_id) REFERENCES prodmon_Job_errors (error_id)
       	ON DELETE CASCADE
)TYPE=InnoDB;

DROP TABLE TABLE prodmon_output_runs;
DROP TABLE prodmon_skipped_events

ALTER TABLE prodmon_Job_instance DROP evts_read, DROP evts_written;

ALTER TABLE prodmon_input_LFN_map DROP instance_id, ADD step_id INT NOT NULL;

ALTER TABLE prodmon_output_LFN_map DROP instance_id, ADD step_id INT NOT NULL;

CREATE TABLE prodmon_output_runs ( 	 
	        instance_id INT NOT NULL, 	 
	        run INT NOT NULL, 	 
	        INDEX (instance_id), 	 
	        FOREIGN KEY (instance_id) REFERENCES prodmon_Job_instance (instance_id) 	 
	         ON DELETE CASCADE 	 
	 )TYPE=InnoDB; 	 
	  	 
CREATE TABLE prodmon_skipped_events ( 	 
	        instance_id INT NOT NULL, 	 
	        run INT NOT NULL, 	 
	        event INT NOT NULL, 	 
	        INDEX (instance_id), 	 
	        FOREIGN KEY (instance_id) REFERENCES prodmon_Job_instance (instance_id) 	 
	         ON DELETE CASCADE 	 
	 )TYPE=InnoDB;
	 
ALTER TABLE prodmon_Job_timing DROP instance_id, ADD step_id INT NOT NULL;
ALTER TABLE prodmon_performance_summary DROP instance_id, ADD step_id INT NOT NULL;
ALTER TABLE prodmon_performance_modules DROP instance_id, ADD step_id INT NOT NULL;

ALTER TABLE prodmon_Job_timing DROP run;