ALTER table merge_outputfile ADD lfn text DEFAULT "" AFTER name;

CREATE TABLE prodmon_node_properties (
       node_id INT NOT NULL AUTO_INCREMENT,
       cpu_speed DOUBLE NOT NULL,
       cpu_description VARCHAR(255),
       number_cpu INT NOT NULL,
       number_core INT NOT NULL,
       memory DOUBLE NOT NULL,
       PRIMARY KEY (node_id)
)TYPE=InnoDB;

CREATE TABLE prodmon_node_map (
	   instance_id INT NOT NULL,
       node_id INT NOT NULL,
       INDEX (node_id, instance_id),
       FOREIGN KEY (node_id) REFERENCES prodmon_node_properties (node_id)
         ON DELETE CASCADE,
       FOREIGN KEY (instance_id) REFERENCES prodmon_Job_instance (instance_id)
         ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_performance_summary (
       instance_id INT NOT NULL,
       metric_class VARCHAR(255) NOT NULL,
       metric_name VARCHAR(255) NOT NULL,
       metric_value VARCHAR(255) NOT NULL,
       INDEX (instance_id, metric_class, metric_name),
       FOREIGN KEY (instance_id) REFERENCES prodmon_Job_instance (instance_id)
         ON DELETE CASCADE
)TYPE=InnoDB;

CREATE TABLE prodmon_performance_modules (
       instance_id INT NOT NULL,
       module_name VARCHAR(255) NOT NULL,
       metric_class VARCHAR(255) NOT NULL,
       metric_name VARCHAR(255) NOT NULL,
       metric_value VARCHAR(255) NOT NULL,
       INDEX (instance_id, module_name, metric_class, metric_name),
       FOREIGN KEY (instance_id) REFERENCES prodmon_Job_instance (instance_id)
         ON DELETE CASCADE
)TYPE=InnoDB;
