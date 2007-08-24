ALTER TABLE prodmon_Job_instance MODIFY insert_time TIMESTAMP NOT NULL default CURRENT_TIMESTAMP;


/*
 * Changes to add max_sites setting per workflow and associate a workflow
 * with sites
 */
ALTER TABLE we_Workflow ADD max_sites  int default NULL;

CREATE TABLE we_workflow_site_assoc (
   workflow_id  varchar(255) not null,
   site_index INT,
   FOREIGN KEY (workflow_id) REFERENCES we_Workflow(id)
     ON DELETE CASCADE,
   FOREIGN KEY (site_index) REFERENCES rc_site(site_index)

) Type=InnoDB;
