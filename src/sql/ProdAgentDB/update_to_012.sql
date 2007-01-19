alter table st_job_fail_attr MODIFY attr_class ENUM("timing" ,"run_numbers", "input_files");

alter table merge_outputfile add key(mergejob);

alter table pm_job add primary key(id);


CREATE TABLE merge_workflow
  (
    id int NOT NULL auto_increment,
    name varchar(255) NOT NULL default '',
    dataset int NOT NULL default '0',
    PRIMARY KEY(id),
    INDEX name(name),
    FOREIGN KEY(dataset) references merge_dataset(id) ON DELETE CASCADE
  )
  TYPE = InnoDB DEFAULT CHARSET=latin1;

