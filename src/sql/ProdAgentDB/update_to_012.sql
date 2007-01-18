alter table st_job_fail_attr MODIFY attr_class ENUM("timing" ,"run_numbers", "input_files");

alter table merge_outputfile add key(mergejob);

alter table pm_job add primary key(id);
