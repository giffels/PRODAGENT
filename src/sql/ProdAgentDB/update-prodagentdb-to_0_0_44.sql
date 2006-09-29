


ALTER TABLE di_job_queue ADD status ENUM ("new", "used") DEFAULT "new";
