
ALTER TABLE merge_dataset
      ADD secondarytiers varchar(255) NOT NULL default ''
      AFTER polltier; 

