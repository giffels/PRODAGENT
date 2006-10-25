

ALTER TABLE merge_outputfile
      ADD failures int NOT NULL default '0'
      AFTER dataset;
ALTER TABLE merge_inputfile
      ADD failures int NOT NULL default '0'
      AFTER filesize;
ALTER TABLE merge_inputfile
      ADD instance int NOT NULL default '0'
      AFTER failures;
ALTER TABLE merge_inputfile
      CHANGE status status enum("unmerged", "undermerge", "merged", "invalid")
             default "unmerged";
ALTER TABLE merge_outputfile
      CHANGE status status enum("merged", "do_it_again", "failed", "undermerge")
      default "undermerge";

