/*
 * To use:
 *
 * Start a MySQL interactive session with same account/privs as ProdAgent
 * database administrator settings
 * use ProdAgentDB;
 * source update-prodagentdb-to-version.sql
 *
 */


/*
 * Update to st_job_attr to contain timing details from job reports
 */

/*
 * Update to add timestamps to st_job_failure and st_job_success tables
 * that get filled on insert
 */

/*
 * extension of the message service with delay functionality of messages
 *
 */
ALTER TABLE ms_message ADD delay VARCHAR(50);
ALTER TABLE ms_history ADD delay VARCHAR(50);

/*
 * Update to include job_type in st_job_success and st_job_failure tables
 */

/*
 * Update to fix bug in trigger tables
 * removed the cascade function for the actions.
 */

/*
 * Update to include MergeSensor tables
 */

/*
A merge_dataset table stores information on datasets watched by the
MergeSensor component.

Fields:

 id            dataset internal id
 prim          primary dataset name
 tier          datatier name
 processed     processed dataset name
 pollTier      datatier used for polling DBS in multiple datatier datasets
 psethash      psethash used in newdataset event specification
 status        status (opened or closed)
 started       watched started on
 updated       last information updated
 version       CMSSW version
 workflow      workflow name
 mergedlfnbase LFN base for merge files
 category      category (preproduction, etc)
 timeStamp     workflow time stamp
 sequence      output sequence number
*/

CREATE TABLE merge_dataset
  (
    id int NOT NULL auto_increment,
    prim varchar(255) NOT NULL default '',
    tier varchar(255) NOT NULL default '',
    processed varchar(255) NOT NULL default '',
    polltier varchar(255) NOT NULL default '',
    psethash varchar(255) NOT NULL default '',
    status enum("open", "closed") default 'open',
    started timestamp NOT NULL default '0000-00-00 00:00:00',
    updated timestamp NOT NULL default CURRENT_TIMESTAMP
                      on update CURRENT_TIMESTAMP,
    version varchar(255) NOT NULL default '',
    workflow varchar(255) NOT NULL default '',
    mergedlfnbase varchar(255) NOT NULL default '',
    category varchar(255) NOT NULL default '',
    timestamp varchar(255) NOT NULL default '',
    sequence int NOT NULL default '0',

    primary key(id),
    unique(prim,tier,processed)
  )
 TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
A merge_outputfile table stores information on merged files in currently
watched datasets.

Fields:

 id       internal output file id
 name     file name
 instance creation instance number
 status   status (merged or to be done again)
 dataset  dataset id

*/

CREATE TABLE merge_outputfile
  (
    id int NOT NULL auto_increment,
    name text NOT NULL default '',
    instance int NOT NULL default '1',
    status enum("merged", "do_it_again") default "merged",
    dataset int NOT NULL default '0',

    PRIMARY KEY(id),

    FOREIGN KEY(dataset) references merge_dataset(id) ON DELETE CASCADE
  )
 TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
A merge_fileblock table stores information on file blocks of
currently watched datasets.

Fields:

  id   internal file block id
  name block name
*/

CREATE TABLE merge_fileblock
  (
    id int NOT NULL auto_increment,
    name varchar(255) NOT NULL default '',

    PRIMARY KEY(id)
  )
  TYPE = InnoDB DEFAULT CHARSET=latin1;

/*
A merge_inputfile table stores information on input files in currently
watched datasets.

Fields:

 id         internal input file id
 name       file name
 block      block name as returned by DBS
 status     status (merged or blocked)
 dataset    dataset id
 mergedfile associated output merged file id

*/

CREATE TABLE merge_inputfile
  (
    id int NOT NULL auto_increment,
    name text NOT NULL default '',
    block int NOT NULL default '0',
    status enum("unmerged", "merged") default "unmerged",
    dataset int NOT NULL default '0',
    mergedfile int default NULL,
    filesize int NOT NULL default '0',

    PRIMARY KEY(id),

    FOREIGN KEY(dataset) references merge_dataset(id) ON DELETE CASCADE,
    FOREIGN KEY(mergedfile) references merge_outputfile(id) ON DELETE CASCADE,
    FOREIGN KEY(block) references merge_fileblock(id) ON DELETE CASCADE
  )
 TYPE = InnoDB DEFAULT CHARSET=latin1;


