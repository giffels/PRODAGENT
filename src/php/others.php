<?php

  # Non running/non finished jobs page of the the Production Agent web interface
  #
  # Author: Carlos Kavka <Carlos.Kavka@ts.infn.it>
  # Date: $Date$
  # Revision: $Revision$

  // include utility functions 
  include 'utils.php';
  include 'boss.php';

  // print page header
  print_header();

  // connect to BOSS database
  $conn = connect_boss_db();

  // query for task information
  $query = "select JOB.TASK_ID as id, JOB.SUB_TIME as subTime, " .
    "       JOB. START_T as startTime, JOB.STOP_T as stopTime, " . 
    "       JOB.GETOUT_T as getOutTime, JOB.LAST_T as lastTime, " .
    "       cmssw.TASK_ID " .
    "       from JOB,cmssw where JOB.TASK_ID=cmssw.TASK_ID " .
    "       order by TASK_ID;";

  // get results
  $result = safe_query($query);

  // job state counters
  $running = 0;
  $finished = 0;
  $other = 0;

  // get number of submitted jobs
  $submitted = mysql_num_rows($result);

  // process all rows
  while ($row = mysql_fetch_array($result, MYSQL_ASSOC)) {

    // get status
    $status = boss_status($row); 

    // update summary info
    switch($status) {
      case 'R':
        $running++;
        break;
      case 'E':
        $finished++;
        break;
      default:
        $other++;
    }
  }

  // print summary
  echo "<h2>Run status summary:</h2><br>";

  print_table(array('Status', 'Number of jobs'),
              array('Running', $running),
              array('Finished', $finished),
              array('Other', $other));

  print_rule();

  // get schedulers information
  $query = "select ID as schedId from SCHEDULERS;";
  $result = safe_query($query);

  $schedulers = array('all');
  while ($row = mysql_fetch_array($result, MYSQL_ASSOC)) {
    array_push($schedulers, $row['schedId']);
  }

  // check for cmssw table
  $result = safe_query("show tables");
  $cmssw = FALSE;
  while ($row = mysql_fetch_row($result)) {
    if ($row[0] == "cmssw") {
      $cmssw = TRUE;
      break;
    }
  }

  // get request from user
  echo "<h2>Select non running/non finished jobs:</h2><br>";

  // use user selected parameters ...
  if (isset($_GET['submit'])) {
    $started = $_GET['started_input'];
    switch ($started) {
      case 1:
        $hours = 1;
        break;
      case 2:
        $hours = 2;
        break;
      case 3:
        $hours = 24;
        break;
      default:
        $hours = 0;
    }
    $scheduler = $_GET['scheduler_input'];
    $dataset = $_GET['dataset_input'];

  // ... or default ones
  } else {
    $started = 'anytime';
    $hours = 0;
    $scheduler = 'all';
    $dataset = '';
  }

  // start form
  echo '<form action="./others.php" method="get">';

  // select started time
  print_select('started_input',
       'Started on: ',
       array('anytime','last 1 hour','last 2 hours','last 24 hours'),
       $started);

  // select scheduler
  print_select('scheduler_input',
       'Scheduler: ',
       $schedulers,
       $scheduler);

  // select dataset
  print_text_entry('dataset_input',
           'Dataset name: ',
           $dataset);
                                                                                
  // submit button
  echo '<br><input type="submit" name ="submit" value="submit">';

  // close form
  echo '</form>';

  // print job information
  if (isset($_GET['submit'])) {

    if ($cmssw) {

      print_rule();

      echo "<h2>Non running/non finished jobs information:</h2><br>";


      // query for task information
      $query = "select JOB.TASK_ID as id, JOB.SUB_TIME as subTime, " .
         "       JOB.START_T as startTime, JOB.STOP_T as stopTime, " .
         "       JOB.GETOUT_T as getOutTime, JOB.LAST_T as lastTime, " .
         "       JOB.SCHEDULER as scheduler, cmssw.CMSSW_VERSION as version, " .
         "       cmssw.PRIMDATASET as prim_ds, cmssw.PROCDATASET as proc_ds, " .
         "       cmssw.TASK_EXIT as task_exit " .
         "from JOB, cmssw where JOB.TASK_ID=cmssw.TASK_ID" .
         (($scheduler == 0) ? "" : " and JOB.SCHEDULER='" .
                                 $schedulers[$scheduler] . "' ") .
         (($started == 0) ? "" : " and START_T > (UNIX_TIMESTAMP() - " .
                                 (3600 * $hours) . ")") . " " .
         (($dataset == 0) ? "" : " and JOB.PRIMDATASET='" . $dataset . '" ') .
         " order by JOB.TASK_ID;";

      // get results
      $result = safe_query($query);

      // jobs counter
      $num_jobs = 0;

      $headers_printed = 0;

      // process all rows
      while ($row = mysql_fetch_array($result, MYSQL_ASSOC)) {
									
        // get status
        $status = boss_status($row);

        // ignore non running/finished tasks
        if ($status == 'R' || $status == 'E')
          continue;

        switch ($status) {
          case 'I':
            $status = "submitted, not yet started";
            break;
          case 'W':
            $status = "not yet submitted";
            break;
          case 'K':
            $status = "killed";
            break;
          case 'OR':
            $status = "output available";
            break;
          case 'I?':
            $status = "scheduled";
            break;
          default:
            $status = "unknown";
        }

        // count it
        $num_jobs++;

        // print headers only if data is available
        if ($headers_printed == 0) {

          $headers_printed = 1;
          echo "<p>Click on the job id to get detailed information.</p>\n";

          // print table header
          start_table();
          table_headers(array('Job id','Status','Submission date','Start time',
                        'Scheduler','Primary dataset','Processed dataset'));
        }

        // print data
        table_row(array('<a href="task.php?id='.$row['id'].'">'.$row['id'].'</a>',
              $status,
	      ($row['subTime'] == 0 ? "n.a." : date("d-m-Y H:i",$row['subTime'])), 
	      ($row['startTime'] == 0 ? "n.a." : date("H:i",$row['startTime'])),
	      $row['scheduler'],
	      $row['prim_ds'],
              $row['proc_ds']));

      }

      // close table
      end_table();

    } else {
                                                                                
      echo '<p>Error: job type CMSSW is not register in database!</p><br>';
      echo '<p>Please register it by following instructions in: ';
      echo '<a href="https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgentDB">';
      echo 'ProdAgentDB</a> twiki page.</p>';
    }

    // message for no jobs
    if ($num_jobs == 0)
      echo "<p>No job matches the selected criteria</p>";

  }

  // close connection
  mysql_close($conn);

  // print page footer
  print_rule();
  print_footer();
?>

