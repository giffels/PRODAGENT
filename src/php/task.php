<?php

  # Jobs page of the the Production Agent web interface
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

  // print title
  echo "<h2>Job information:</h2><br>";

  if (isset($_GET['id'])) {

    $task_id = $_GET['id'];

    // query for task information
    $query = "select JOB.TASK_ID as id, JOB.SUB_TIME as subTime, " .
         "       JOB.START_T as startTime, JOB.STOP_T as stopTime, " .
         "       JOB.EXEC_HOST as host, JOB.SCHED_ID as schedId, " .
         "       JOB.GETOUT_T as getOutTime, JOB.LAST_T as lastTime, " .
         "       JOB.SCHEDULER as scheduler, cmssw.CMSSW_VERSION as version, " .
         "       cmssw.PRIMDATASET as prim_ds, cmssw.PROCDATASET as proc_ds, " .
         "       cmssw.TASK_EXIT as taskExit, cmssw.N_EVT as evt, " .
         "       cmssw.N_RUN as run, cmssw.SE_IN as seIn, cmssw.SE_OUT as seOut, " .
         "       cmssw.SE_PATH as sePath, cmssw.SE_SIZE as seSize, " .
         "       cmssw.stageOut_START as startSO, cmssw.stageOut_STOP as stopSO, " .
         "       cmssw.stageOut_EXIT as exitSO " .
         "from JOB, cmssw where JOB.TASK_ID=cmssw.TASK_ID and JOB.TASK_ID=" .
         $task_id . ";";  

    // get results
    $result = safe_query($query);

    // process row
    $row = mysql_fetch_array($result, MYSQL_ASSOC);

    // print information
    start_table(0);

    // general information
    table_row(array('Job ID:', $row['id']));
    table_row(array('  ', '  '),' ');
    table_row(array('  ', '  '),' ');

    // CMSSW information
    table_row(array("<b>CMSSW information</b>", ''),' ');
    table_row(array('  ', '  '),' ');

    table_row(array('CMSSW version: ', $row['version']));
    table_row(array('Primary dataset: ', $row['prim_ds']));
    table_row(array('Processed dataset: ', $row['proc_ds']));
    table_row(array('Number of events: ', $row['evt']));
    table_row(array('Run number: ', $row['run']));
    table_row(array('Exit code: ', $row['taskExit']));
    table_row(array('  ', '  '),' ');
    table_row(array('  ', '  '),' ');

    // execution information
    table_row(array("<b>Execution information</b>", ''),' ');
    table_row(array('  ', '  '),' ');

    table_row(array('Host: ', $row['host']));
    table_row(array('Scheduler ID: ',
      ($row['schedId'] == ' ' ? 'n.a.' : $row['schedId'])));
    table_row(array('Submission time: ',
      ($row['subTime'] == 0 ? "n.a." : date("d-m-Y H:i",$row['subTime'])))); 
    table_row(array('Start time: ',
      ($row['startTime'] == 0 ? "n.a." : date("d-m-Y H:i",$row['startTime'])))); 
    table_row(array('Stop time: ',
      ($row['stopTime'] == 0 ? "n.a." : date("d-m-Y H:i",$row['stopTime'])))); 
    table_row(array('Get output time: ',
      ($row['getOutTime'] == 0 ? "n.a." : date("d-m-Y H:i",$row['getOutTime'])))); 
    table_row(array('  ', '  '),' ');
    table_row(array('  ', '  '),' ');

    // Storage element information
    table_row(array("<b>Storage element information</b>", ''),' ');
    table_row(array('  ', '  '),' ');

    table_row(array('Input: ', $row['seIn']));
    table_row(array('Output: ', $row['seOut']));
    table_row(array('Path: ', $row['sePath']));
    table_row(array('Size: ', $row['seSize']));
    table_row(array('  ', '  '),' ');

    // stage out
    table_row(array("<b>Stage out information</b>", ''),' ');
    table_row(array('  ', '  '),' ');

    table_row(array('Start time: ', 
      ($row['startSO'] == 0 ? "n.a." : date("d-m-Y H:i",$row['startSO'])))); 
    table_row(array('Stop time: ', 
      ($row['stopSO'] == 0 ? "n.a." : date("d-m-Y H:i",$row['stopSO'])))); 
    table_row(array('Exit code: ', $row['exitSO']));

    end_table();
  }
  
  // close database
  mysql_close($conn);

  // print page footer
  print_rule();
  print_footer();
?>

