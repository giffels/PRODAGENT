<?php
                                                                                
  # Message interaction page of the Production Agent web interface
  #
  # Author: Carlos Kavka <Carlos.Kavka@ts.infn.it>
  # Date: $Date$
  # Revision: $Revision$
                                                                                
  // include utility functions
  include 'utils.php';
  include 'boss.php';
                                                                                
  // print page header
  print_header();

  // print components
  echo "<h2>Messages removal</h2><br>\n";

  // connect to ProdAgentDB database
  $conn = connect_pa_db();

  // information submitted?
  if (isset($_GET['remove'])) {
    $submit = $_GET['remove'];
    $cmp = $_GET['component'];
    $msg = $_GET['message'];
    $src = $_GET['source'];

    // query
    $query  = "delete from ms_message" .
              (($cmp != -1 || $src != -1 || $msg != -1) ?
                   " where messageid=messageid " : "") .
              (($cmp == -1) ? "" : " and dest='" . $cmp . "' ") .
              (($src == -1) ? "" : " and source='" . $src . "' ") .
              (($msg == -1) ? "" : " and type='" . $msg . "' ") .
              " order by time";
                               
    // get results
    $result = safe_query($query);

    // check number of messages removed
    $affected_rows = mysql_affected_rows();

    if ($affected_rows > 0) {
      echo '<p>A total of ' . $affected_rows .
           ' message' . ($affected_rows == 1 ? " was" : "s were") .
           ' deleted.</p>';
    } else {
      echo 'No messages were selected!\n';
    }

  }

  // close connection
  mysql_close($conn);

  // print page footer
  print_rule();
  print_footer();
?>

