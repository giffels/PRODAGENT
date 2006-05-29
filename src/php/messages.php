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

  // print title
  echo "<h2>Messages to be delivered:</h2><br>\n";

  // connect to ProdAgentDB database
  $conn = connect_pa_db();

  // query database to get components
  $query  = "select procid, name from ms_process";
                                                                                
  // get results
  $result = safe_query($query);

  // get list of components
  $component = array(-1 => 'all');
  while($row = mysql_fetch_row($result)) {
    $component[$row[0]] = $row[1];
  }

  // query database to get message types
  $query  = "SELECT typeid, name FROM ms_type";
                                                                                
  // get results
  $result = safe_query($query);

  // get list of components
  $message = array(-1 => 'all');
  while($row = mysql_fetch_row($result)) {
    $message[$row[0]] = $row[1];
  }

  // information submitted?
  if (isset($_GET['submit'])) {
    $submit = $_GET['submit'];
    $cmp = $_GET['component'];
    $msg = $_GET['message'];
    $src = $_GET['source'];
  } else {
    $cmp = -1;
    $msg = -1;
    $src = -1;
  }

  // select criteria
  echo '<form action="./messages.php" method="get">';

  print_select('component', 'Select target component: ',
               $component, $cmp);

  print_select('source', 'Select source component: ',
               $component, $src);

  print_select('message', 'Select message type: ',
               $message, $msg);

  echo '<br><input type="submit" name ="submit" value="submit">';

  // close form
  echo '</form>';

  // print messages
  if (isset($submit)) {
              
    // query
    $query  = "select messageid, time, type, source, dest, payload " .
              "  from ms_message" .
              (($cmp != -1 || $src != -1 || $msg != -1) ?
                   " where messageid=messageid " : "") .
              (($cmp == -1) ? "" : " and dest='" . $cmp . "' ") .
              (($src == -1) ? "" : " and source='" . $src . "' ") .
              (($msg == -1) ? "" : " and type='" . $msg . "' ") .
              " order by time";
                               
    // get results
    $result = safe_query($query);

    // print table
    if (mysql_num_rows($result) != 0) {
    
      print_rule();
      echo "<h2>Message list:</h2>";
                                                                                
      start_table();
      table_headers(array("Id", "Time stamp", "Message type", "Source", "Target", "Payload"));
      while($row = mysql_fetch_row($result)) {
        echo "<tr align=left>\n";
        echo "<td> " . $row[0] . "</td>";
        echo "<td> " . $row[1] . "</td>";
        echo "<td> " . $message[$row[2]] . "</td>";
        echo "<td> " . $component[$row[3]] . "</td>";
        echo "<td> " . $component[$row[4]] . "</td>";
        echo "<td> " . $row[5] . "</td>";
        echo "</tr>\n";
      }
      end_table();

      // print remove form
      echo '<form action="./remove.php" method="get">';
      echo '<br><input type="submit" name="remove" value="remove">';
        
      echo '<input type="hidden" name="component" value="' . $cmp . '">';
      echo '<input type="hidden" name="source" value="' . $src . '">';
      echo '<input type="hidden" name="message" value="' . $msg . '">';

      // close form
      echo '</form>';

    } else
      echo "No messages to be delivered match the selected criteria";
  }

  // close connection
  mysql_close($conn);

  // print page footer
  print_rule();
  print_footer();
?>

