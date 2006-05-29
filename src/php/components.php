<?php
                                                                                
  # Components listing page of the Production Agent web interface
  #
  # Author: Carlos Kavka <Carlos.Kavka@ts.infn.it>
  # Date: 16 May 2006
  # Date: $Date$
  # Revision: $Revision$

  // include utility functions
  include 'utils.php';
  include 'boss.php';
                                                                                
  // print page header
  print_header();

  // print components
  echo "<h2>Components registered in the message service:</h2><br>\n";

  // connect to ProdAgentDB database
  $conn = connect_pa_db();

  // query for components information
  $query  = "select name, host, pid from ms_process where pid != 0";

  // get results
  $result = safe_query($query);

  // print table
  if (mysql_num_rows($result) != 0)
    print_mysql_table($result, array("Component name", "Host", "PID"), "center");
  else
    print "No components registered\n";

  // close connection
  mysql_close($conn);

  // print page footer
  print_rule();
  print_footer();
?>

