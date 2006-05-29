<?php
                                                                                
  # Subscriptions listing page of the Production Agent web interface
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
  echo "<h2>Component subscriptions registered in the message service:</h2><br>\n";

  // connect to ProdAgentDB database
  $conn = connect_pa_db();

  // query for components information
  $query  = "select ms_process.name, ms_type.name " .
            "from ms_process, ms_type, ms_subscription " .
            "where ms_process.procid=ms_subscription.procid and " .
                  "ms_type.typeid=ms_subscription.typeid " .
            " order by ms_process.name";

  // get results
  $result = safe_query($query);

  // print table
  if (mysql_num_rows($result) != 0)
    print_mysql_table($result, array("Component name", "Message"), 'center');
  else
    print "No subscriptions registered\n";

  // close connection
  mysql_close($conn);

  // print page footer
  print_rule();
  print_footer();
?>

