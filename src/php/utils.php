<?php

  # Utility functions of the the Production Agent web interface
  # 
  # Author: Carlos Kavka <Carlos.Kavka@ts.infn.it>
  # Date: $Date$
  # Revision: $Revision$

  # Define your database parameters as specified in ProdAgentConfig.xml
  #
  # Example:
  #
  # define('dbhost', 'localhost');
  # define('dbsocket', '/data/mysqldata/mysql.sock');
  # define('dbport', '');
  # define('dbuser', 'MyUser');
  # define('dbpass', 'MyPassword');
  # define('dbname', 'ProdAgentDB');

  define('dbhost', 'localhost');
  define('dbsocket', '/data/mysqldata/mysql.sock');
  define('dbport', '');
  define('dbuser', 'MyUser');
  define('dbpass', 'MyPassword');
  define('dbname', 'ProdAgentDB');
  
  # Function: connect_boss_db()
  #
  #   Connect to the BOSS database
  #
  # Return: a database connection
 
  function connect_boss_db() {

    $boss_dbname = dbname . '_BOSS';

    // build database id
    $db = dbhost .
          (dbport == '' ? '' : ':' . dbport) .
          (dbsocket == '' ? '' : ':' . dbsocket);

    // connect to database
    $conn = mysql_connect($db, dbuser, dbpass)
      or die ('Error connecting to mysql server');

    // select database 
    mysql_select_db($boss_dbname)
      or die ('Error selecting database');
 
    return $conn;
  }

  #
  # Function: connect_pa_db()
  #
  #  Connect to the ProdAgentDB database
  #
  # Return: a database connection

  function connect_pa_db() {

    // build database id
    $db = dbhost .
          (dbport == '' ? '' : ':' . dbport) .
          (dbsocket == '' ? '' : ':' . dbsocket);
                                                                                
    // connect to database
    $conn = mysql_connect($db, dbuser, dbpass)
      or die ('Error connecting to mysql server');
      
    // select database                                                                          
    mysql_select_db(dbname)
      or die ('Error selecting database');
                                                                                
    return $conn;
  }
  
  # Function safe_query()
  #
  #   Perform a query displaying error information when there is a problem
  #
  # Arguments:
  #
  #   $query: the string query
  #
  # Return: query results

  function safe_query($query) {

    // perform query
    $result = mysql_query($query)
    or die("query failed: " .
             "<li>error number = " . mysql_errno() .
             "<li>error = " . mysql_error() .
             "<li>query = " . $query);

    // return result
    return $result;
  }

  #
  # Function start_table()
  #
  #   print a standard table header
  #
  # Arguments:
  #
  #   $border = table border size (default: 1)

  function start_table($border = 1) {

    echo '<table border ="' . $border .'">';
  }
 
  #
  # Function end_table()
  #
  #   print a standard table footer
  #
 
  function end_table() {

    echo "</table>\n";
  }
 
  #
  # Function table_headers()
  #
  #   print table headers
  #
  # Arguments:
  #
  #   $header: array of column labels
   
  function table_headers($header) {

    // get number of columns
    $num_cols = count($header);

    // start header row 
    echo "<tr align = center>\n";

    // print all cells
    for ($i = 0; $i < $num_cols; $i++) {
  
      echo "<th>" . $header[$i] . "</th>\n";
    }

    // end header row
    echo "</tr>\n";
  }
 
  #
  # Function table_row()
  #
  #   print table row
  #
  # Arguments:
  #
  #   $row: array of row values
  #   $default: value used to replace NULLs (default = 'n.a.')
  
  function table_row($row,$default = "n.a.") {

    // get number of columns
    $num_cols = count($row);

    // start row
    echo '<tr>';

    // print all cells
    $first = TRUE;
    for ($i = 0; $i < $num_cols; $i++) {

      // check for empty values
      if ($row[$i] == "")
        $data = $default;
      else
        $data = $row[$i];

      // print data cells
      if ($first) { 
        echo '<td align="left">' . $data . "</td>\n";
        $first = FALSE;
      } else
        echo '<td align="right">' . $data . "</td>\n";
    }

    // end row
    echo "</tr>\n";
  }
 
  # Function: print_mysql_table()
  #
  #   Print an html table from a mysql query result
  #
  # Arguments:
  #
  #   $result: query result
  #   $title: array of column labels, if null the first row is used
  #   $align: title row alignment (default right)
   
  function print_mysql_table($result, $title='', $align='right') {

    $num_cols = mysql_num_fields($result);
 
    start_table();
 
    // headers
    echo '<tr align ="'.$align.'">';
    for ($i = 0; $i < $num_cols; $i++) {

      if (is_array($title))
        echo '<th>' . $title[$i] . "</th>\n";
      else
        echo '<th>' .
              mysql_field_name($result, $i) . "</th>\n";
    }
    echo "</tr>\n";
 
    // content
    while($row = mysql_fetch_row($result)) {
  
      echo "<tr align=left>\n";
      for($i = 0; $i < $num_cols; $i++) {
    
        echo "<td>\n";
        if (!isset($row[$i]))
          echo "NULL";
        else
          echo $row[$i];
        echo "</td>\n";
      }
      echo "</tr>\n";
    }

    end_table();
  }

  # Function: print_table()
  #
  #   Print an html table 
  #
  # Arguments: any number of table rows
   
  function print_table() {

    // get number of rows
    $rows = func_get_args();

    // start a table
    start_table();
 
    // print each row
    while (list($index, $row) = each($rows)) {

      echo '<tr>';

      // print each element
      $first = TRUE;
      while (list($col, $data) = each($row)) {
    
        if ($first) {
          echo '<td align="left">' . $data . "</td>\n";
          $first = FALSE;
        } else
          echo '<td align="right">' . $data . "</td>\n";
      }
      echo "</tr>\n";
    }

    // close a table
    end_table();
  }

  #
  # Function print_select()
  #
  #   Generate a select element
  #
  # Arguments:
  #
  #   $name: element name
  #   $text: title
  #   $option: array of (option, value) pairs
  #   $default: default key
                                                              
  function print_select($name, $text, $option, $default) {

    echo $text;
    echo '<select name="'. $name . '">';
    reset($option);
    while(list($key,$value) = each($option)) {
      echo '<option value=' . $key .
           (($key == $default) ? " selected" : "") .
           '>' . $value;
    }
    echo '</select><br>';
  }

  #
  # Function print_text_entry()
  #
  #   Generate a text entry field
  #
  # Arguments:
  #
  #   name: element name
  #   text: title
  #   $default: default value
                                                                                
  function print_text_entry($name, $text, $default) {

    echo $text;
    echo '<input type="text" name ="'. $name . '" value="' . $default .
         '"><br>';

  }

  #
  # Function print_header()
  #
  #   Print the document header

  function print_header() {

    echo '
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
  <html>
    <head>
      <link href="css/default.css" rel="stylesheet" type="text/css">
      <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type">
      <title>
         ProdAgent web interface
      </title>
    </head>
    <body>
      <div id="container">
        <div id="header">
          <div id="branding">
            <div class="logo">
              <img class="logoImage" alt="CMS" src="logocms.png"
                   title="CMS ProdAgent web interface">
            </div>
            <div id="branding-tagline-name">
              ProdAgent web interface<br>
            </div>
          </div>
        </div>
      </div>
      <center>
        Job status: <a href="index.php">Jobs finished</a> |
                    <a href="running.php">Jobs running</a> |
                    <a href="others.php">Others</a>  
                    <br>
        Message service: <a href="components.php">Components</a> |
                         <a href="subscriptions.php">Subscriptions</a> |
                         <a href="messages.php">Messages</a> 
      </center>
      <br>';
  }

  #
  # Function print_footer()
  #
  #   Print the document footer

  function print_footer() {
    echo '
  <br>
  <div id="footer">
    <script type="text/javascript">
      document.write("Page generated on: " + document.lastModified);
    </script>
  </div>
  </body>
  </html>';
  }

  #
  # Function print_rule()
  #
  #   Print a rule

  function print_rule() {

    echo "<hr>\n";
  } 
?>

