<?php

  # Boss utility functions of the the Production Agent web interface
  # 
  # Author: Carlos Kavka <Carlos.Kavka@ts.infn.it>
  # Date: $Date$
  # Revision: $Revision$

  # Function: boss_status()
  #
  #   return BOSS status
  #
  # Arguments:
  #
  #   $row: result from query to BOSS database
  #
  # Return: job status string (following BOSS standard)
  
  function boss_status($row) {

    // get values
    $subTime = $row['subTime'];
    $startTime = $row['startTime'];
    $stopTime = $row['stopTime'];
    $getOutTime = $row['getOutTime'];
    $lastTime = $row['lastTime'];

    // declared but not yet submitted
    if ($subTime == 0 && $startTime == 0 && $stopTime == 0)
      return "W";

    // killed
    elseif ($stopTime < 0)
      return "K";

    // ready (output not fetched yet)
    elseif ($stopTime > 0 && $getOutTime <= 0)
      return "OR";

    // finished
    elseif ($stopTime >0 && $getOutTime > 0)
      return "E";

    // in queue
    elseif ($startTime == 0)
      return "I?";

    # ignoring for now real time reporting
    #
    #// reported in last 10 minutes
    #elseif (time() - $lastTime < 600)
    #  return "R";
    #
    #// not reporting
    #else
    #  return "A"; 

    // running
    return "R";
  }

?>

