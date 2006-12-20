<?

$ds_CSA06_minbias= new dataSet("CSA06-082-os-TTbar","prod");

echo "<br>";
$vettore1=array_reverse($ds_CSA06_minbias->getJob("db","ce",0,11111111111111111,"Failed"));
while(list($key,$arr)=each($vettore1)) {
   $path1="/home/prodagent/Prodagent_v039/prodarea/JobTracking/";
   $file1=$path1."BossJob_".$arr[task_id]."_1/Submission_".$arr[id];
   echo date("d/m/y G:i:s",$arr[task_stop]).",  $arr[comment],  $arr[task_id],  $arr[id]<br>";
}


class dataSet {
  var $prodtype;
  var $jobtype;  
  function dataSet($arg1,$arg2) {
    $this->prodtype=$arg1;
    $this->jobtype=$arg2;
  }

  function getJob($arg1,$arg2,$arg3,$arg4,$arg5) {
    // arg1 = db,file,mix
    // arg2 = ce
    // arg3 = starttime timestamp
    // arg4 = stoptime timestamp  
    // arg5 = stato

    if($arg1 == "db") {
      if($this->jobtype=="merge") $name=$this->jobtype."job-[0-9].[0-9]";
      if($this->jobtype=="prod") $name=$this->prodtype."-[0-9]";
      if($arg2 != "") $qce=" and ";
      $qtime="";
      if($arg3 != $arg4) { $qtime=" and a.sub_time>=$arg3 and a.sub_time<$arg4";}
      if($arg5 == "all") $qstato="";
      if($arg5 == "Running") $qstato=" and sched_status='Running'";
      if($arg5 == "Scheduled") $qstato=" and sched_status='Scheduled'";
      if($arg5 == "Success") $qstato=" and (cmssw.task_name='Success' or (cmssw.task_name='StageOut1' and cmssw.task_exit=0))";
      if($arg5 == "Failed") {
	$query="select c.task_stop,c.comment,a.task_id,a.id from JOB a,TASK_HEAD b,cmssw c where a.task_id=b.id and a.task_id=c.task_id and a.id=c.id and b.task_name regexp '$name' and 
(c.task_name='Failed' or c.task_exit>0) order by c.task_stop asc";
      }
      $fromdb= new connect_db("pccms6","ProdAgentDB_BOSS",$query);
      $array=$fromdb->getResult();
      return $array;
    }

    if($arg1 == "file") {
      if($this->jobtype=="merge") $name=$this->jobtype."job";
      if($this->jobtype=="prod") $name=$this->prodtype;
      $path="/home/prodagent/Prodagent_v039/prodarea/JobCreator/";
      if($dir=@opendir($path)) {
	while($file=@readdir($dir)) {
          if($arg3 != $arg4) { $bool = (filectime($path.$file) >= $arg3 && filectime($path.$file) < $arg4); }
          else { $bool = TRUE;}
          if(strstr($file,$name) && $bool) {
	    $subfile=$path.$file."/".substr($file,0,strpos($file,"-cache"))."id";
	    if(file_exists($subfile)) {
	      $fp=fopen($subfile,"r");
	      $contents=fread($fp,filesize($subfile));
	      fclose($fp);
	      parse_str($contents);
	      $subarray[task_id]=$JobId;
	    } else {
	      $subarray[task_id]="N/A";
	    } 
	    $array[]=$subarray;
	  }
	}
      }
      return $array;
    }
  }




  function getTotJobSub($arg1,$arg2,$arg3,$arg4) {
    // arg1 = db,file,mix
    // arg2 = ce
    // arg3 = starttime timestamp
    // arg4 = stoptime timestamp  

    if($arg1 == "db") {
      if($this->jobtype=="merge") $name=$this->jobtype."job-[0-9].[0-9]";
      if($this->jobtype=="prod") $name=$this->prodtype."-[0-9]";
      if($arg2 != "") $qce=" and ";
      if($arg3 != $arg4) { $query="select count(a.task_id) from JOB a,TASK_HEAD b where a.id=1 and a.task_id=b.id and b.task_name regexp '$name' and a.sub_time>=$arg3 and a.sub_time<$arg4";}
      else { $query="select count(a.task_id) from JOB a,TASK_HEAD b where a.id=1 and a.task_id=b.id and b.task_name regexp '$name'";}
      $fromdb= new connect_db("pccms6","ProdAgentDB_BOSS",$query);
      $array=$fromdb->getResult();
      list($key,$tot)=each($array[0]);
      return $tot;
    }
    if($arg1 == "file") {
      $tot=0;
      if($this->jobtype=="merge") $name=$this->jobtype."job";
      if($this->jobtype=="prod") $name=$this->prodtype;
      $path="/home/prodagent/Prodagent_v039/prodarea/JobCreator/";
      if($dir=@opendir($path)) {
	while($file=@readdir($dir)) {
          if($arg3 != $arg4) { $bool = (filectime($path.$file) >= $arg3 && filectime($path.$file) < $arg4); }
          else { $bool = TRUE;}
          if(strstr($file,$name) && $bool) {
	    $subfile=$path.$file."/DashboardInfo.xml";
	    if(file_exists($subfile)) {
	      $fp=fopen($subfile,"r");
	      $contents=fread($fp,filesize($subfile));
	      fclose($fp);
	      $inizio=strpos($contents,"Task=");
	      $fine=strpos(strstr($contents,'Task'),"\">");
	      $dataset=substr($contents,$inizio+16,$fine-16);
	    } else {
	      $dataset="N/A";
	      echo "flop: $ii++<br>";
	    } 
	    if(!strcmp($this->prodtype,$dataset)) $tot++;
	  }
	}
      }
      return $tot;
    }
  }

  function getJobSub($arg1,$arg2,$arg3,$arg4) {
    // arg1 = db,file,mix
    // arg2 = ce
    // arg3 = starttime timestamp
    // arg4 = stoptime timestamp  

    if($arg1 == "db") {
      if($this->jobtype=="merge") $name=$this->jobtype."job-[0-9].[0-9]";
      if($this->jobtype=="prod") $name=$this->prodtype."-[0-9]";
      if($arg2 != "") $qce=" and ";
      if($arg3 != $arg4) { $query="select a.task_id from JOB a,TASK_HEAD b where a.id=1 and a.task_id=b.id and b.task_name regexp '$name' and a.sub_time>=$arg3 and a.sub_time<$arg4";}
      else { $query="select a.task_id from JOB a,TASK_HEAD b where a.id=1 and a.task_id=b.id and b.task_name regexp '$name'";}
      $fromdb= new connect_db("pccms6","ProdAgentDB_BOSS",$query);
      $array=$fromdb->getResult();
      return $array;
    }

    if($arg1 == "file") {
      if($this->jobtype=="merge") $name=$this->jobtype."job";
      if($this->jobtype=="prod") $name=$this->prodtype;
      $path="/home/prodagent/Prodagent_v039/prodarea/JobCreator/";
      if($dir=@opendir($path)) {
	while($file=@readdir($dir)) {
          if($arg3 != $arg4) { $bool = (filectime($path.$file) >= $arg3 && filectime($path.$file) < $arg4); }
          else { $bool = TRUE;}
          if(strstr($file,$name) && $bool) {
	    $subfile=$path.$file."/".substr($file,0,strpos($file,"-cache"))."id";
	    if(file_exists($subfile)) {
	      $fp=fopen($subfile,"r");
	      $contents=fread($fp,filesize($subfile));
	      fclose($fp);
	      parse_str($contents);
	      $subarray[task_id]=$JobId;
	    } else {
	      $subarray[task_id]="N/A";
	    } 
	    $array[]=$subarray;
	  }
	}
      }
      return $array;
    }
  }



 function getTotJobReSub($arg1,$arg2,$arg3,$arg4) {
    // arg1 = db,file,mix
    // arg2 = ce
    // arg3 = starttime timestamp
    // arg4 = stoptime timestamp  

    if($arg1 == "db") {
      if($this->jobtype=="merge") $name=$this->jobtype."job-[0-9].[0-9]";
      if($this->jobtype=="prod") $name=$this->prodtype."-[0-9]";
      if($arg2 != "") $qce=" and ";
      if($arg3 != $arg4) { $query="select count(a.task_id) from JOB a,TASK_HEAD b where a.id>1 and a.task_id=b.id and b.task_name regexp '$name' and a.sub_time>=$arg3 and a.sub_time<$arg4";}
      else { $query="select count(a.task_id) from JOB a,TASK_HEAD b where a.id>1 and a.task_id=b.id and b.task_name regexp '$name'";}
      $fromdb= new connect_db("pccms6","ProdAgentDB_BOSS",$query);
      $array=$fromdb->getResult();
      list($key,$tot)=each($array[0]);
      return $tot;
    }

    if($arg1 == "file") {
      $tot=0;
      if($this->jobtype=="merge") $name=$this->jobtype."job";
      if($this->jobtype=="prod") $name=$this->prodtype;
      $path="/home/prodagent/Prodagent_v039/prodarea/JobCreator/";
      if($dir=@opendir($path)) {
	while($file=@readdir($dir)) {
          if($arg3 != $arg4) { $bool = (filectime($path.$file) >= $arg3 && filectime($path.$file) < $arg4); }
          else { $bool = TRUE;}
          if(strstr($file,$name) && $bool) {
	    $subfile=$path.$file."/DashboardInfo.xml";
	    if(file_exists($subfile)) {
	      $fp=fopen($subfile,"r");
	      $contents=fread($fp,filesize($subfile));
	      fclose($fp);
	      $inizio=strpos($contents,"Task=");
	      $fine=strpos(strstr($contents,'Task'),"\">");
	      $dataset=substr($contents,$inizio+16,$fine-16);
	    } else {
	      $dataset="N/A";
	    } 
	    if(!strcmp($this->prodtype,$dataset)) $tot++;
	  }
	}
      }
      return $tot;
    }
  }


}


class connect_db {
  var $results;
      function connect_db($arg1,$arg2,$arg3) {
	if ($arg1 == "pccms6") {
	    $host_mysql="pccms6.cmsfarm1.ba.infn.it";
            $login_mysql="antonio";
            $pass_mysql="boss_passw_1";
            @mysql_connect($host_mysql,$login_mysql,$pass_mysql) or die ("Connessione al DB non riuscita"); 
            @mysql_select_db($arg2) or die ("DB non trovato!");
	    $this->results=mysql_query($arg3);
        }
      }
      function getResult() {
       	$nfield=mysql_num_fields($this->results);
        $nrow=mysql_num_rows($this->results);
	for($irow=0;$irow<$nrow;$irow++) {
	  $row=mysql_fetch_array($this->results);
	  for($ifield=0;$ifield<$nfield;$ifield++){
	    $fname=mysql_field_name($this->results,$ifield);
	    $subarray[$fname]=$row[$fname];
	  }
          $array[]=$subarray;
	}
	return $array;
      }
}          

?>
