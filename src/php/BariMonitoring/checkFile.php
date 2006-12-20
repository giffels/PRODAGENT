<?

  $taskid=$_GET['task_id'];
  $id=$_GET['id'];
  $file=$_GET['file'];

  $path="/home/prodagent/Prodagent_v039/prodarea/JobTracking/BossJob_".$taskid."_1/Submission_$id/";
  $file=$path.$file;
  $fp=fopen($file,"r");
$contents=fread($fp,filesize($file));
echo nl2br($contents);

?>