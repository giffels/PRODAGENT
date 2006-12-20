<?php
$job_type=$_GET["job_type"];	
$production=$_GET["production"];	
if($job_type=="prod"){$filename="file_lists/$production-dbsunmerg.txt";}
else {$filename="file_lists/$production-dbsmerg.txt";}

if(file_exists($filename)){
$last_change=filectime ($filename);
$fd = fopen ($filename, "r");
	$content = fread ($fd, filesize ($filename));
	fclose ($fd);

echo "<h2> Listing of file <font color=blue> $filename </font></h2>";
echo "<h4>last update ".date("G:i d/m",$last_change)."<br>file size: ".filesize ($filename)."</h4>";
echo "<pre>$content</pre>";
}
else{
echo "<h2> File <font color=blue> $filename </font> not available<h2>";
}
?>
