<script>
function update_xml(par){
        var obj = document.form_update_xml;
        var blob='';
        for(var c=0;c<obj.length;c++){
                if(obj.elements[c].type=='text')
                        blob+=obj.elements[c].id+' @@ '+obj.elements[c].value+' ### ';
        }
        document.getElementById('blob').value=blob;
        document.getElementById('updt_ck').value=par;
        obj.submit();
}
</script>
<? 
$blob=$_GET['blob'];
$updt_ck=$_GET['updt_ck'];
$select_name_prod=-1;
if(isset($_GET['select_name_prod']))$select_name_prod=$_GET['select_name_prod'];
?>
<?
$xml_file="projekte.xml";
$root_tag="Team";
$xml_file="local/ProdConfMonitor.xml";
$root_tag="Production";
$black_node=array('Telefon','Site');
$black_child=array('prod_success_job_cond','prod_failed_job_cond','prod_job_special_cond','merge_success_job_cond','merge_failed_job_cond');


if($updt_ck=='delete'){
	echo "$Name cancelled ";
	$dokument 	= domxml_open_file($xml_file);
	$elements	= $dokument->get_elements_by_tagname($root_tag);
	$element 	= $elements[$select_name_prod];
	$element->unlink_node();
	//$children 	= $element->child_nodes();
	//$child 		= $element->remove_child($children);
	unlink($xml_file);
	$dokument->dump_file($xml_file, false, true);
}

if($blob!='' && $updt_ck=='modify'){
	update_file($blob,$xml_file,$root_tag);
}

if($blob!='' && $updt_ck=="update"){
	$dokument = domxml_open_file($xml_file);
	$elements_clone = $dokument->get_elements_by_tagname($root_tag);
	$element_clone = $elements_clone[$select_name_prod];
	$parent_clone = $element_clone->parent_node();
	$clone = $element_clone->clone_node(true);
	//if($updt_ck=='modify')
	//	$clone = $element_clone;

	$couple_tag_val=array();
	$pieces = explode(" ### ", $blob);
	for($d=0;$d<count($pieces);$d++){
		$couple_tag_val_temp=explode(" @@ ",$pieces[$d]);
		if($couple_tag_val_temp[0]!='')
			$couple_tag_val[$couple_tag_val_temp[0]][]=$couple_tag_val_temp[1];
	}
	foreach($couple_tag_val as $key => $value){
		foreach($value as $key2 => $value2){
			update_element($key,$value2,$key2,$clone);
		}
	}
	$parent_clone->append_child($clone);
	unlink($xml_file);
	$dokument->dump_file($xml_file, false, true);
}
?>


To modify a produciction select the production to modify, edit appropriate fields and click "Modify button"<br>
To add a production select the production similar, edit appropriate fields and click "Add production field"<br>

<form name='form_update_xml' method='get'>
<?
select_name_prod();
?>
<textarea id='blob' name='blob' style="visibility:hidden"></textarea>
<input type="hidden" name="updt_ck" id="updt_ck" value=0>
<?
if($select_name_prod!='-1'){
?>
<input type='button' value='Add production' onclick='update_xml("update")'>
&nbsp;<input type='button' value='Modify production' onclick='update_xml("modify")'>
&nbsp;<input type='button' value='Delete production' onclick='update_xml("delete")'>
<br>
<a href="index.php?">Home page</a>
<hr>
<?php
	$dokument = domxml_open_file($xml_file);
	$elements_clone = $dokument->get_elements_by_tagname($root_tag);
	$element_clone = $elements_clone[$select_name_prod];
	//$element_clone = $elements_clone[count($elements_clone)-1];
	if($element_clone){
		$nextNode = $element_clone->first_child();
		PrintDomTree($element_clone);
	}
	//printElements($nextNode);
}
print "</form>";


function printElements($domNode)
{
	if($domNode)
	{
		global $indent,$root_tag;
		if($domNode->node_type() == XML_ELEMENT_NODE)
		{
			print "<br />".$indent." &lt;".$domNode->node_name()."&gt <input size='100' id='".$domNode->node_name()."' type='text' value='".$domNode->get_content()."'";
			$indent.="  ";
			$nextNode = $domNode->first_child();
			printElements($nextNode);
		}

		$nextNode = $domNode->next_sibling();
		printElements($nextNode);
	}
}

function update_file($blob,$xml_file,$root_tag){
	global $select_name_prod;
	$dokument = domxml_open_file($xml_file);
	$elements_clone = $dokument->get_elements_by_tagname($root_tag);
	$element_clone = $elements_clone[$select_name_prod];
	$parent_clone = $element_clone->parent_node();
	//$clone = $element_clone->clone_node(true);

	$couple_tag_val=array();
	$pieces = explode(" ### ", $blob);
	for($d=0;$d<count($pieces);$d++){
		$couple_tag_val_temp=explode(" @@ ",$pieces[$d]);
		if($couple_tag_val_temp[0]!='')
			$couple_tag_val[$couple_tag_val_temp[0]][]=$couple_tag_val_temp[1];
	}
	foreach($couple_tag_val as $key => $value){
		foreach($value as $key2 => $value2){
			$clone=update_element($key,$value2,$key2,$element_clone);
		}
	}
	unlink($xml_file);
	$dokument->dump_file($xml_file, false, true);
}

function update_element($par1,$par2,$index,$clone){
	$elements = $clone->get_elements_by_tagname("$par1");
	$element = $elements[$index];
	if($element!=''){
		$children = $element->child_nodes();
		if($children[0])
			$element->remove_child($children[0]);
		$element->set_content("$par2");
	}
	return $clone;
}
function select_name_prod(){
	global $xml_file,$select_name_prod;
	$dokument = domxml_open_file($xml_file);
	$root = $dokument->document_element();

	$node_array = $root->get_elements_by_tagname("Name");
	echo "<select name='select_name_prod' onChange=\"javascript:this.form.submit();\">";
	echo "<option value='-1'>select a production</option>";
	for ($i = 0; $i<count($node_array); $i++) {
		$node = $node_array[$i];
		if($i==$select_name_prod)
			echo "<option value='$i' selected>".$node->get_content()."</option>";
		else
			echo "<option value='$i'>".$node->get_content()."</option>";
	}
	echo "</select>";
}

function PrintDomTree($DomNode)
{
	global $black_node,$black_child;
	if ($ChildDomNode = $DomNode->first_child()) {
		static $depth = 0;

		$whitespace = "\n<br>".str_repeat(" ", ($depth * 2));

		$node_content="";
		$node_name="";
		while ($ChildDomNode) {
			if ($ChildDomNode->node_type() == XML_TEXT_NODE) {
				if(trim($ChildDomNode->node_value())!=''){
					$node_content=$ChildDomNode->get_content();
				}
			} elseif ($ChildDomNode->node_type() == XML_ELEMENT_NODE) {
				$HasTag = 1;
				echo $whitespace;
				//echo $ChildDomNode->node_name().": ";
				if (!in_array($ChildDomNode->node_name(), $black_node) && !in_array($ChildDomNode->node_name(),$black_child))
					echo $ChildDomNode->node_name().": <input size='100' id='".$ChildDomNode->node_name()."' type='text' value='".$ChildDomNode->get_content()."'>";


				if ($ChildDomNode->has_child_nodes()) {
					$depth++;
					if (PrintDomTree($ChildDomNode)) {
						echo $whitespace;
					}
					$depth--;
				}
				echo "</", $ChildDomNode->node_name(), ">";
			}
			$ChildDomNode = $ChildDomNode->next_sibling();
		}
		//echo "<br>1: ".$node_content." 2:".$node_name."<hr>";
		return $HasTag;
	}
}

?>
