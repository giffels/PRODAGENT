<?
//include_once("common/dbLib-FTS.php");

class getProdParams {
	var $val;
	var $allprod;
	var $sites;
	var $filename = "local/ProdConfMonitor.xml";
	var $key_start;
	var $key_stop;
	var $CE_ls;

	function getProdParams($arg="dummy") {
		$xml_parser =xml_parser_create();
		$data = implode(" ",file($this->filename));
		xml_parse_into_struct($xml_parser,$data,$values,$tags);
		xml_parser_free($xml_parser);
		while(list($key,$val)=each($values)) {
			if ($val[tag] == "PRODUCTION" && $val[type] == "open") $key_popen=$key;
			if ($val[tag] == "NAME" && $val[value] == $arg && $key_popen) $key_name=$key;
			if ($val[tag] == "PRODUCTION" && $val[type] == "close" && $key_name) $key_pclose=$key;
			if($key_popen && $key_name && $key_pclose) break;    	   
		}
		$this->key_start=$key_popen;
		$this->key_stop=$key_pclose;
		$this->val=$values;
	}

	function getAllProds() {
		$xml_parser =xml_parser_create();
		$data = implode(" ",file($this->filename));
		xml_parse_into_struct($xml_parser,$data,$values,$tags);
		xml_parser_free($xml_parser);
		while(list($key,$val)=each($values)) {
			if ($val[tag] == "NAME") $allprod[]=$val[value];
		}
		return $allprod;
	}
	function getPar($arg) {
		reset ($this->val);
		while(list($key,$val)=each($this->val)) {
			if($key >= $this->key_start && $key <= $this->key_stop) {
				if ($val[tag] == strtoupper($arg)) return $val[value];
			} 
		}
	}

	function getAllSites() {
		reset ($this->val);
		while(list($key,$val)=each($this->val)) {
			if($key >= $this->key_start && $key <= $this->key_stop) {
				if ($val[tag] == "SITE" && $val[type] == "open") $k_site=$key;
				if ($val[tag] == "SITENAME" && $k_site) {
					$sito=$val[value];
				}
				if ($val[tag] == "CE" && $k_site) {
					$ce_list[]=$val[value];
				}
				if ($val[tag] == "SE" && $k_site) {
					$se_list[]=$val[value];
				}
				if ($val[tag] == "SITE" && $val[type] == "close") {
					unset($k_site);	  
					$sites[$sito]=array("celist"=>$ce_list,"selist"=>$se_list);
					$ce_list = array();
					$se_list = array();
				}
			}
		}
		return $sites;
	}

	function getAllCEs() {
		reset ($this->val);
		while(list($key,$val)=each($this->val)) {
			$CE_ls[]='';
			if($key >= $this->key_start && $key <= $this->key_stop) {
				if ($val[tag] == "SITE" && $val[type] == "open") $k_site=$key;
				if ($val[tag] == "SITENAME" && $k_site) {
					$sito=$val[value];
				}
				if ($val[tag] == "CE" && $k_site) {
					$CE_ls[]=$val[value];
				}
				if ($val[tag] == "SITE" && $val[type] == "close") {
					unset($k_site);	  
					$sites[$sito]=array("celist"=>$ce_list,"selist"=>$se_list);
				}
			}
		}
		return $CE_ls;
	}

}
?>
