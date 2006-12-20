<?php

  # Configuration class
  #
  # Revision = "$Id$"
  # Version = "$Revision$"
  # Author = "Carlos Kavka <Carlos.Kavka@ts.infn.it>"

  class Configuration {

  // production agent data
  var $parameter;

  // create a configuration object
  function Configuration() {

    # get basic configuration
    $this->getBasicConfiguration();

    # read configuration from file
    $this->readProdAgentConfig();  
  }
  
  // return a specific parameter
  function getParameter($block,$param) {

    return $this->parameter[$block][$param];
  }

  // read ProdAgent configuration file

  function readProdAgentConfig() {

    # get file location
    $configFile = $this->getParameter(Environment, PRODAGENT_CONFIG);

    # read it
    $file = file_get_contents($configFile);
    $encodedFile = utf8_encode($file);

    $domDocument = domxml_open_mem($encodedFile);

    if (!$domDocument) {
      echo "Cannot read ProdAgent configuration file";   
      exit;
    }

    # get xml root node 
    $rootDomNode = $domDocument->document_element();
  
    # get parameters organized by sections
    $blockList = $rootDomNode->get_elements_by_tagname("ConfigBlock");

    # get all config blocks
    foreach ($blockList as $block) {
      $paramList = $block->get_elements_by_tagname("Parameter");

      # get all parameters
      $paramData = array();
      foreach ($paramList as $param) {
        $paramData[$param->get_attribute("Name")] = 
          $param->get_attribute("Value");
      }

      # store them into block
      $this->parameter[$block->get_attribute("Name")] = $paramData;
    }
  }

  // get basic PA configuration
  function getBasicConfiguration() {

    # read it
    $file = file_get_contents("local/PAConfig.xml");
    $encodedFile = utf8_encode($file);
    
    $domDocument = domxml_open_mem($encodedFile);

    if (!$domDocument) {
      echo "Cannot read PA monitor configuration file";
      exit;
    }

    # get xml root node 
    $rootDomNode = $domDocument->document_element();

    # get ROOT DIR
    $paRoot = $rootDomNode->get_elements_by_tagname("PRODAGENT_ROOT");
    $this->parameter[Environment][PRODAGENT_ROOT] = 
      trim($paRoot[0]->get_content());

    # get WORK DIR 
    $workDir = $rootDomNode->get_elements_by_tagname("PRODAGENT_WORKDIR");
    $this->parameter[Environment][PRODAGENT_WORKDIR] = 
      trim($workDir[0]->get_content());

    # get CONFIG FILE 
    $confFile = $rootDomNode->get_elements_by_tagname("PRODAGENT_CONFIG");
    $this->parameter[Environment][PRODAGENT_CONFIG] = 
      trim($confFile[0]->get_content());
  }
}
?>

