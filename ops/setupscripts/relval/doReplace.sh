#!/bin/bash


#First check if the ProdAgentConfig.xml file already exists:
if [ -e $PBIN/ProdAgentConfig.xml ]; then
    #Prompt user for making sure that he/she wants to proceed:
    echo -e "\nThis script will erase the previous configuration files in $PBIN ! Do you want to continue (Y/n)? "
    read choice
    if [ "$choice" != "Y" ]; then echo "Installation aborted"; exit 1; fi
fi

#Create the directory that is going to contain the configuration files:
mkdir -p "/data/$PRODAGENT_USER/PAProd/$RUN_SITE-$INST_TYPE-$PAVERSION"

#Put the right paths into your ProdAgentConfig.xml:
more ProdAgentConfig.xml_$INST_TYPE"template" | sed s/_PAUSER/$PRODAGENT_USER/g | sed s/_PAVERSION/$PAVERSION/g | sed s/_PAINITIAL/$PRODAGENT_INITIAL/g | sed s/_INSTTYPE/$INST_TYPE/g | sed s/_RUNSITE/$RUN_SITE/g > tmp && mv tmp $PBIN/ProdAgentConfig.xml

#Next put the right paths into your SubmitterPluginConfig.xml:
more SubmitterPluginConfig.xml_$INST_TYPE"template" | sed s/_PAUSER/$PRODAGENT_USER/g | sed s/_PAVERSION/$PAVERSION/g | sed s/_PAINITIAL/$PRODAGENT_INITIAL/g | sed s/_INSTTYPE/$INST_TYPE/g | sed s/_RUNSITE/$RUN_SITE/g > tmp && mv tmp $PBIN/SubmitterPluginConfig.xml

#Move the CreatorPluginConfig.xml to its appropriate place:
cp CreatorPluginConfig.xml_$INST_TYPE"template" $PBIN/CreatorPluginConfig.xml

#Move the jdl file that configures the submissions and the ResourceMonitor plugin config (not used in the T0LSF installation):
cp jdl_reqs.jdl $PBIN/jdl_reqs.jdl 
cp MonitorPluginConfig.xml_LCGtemplate $PBIN/MonitorPluginConfig.xml

#Move the initializing scripts to the created directory:
#cp prodagent_init.sh $PBIN/prodagent_init.sh
cp env_mytestarea.sh $PBIN/env_mytestarea.sh
cp start_prodagent.sh_$INST_TYPE $PBIN/start_prodagent.sh

