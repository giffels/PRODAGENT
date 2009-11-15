#!/bin/bash

#MLM First we have to get the env variable for location of install.  We should pass this as an argument
source $PBIN/env_mytestarea.sh;

if [ -d $PRODAGENT_WORKDIR ]; then
    #Prompt user for making sure that he/she wants to proceed:
    echo -e "\nThis installation will erase the previous one in the directory $PRODAGENT_WORKDIR ! Do you want to continue (Y/n)? "
    read choice
    if [ "$choice" != "Y" ]; then echo "Installation aborted"; exit 1; fi
fi

#Make the directory
mkdir -p $MYTESTAREA;

#cleanup old install if found
rm -rf $MYTESTAREA/aptinstaller.sh
rm -rf $MYTESTAREA/bin
rm -rf $MYTESTAREA/data
rm -rf $MYTESTAREA/etc
rm -rf $MYTESTAREA/lib
rm -rf $MYTESTAREA/log.txt
rm -rf $MYTESTAREA/RPMS
rm -rf $MYTESTAREA/$SCRAM_ARCH
rm -rf $MYTESTAREA/system-import.*
rm -rf $MYTESTAREA/var


#now start the install (it depends on the architecture! See https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgentInstallation#ProdAgent_Installation_Guide)
    wget -O $MYTESTAREA/bootstrap-$SCRAM_ARCH.sh http://cmsrep.cern.ch/cmssw/cms/bootstrap.sh
    sh -x $MYTESTAREA/bootstrap-$SCRAM_ARCH.sh setup -repository comp -path $MYTESTAREA -arch $SCRAM_ARCH
    source $MYTESTAREA/$SCRAM_ARCH/external/apt/0.5.15lorg3.2-cmp/etc/profile.d/init.sh
    apt-get update
    apt-get install cms+prodagent+PRODAGENT_$PAVERSION-cmp
