#!/bin/bash

source $PBIN/env_mytestarea.sh;

#new package install style
#source $VO_CMS_SW_DIR/$SCRAM_ARCH_INSTALL/cms/prodagent/PRODAGENT_$RPMVERSION-cmp/etc/profile.d/dependencies-setup.sh 
source $VO_CMS_SW_DIR/$SCRAM_ARCH_INSTALL/cms/prodagent/PRODAGENT_$RPMVERSION/etc/profile.d/dependencies-setup.sh 

export PRODAGENT_ROOT=$MYTESTAREA/PRODAGENT
export PRODCOMMON_ROOT=$MYTESTAREA/PRODCOMMON
export WTBASE=$MYTESTAREA/WMCORE/src
export DBS_ROOT=$MYTESTAREA/DBS
export WMCORE_ROOT=$MYTESTAREA/WMCORE
export PATH=$PRODAGENT_ROOT/bin:$DBS_ROOT/Clients/Python/bin:$PATH
export PYTHONPATH=$PRODAGENT_ROOT/lib:$PRODCOMMON_ROOT/lib:$DBS_ROOT/Clients/Python:$WMCORE_ROOT/lib:$MYTESTAREA/T0/src/python:$MYTESTAREA/T0/src/python/T0:$MYTESTAREA/LUMIDB/LumiWebService/Client:$PYTHONPATH

export PUTIL=$PRODAGENT_ROOT/util

alias PAmysql='mysql -u root -p -S $PRODAGENT_WORKDIR/mysqldata/mysql.sock'

# This line adds the PhEDEx commands to the PATH.
#source $VO_CMS_SW_DIR/$SCRAM_ARCH_INSTALL/cms/PHEDEX-micro/PHEDEX_3_0_4-cmp/etc/profile.d/init.sh

# setup the castor commands needed by CleanupManager
source $VO_CMS_SW_DIR/$SCRAM_ARCH_INSTALL/external/castor/$CASTORVERSION/etc/profile.d/init.sh

#and here we hack the DBS env
# commented out DAM & Vijay
export DBS_CLIENT_CONFIG=$DBS_ROOT/Clients/Python/DBSAPI/dbs.config 

export X509_HOST_CERT=/data/cmsprod/X509/tier0cert.pem
export X509_HOST_KEY=/data/cmsprod/X509/tier0key.pem

#export X509_HOST_CERT=/afs/cern.ch/user/c/cmsprod/private/certs/tier0cert.pem
#export X509_HOST_KEY=/afs/cern.ch/user/c/cmsprod/private/certs/tier0key.pem

#prodAgentd --status

