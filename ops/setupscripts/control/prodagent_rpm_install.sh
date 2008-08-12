
#MLM First we have to get the env variable for location of install.  We should pass this as an argument
source $PBIN/env_mytestarea.sh;

#Make the directory
rm -rf $VO_CMS_SW_DIR
mkdir -p $VO_CMS_SW_DIR

wget -O $VO_CMS_SW_DIR/bootstrap.sh http://cmsrep.cern.ch/cmssw/cms/bootstrap.sh

#sh -x $VO_CMS_SW_DIR/bootstrap.sh setup -repository comp -path $VO_CMS_SW_DIR -arch $SCRAM_ARCH_INSTALL >& $VO_CMS_SW_DIR/bootstrap_$SCRAM_ARCH.log
sh -x $VO_CMS_SW_DIR/bootstrap.sh setup -repository comp -path $VO_CMS_SW_DIR -arch $SCRAM_ARCH_INSTALL

source $VO_CMS_SW_DIR/$SCRAM_ARCH_INSTALL/external/apt/$APT_VER/etc/profile.d/init.sh

apt-get update
apt-get install cms+prodagent+PRODAGENT_$PAVERSION-cmp
#apt-get install cms+PHEDEX-micro+PHEDEX_2_5_2-cmp
