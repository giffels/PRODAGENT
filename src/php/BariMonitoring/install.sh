#!/bin/bash  
# 
# Installation script for Bari Monitoring
#
# Arguments:
#
#   -a: skip apache installation
#   -p: skip php installation
#
# Assume the files:
#                  apache_1.3.37.tar.gz
#                  php-4.4.4.tar.gz
#                  adodb493a.tgz
#                  jpgraph-1.20.5.tar.gz
#
# are located in the current directory, and also
# the ProductionAgent environment is available

# files required

APACHE_FILE=apache_1.3.37.tar.gz
PHP_FILE=php-4.4.4.tar.gz
ADODB_FILE=adodb493a.tgz
JPGRAPH_FILE=jpgraph-1.20.5.tar.gz

# directories

APACHE_DIR=${APACHE_FILE%\.tar.gz}
PHP_DIR=${PHP_FILE%\.tar.gz}
JPGRAPH_DIR=${JPGRAPH_FILE%\.tar.gz}

# determine if apache and PHP have to be installed

SKIP_APACHE=false
SKIP_PHP=false

# process command line arguments

while getopts ap opt
do
   case "$opt" in
     a)  SKIP_APACHE=true;;
     p)  SKIP_PHP=true;;
     \?) echo >&2 "usage: $0 [-a] [-p]"
         echo >&2 "  -a: skip apache compilation"
         echo >&2 "  -p: skip apache compilation"
         exit 1;;
   esac
done 

# check for prodagent configuration information

if [ -z "$PRODAGENT_WORKDIR" ]
then
    echo "Please set the ProdAgent environment"
    exit 1
fi

if [ -z "$MYTESTAREA" ]
then
    echo "Please set the ProdAgent environment"
    exit 1
fi

# check files are there

if [[ $SKIP_APACHE == false && ! -e $APACHE_FILE ]];
then
    echo "File $APACHE_FILE not found" 
    echo "Please copy it into the current directory"
    exit 1
fi

if [[ $SKIP_PHP == false && ! -e $PHP_FILE ]];
then
    echo "File $PHP_FILE not found"
    echo "Please copy it into the current directory"
    exit 1
fi

if [ ! -e $ADODB_FILE ];
then
    echo "File $ADODB_FILE not found"
    echo "Please copy it into the current directory"
    exit 1
fi

if [ ! -e $JPGRAPH_FILE ];
then
    echo "File $JPGRAPH_FILE not found"
    echo "Please copy it into the current directory"
    exit 1
fi

# set the base directory for installation

BASEDIR=`pwd`

# install and configure apache

if [ $SKIP_APACHE == false ]; then

    echo "Installing Apache..."

    # configuration
 
    tar xzf $APACHE_FILE
    cd $APACHE_DIR
    ./configure --prefix=$BASEDIR/apache --enable-module=so --enable-module=auth

    if [ $? != 0 ]; then
	echo "Error in configuration of apache"
	exit 1
    fi

    # compilation

    make
    if [ $? != 0 ]; then
	echo "Error in compilation of apache"
    exit 1
    fi

    # installation

    make install
    if [ $? != 0 ]; then
	echo "Error in installation of apache"
	exit 1
    fi

    # modify configuration
 
    cd ../apache/conf

    cp httpd.conf.default temp.conf

    # add php as a recongnized type in apache

    echo -e '\nAddType application/x-httpd-php .php' >> temp.conf

    # modify .htaccess processing

    sed -e '320,330s/AllowOverride None/AllowOverride All/' temp.conf > httpd.conf

fi

# install and configure php

cd $BASEDIR

if [ $SKIP_PHP == false ]; then

    echo "Installing php..."
    
    tar xzf $PHP_FILE
    cd $PHP_DIR
    ./configure --with-mysql --with-apxs=$BASEDIR/apache/bin/apxs \
	--prefix=$BASEDIR/php --with-gd \
	--with-zlib-dir=$MYTESTAREA/slc3_ia32_gcc323/external/zlib/1.1.4 \
	--with-dom --with-config-file-path=$BASEDIR/php/lib
    if [ $? != 0 ]; then
	echo "Error in configuration of php"
	exit 1
    fi

    make
    if [ $? != 0 ]; then
	echo "Error in compilation of php"
	exit 1
    fi

    make install
    if [ $? != 0 ]; then
	echo "Error in installation of php"
	exit 1
    fi
    
    cp php.ini-dist $BASEDIR/php/lib/php.ini
    
fi

# create web area 

echo "Installing Bari monitoring"

cd $BASEDIR/apache/htdocs
mkdir -p PA

for dirname in `echo common config graph local menu_date modules plots restricted_folder`;do
    cp -r $PRODAGENT_ROOT/src/php/BariMonitoring/$dirname PA 
    if [ $? != 0 ]; then
	echo "Error: cannot find BariMonitoring source in $PRODAGENT_ROOT/src/php"
	exit 1
    fi
done

cp  $PRODAGENT_ROOT/src/php/BariMonitoring/*.php PA 
if [ $? != 0 ]; then
    echo "Error: cannot find BariMonitoring source in $PRODAGENT_ROOT/src/php"
    exit 1
fi
cp  $PRODAGENT_ROOT/src/php/BariMonitoring/*.sh PA 
if [ $? != 0 ]; then
    echo "Error: cannot find BariMonitoring source in $PRODAGENT_ROOT/src/php"
    exit 1
fi

# create link to production area and empty log files

cd PA

ln -f -s $PRODAGENT_WORKDIR Production

touch DBSInterface.txt JobSubmitter.txt JobTracking.txt MergeSensor.txt

# change permissions for configuration and plots area 

chmod 777 plots
chmod 777 local/ProdConfMonitor.xml
chmod 777 local/Site.xml

# uncompress jpgraph

tar xzf $BASEDIR/$JPGRAPH_FILE

if [ $? != 0 ]; then
    echo "Error: cannot untar jpgraph"
    exit 1
fi

mv $JPGRAPH_DIR jpgraph

# uncompress adodb

cd ..

tar xzf $BASEDIR/$ADODB_FILE

if [ $? != 0 ]; then
    echo "Error: cannot untar adodb"
    exit 1
fi

# create temporary directory for graphics

mkdir -p /tmp/${PRODAGENT_WORKDIR}/adodb_cache

# update local PA configuration

cd $BASEDIR/apache/htdocs/PA/local
sed -e s%DUMMY\_ROOT%$PRODAGENT_ROOT% PAConfig.xml.dummy | \
sed -e s%DUMMY\_WORKDIR%$PRODAGENT_WORKDIR% | \
sed -e s%DUMMY\_CONFIG%$PRODAGENT_CONFIG% > PAConfig.xml

# setup .htaccess file in the restricted folder
cd $BASEDIR/apache/htdocs/PA/restricted_folder

cat <<EOF >> .htaccess
AuthName "Restricted Area"
AuthType Basic
AuthUserFile $BASEDIR/apache/htdocs/PA/restricted_folder/.htpasswd
require valid-user
EOF

# go back to top directory

cd $BASEDIR

# display messages

echo
echo "Installation finished" 

echo "Log in your mysql server as root and execute the following command,"
echo "where ProdAgentUser and ProdAgentPassword has to be replaced with your"
echo "settings as defined in fields user and passwd in block ProdAgentDB in"
echo "your Production Agent configuration file."
echo     
echo "mysql> UPDATE mysql.user"
echo "         SET Password = OLD_PASSWORD('ProdAgentPassword')"
echo "         WHERE Host = 'localhost' AND User = 'ProdAgentUser'; "
echo "mysql> FLUSH PRIVILEGES; "
echo
echo "Start the deamon to get component log messages updated regularly :"
echo "  (cd ${BASEDIR}/apache/htdocs/PA; nohup sh ComponentLog.sh &)"
echo
echo "Start apache:"
echo "  ${BASEDIR}/apache/bin/apachectl start"
echo
echo "Set the password for restricted area:"
echo "  ${BASEDIR}/apache/bin/htpasswd -n prodagent >> ${BASEDIR}/apache/htdocs/PA/restricted_folder/.htpasswd "
echo
echo "Point your browser to http://localhost:8080/PA/index.php"
echo
echo "When necessary, stop the apache server:"
echo "  ${BASEDIR}/apache/bin/apachectl stop"

