#!/bin/sh

# Importing the option parser
. ~/backfill/scripts/OptionParser.sh


# Description of the script
Abstract \${0##*/} - Realocate archives in function of site and acquisition era.


# Description of how to use the script
Usage \${0##*/} "[-s,-f] site1 [site2] ..."


# Defining the parameters to be parsed
Option s success success 'Relocate only the success archive.'
Option f failure failure 'Relocate only the failure archive.'

# Generating the option parser
GenerateParser "$@"


if (( ${#arg[@]} < 1 )); then
    Usage
fi

if [ -z $success ] && [ -z $failure ]
then
    success="true"
    failure="true"
fi

if [ -z $PRODAGENT_ROOT ]
then
    echo 'Missing $PRODAGENT_ROOT enviroment variable.'
    exit 1
fi

for site in ${arg[@]}; do

    if [ -n "$success" ]
    then
        cd ${PRODAGENT_ROOT}/archive/success;
        
        for tb in *${site}*.tar.gz; do

            
            echo "considering $tb ..."

            era=`echo ${tb} | awk '{match( $0,"([[:digit:]])-(.*)_(.*)_backfill-(.*)-(.*)",a ); print a[1]}'`;
            version=`echo ${tb} | awk '{match( $0,"([[:digit:]])-(.*)_(.*)_backfill-(.*)-(.*)",a ); print a[4]}'`;

            dir=./${site}/${era}/${version};

            if [ -d  ${dir} ]; then
                echo "mv ${tb} ${dir}"
                mv ${tb} ${dir}
            else
                echo "mkdir -p ${dir}"
                mkdir -p ${dir}
                echo "mv ${tb} ${dir}"
                mv ${tb} ${dir}
            fi
        done
    fi 

    if [ -n "$failure" ]
    then
        cd ${PRODAGENT_ROOT}/archive/failure;

        for tb in *${site}*.tar.gz; do
            era=`echo ${tb} | awk '{match( $0,"([[:digit:]])-(.*)_(.*)_backfill-(.*)-(.*)",a ); print a[1]}'`;        version=`echo ${tb} | awk '{match( $0,"([[:digit:]])-(.*)_(.*)_backfill-(.*)-(.*)",a ); print a[4]}'`;

            dir=./${site}/${era}/${version};
    
            if [ -d  ${dir} ]; then
                echo "mv ${tb} ${dir}"
                mv ${tb} ${dir}
            else
                echo "mkdir -p ${dir}"
                mkdir -p ${dir}
                echo "mv ${tb} ${dir}"
                mv ${tb} ${dir}
            fi
        done
     fi

done
