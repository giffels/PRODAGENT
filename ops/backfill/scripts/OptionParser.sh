#!/bin/bash
# Light weight option parser for bash
# Victor E. Bazterra CHPC University of Utah 2005
#


abstract=""


usage="Usage: \${0##*/} [options] [arg1] [arg2] ..."


function Abstract()
{
 abstract=$@
}


function Usage()
{
 usage=$@
}


function Option()
{
 eval "options=\"$options $1,$2,$3\""
 eval "main_help=\"$main_help \t -$1,--$2\n\""
 eval "main_help=\"$main_help \t $4\n\n\""
}


function OptionWithArgument()
{
 ARGUMENT=`echo $3 | tr a-z A-Z`
 eval "options_warg=\"$options_warg $1,$2,$3\""
 eval "main_help=\"$main_help \t -$1 $ARGUMENT, --$2=$ARGUMENT\n\""
 eval "main_help=\"$main_help \t $4\n\n\""
}


function OptionWithExpression()
{
 ARGUMENT=`echo $3 | tr a-z A-Z`
 eval "options_wexp=\"$options_wexp $1,$2,$3\""
 eval "main_help=\"$main_help \t --$2=$ARGUMENT\n\""
 eval "main_help=\"$main_help \t $4\n\n\""
}


function GenerateParser()
{
 Option h help help 'Print this help.'

 string="Usage() {
           echo;
           echo \"$abstract\";
           echo;
           echo \"$usage\";
           echo;
           echo -e -n \"$main_help \";
           exit;
         };
         count_arg=1;
         while [ \$# != 0 ];
           do case \"\$1\" in"
 for option in $options
 do
   field1=${option%%,*}
   field3=${option##*,}
   ftemp=${option#*,}
   field2=${ftemp%,*}
   string="$string
           -$field1) $field3=true;;
           --$field2) $field3=true;;"
 done
 for option in $options_warg
 do
   field1=${option%%,*}
   field3=${option##*,}
   ftemp=${option#*,}
   field2=${ftemp%,*}
   string="$string
           -$field1) if [ \$# != 1 ];
                     then
                       $field3=\$2;
                       shift;
                     else
                       echo \"Missing argument for option: \$1.\";
                       exit;
                     fi;;
           --$field2=*) $field3=\${1#-*=};
                        if [ -z \"\$$field3\" ];
                        then
                          echo \"Missing argument for option: \$1.\";
                          exit;
                        fi;;"
 done
 for option in $options_wexp
 do
   field1=${option%%,*}
   field3=${option##*,}
   ftemp=${option#*,}
   field2=${ftemp%,*}
   string="$string
           --$field2=*) $field3=\${1#-*=};
                        if [ -z \"\$$field3\" ];
                        then
                          echo \"Missing argument for option: \$1.\";
                          exit;
                        fi;;"
 done
 string="$string
         -*) echo \"Unrecognized option: \$1.\";
             Usage;;
         *) arg[count_arg]=\$1;
            let \"count_arg+=1\";;
         esac;
         shift;
         done;
         if [ -n \"\$help\" ];
         then
           Usage;
         fi"
 eval $string
}

