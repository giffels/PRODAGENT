
#!/bin/sh 
nohup mysqld_safe --defaults-extra-file=$PRODAGENT_WORKDIR/mysql/my.cnf --datadir=$PRODAGENT_WORKDIR/mysqldata --socket=$PRODAGENT_WORKDIR/mysql/sock --skip-networking --log-error=$PRODAGENT_WORKDIR/mysql/error.log --pid-file=$PRODAGENT_WORKDIR/mysql/mysqld.pid > /dev/null 2>&1 < /dev/null & 


