
cd $MYTESTAREA/T0/src/sql/
#This is the production instance
sqlplus CMS_T0AST/PlumJ4m791@DEVDB10 @TOAST_Oracle
#This is Simons dev instance
#sqlplus CMS_T0AST_SIMON/PlumJ4m791@DEVDB10 @TOAST_Oracle
cd $PBIN

python t0astgrants.py

