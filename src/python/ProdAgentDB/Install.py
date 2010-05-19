
import os

import getopt
import getpass
import sys

from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails

def usage():

    usage = \
    """
    Usage: 
           You can optionally specify the ProdAgent Config file with
           --config otherwise it will expect to get the config file from
           the $PRODAGENT_CONFIG environment variable.
           --help this message.

           Make sure you have the access rights to install the schema.


    """ 
    print usage

def preInstall(valid):
   # check the input

   try:
       opts, args = getopt.getopt(sys.argv[1:], "", valid)
   except getopt.GetoptError, ex:
       print str(ex)
       usage()
       sys.exit(1)

   config = None
   block = None   
   for opt, arg in opts:
       if opt == "--config":
           config = arg
       if opt == "--help":
           usage()
           sys.exit(1)
       if opt == "--block":
           block = arg 

   if config == None:
       config = os.environ.get("PRODAGENT_CONFIG", None)
       if config == None:
           msg = "No ProdAgent Config file provided\n"
           msg += "either set $PRODAGENT_CONFIG variable\n"
           msg += "or provide the --config option"
   if block == None:
      block = 'ProdAgentDB'

   print
   print("This script assumes the mysql server is up and running and that you have")
   print("a MySQL user name and password with privileges to install and configure")
   print("the prod agent database")
   print

   print("Using config file: "+config)
   return config, block

def adminLogin():
   # ask for password (optional)
   print
   userName=raw_input('Please provide the mysql user name (typically "root") for updating the \ndatabase server (leave empty if not needed): ')
   if userName=='':
       userName='root'
   print
   passwd=getpass.getpass('Please provide mysql passwd associated to this user name for \nupdating the database server: ')
   if passwd=='':
       passwd="''"
   return (userName,passwd)


def installMySQLDB(schemaLocation,dbName,socketFileLocation,portNr,host,installUser,replace=True ):

   updateData="--user="+installUser['userName']+ " --password="+str(installUser['passwd'])
   if (socketFileLocation!=""):
      updateData+="  --socket="+socketFileLocation
   else:
      choice=raw_input('\nYou will be using ports and hosts settings instead of \n'+\
                       'sockets. Are you sure you want to use this to connect \n'+\
                       'to a database as it is a potential security risk? (Y/n)\n ')
      if choice=='Y':
          updateData+="  --port="+portNr+"  --host="+host
      else:
          sys.exit(1)
   
   # install schema.
   try:
      y=''
      if replace:
          stdout=os.popen('mysql '+updateData+' --exec \'DROP DATABASE IF EXISTS '+dbName+';\'')
          y+=str(stdout.read())
          stdout=os.popen('mysql '+updateData+' --exec \'CREATE DATABASE '+dbName+';\'')
          y+=str(stdout.read())
     
      stdout=os.popen('mysql '+updateData+' '+dbName+' < '+schemaLocation)
      y+=str(stdout.read())
      if y!='':
          raise Exception('ERROR',str(y))
      print('')
      if replace:
          print('Created Database: '+dbName+'\n')
      else:
          print('Augmented Database: '+dbName)
   except Exception,ex:
      print('Could not proceed. Perhaps due to one of the following reasons: ')
      print('-You do not have permission to create a new database.')
      print('Check your SQL permissions with your database administrator')
      print('-Connecting through a port (not socket), but the firewall is blocking it')
      print('-The wrong host name')
      raise


def grantUsers(dbName,socketFileLocation,portNr,host,users,installUser):

   updateData="--user="+installUser['userName']+ " --password="+str(installUser['passwd'])
   if (socketFileLocation!=""):
      updateData+="  --socket="+socketFileLocation
   else:
      choice=raw_input('\nYou will be using ports and hosts settings instead of \n'+\
                       'sockets. Are you sure you want to use this to connect \n'+\
                       'to a database as it is a potential security risk? (Y/n)\n ')
      if choice=='Y':
          updateData+="  --port="+portNr+"  --host="+host
      else:
          sys.exit(1)

   for user in users.keys():
       passwd=users[user]

       import socket
       connect_from=socket.gethostname()
       print('WARNING: using "'+connect_from +'" as hostname to grant access')
   
       grantCommand='GRANT UPDATE,SELECT,DELETE,INSERT ON '+dbName+'.* TO \''+user+'\'@\''+connect_from+'\' IDENTIFIED BY \''+passwd+'\';'
   
       # update grant information.
   
       try:
           command='mysql '+updateData+' --exec "'+grantCommand+'"'
           stdout=os.popen(command)
           y=''
           y=stdout.read()
           if y!='':
               raise
           print('')
           print('Provided access to database: '+dbName+' for user profile: '+user+' and host: '+connect_from)
       except Exception,ex:
           print(str(y))
           print('Perhaps you do not have permission to grant access.')
           print('Check your SQL permissions with your database administrator')

   
       grantCommand='GRANT UPDATE,SELECT,DELETE,INSERT ON '+dbName+'.* TO \''+user+'\'@\''+host+'\' IDENTIFIED BY \''+passwd+'\';'
       print('WARNING: using "'+host +'" as hostname to grant access')

       try:
           command='mysql '+updateData+' --exec "'+grantCommand+'"'
           stdout=os.popen(command)
           y=''
           y=stdout.read()
           if y!='':
               raise
           print('')
           print('Provided access to database: '+dbName+' for user profile: '+user+' and host: '+host)
       except Exception,ex:
           print(str(y))
           print('Perhaps you do not have permission to grant access.')
           print('Check your SQL permissions with your database administrator')
	  



def installOracleDB(dbType, user, passwd, tnsName, schemaLocation):

   try:
      if dbType == 'oracle':
         print 'Removing Installed Schema if any...'         
         removeSchema = """
                        BEGIN
                           -- Tables
                           FOR o IN (SELECT table_name name FROM user_tables) LOOP
                               dbms_output.put_line ('Dropping table ' || o.name || ' with dependencies');
                               execute immediate 'drop table ' || o.name || ' cascade constraints';
                           END LOOP;

                           -- Sequences
                           FOR o IN (SELECT sequence_name name FROM user_sequences) LOOP
                               dbms_output.put_line ('Dropping sequence ' || o.name);
                               execute immediate 'drop sequence ' || o.name;
                           END LOOP;

                           -- Triggers
                           FOR o IN (SELECT trigger_name name FROM user_triggers) LOOP
                              dbms_output.put_line ('Dropping trigger ' || o.name);
                              execute immediate 'drop trigger ' || o.name;
                           END LOOP;

                           -- Synonyms
                           FOR o IN (SELECT synonym_name name FROM user_synonyms) LOOP
                               dbms_output.put_line ('Dropping synonym ' || o.name);
                               execute immediate 'drop synonym ' || o.name;
                           END LOOP;
                        END;
                        /
                        """

         stdout = os.popen('sqlplus %s/%s@%s << ! %s' %(user, passwd, tnsName,removeSchema))
         print str(stdout.read())
         print 'Previously Installed  Schema deleted successfully'
         print '\nInstalling New Schema ...'
         stdout=os.popen('sqlplus %s/%s@%s < %s' %(user, passwd, tnsName, schemaLocation))
         print str(stdout.read())
         print 'Schema Installed successfully'
      else:
         print 'Config block is not of oracle dbType'
         raise RuntimeError('please provide the oracle supported config block', 1) 

   except Exception, ex:
      print str(ex)
      print 'Schema Installation Failed'



