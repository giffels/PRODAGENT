
import os

import getopt
import getpass
import sys

from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails

def usage(scriptName):
    usage = \
    """
    Usage: %s
           You can optionally specify the ProdAgent Config file with
           --config otherwise it will expect to get the config file from
           the $PRODAGENT_CONFIG environment variable.
           --help this message.

           Make sure you have the access rights to install the schema.


    """ %(scriptName)
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
   
   for opt, arg in opts:
       if opt == "--config":
           config = arg
       if opt == "--help":
           usage()
           sys.exit(1)

   if config == None:
       config = os.environ.get("PRODAGENT_CONFIG", None)
       if config == None:
           msg = "No ProdAgent Config file provided\n"
           msg += "either set $PRODAGENT_CONFIG variable\n"
           msg += "or provide the --config option"

   print
   print("This script assumes the mysql server is up and running and that you have")
   print("a MySQL user name and password with privileges to install and configure")
   print("the prod agent database")
   print

   print("Using config file: "+config)
   return config

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


def installDB(schemaLocation,dbName,socketFileLocation,portNr,host,users,installUser,replace=True ):

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
          stdin,stdout=os.popen4('mysql '+updateData+' --exec \'DROP DATABASE IF EXISTS '+dbName+';\'')
          y+=str(stdout.read())
          stdin,stdout=os.popen4('mysql '+updateData+' --exec \'CREATE DATABASE '+dbName+';\'')
          y+=str(stdout.read())
     
      stdin,stdout=os.popen4('mysql '+updateData+' '+dbName+' < '+schemaLocation)
      y+=str(stdout.read())
      if y!='':
          raise Exception('ERROR',str(y))
      print('')
      if replace:
          print('Created Database: '+dbName)
      else:
          print('Augmented Database: '+dbName)
   except Exception,ex:
      print(str(ex))
      print('Perhaps you do not have permission to create a new database.')
      print('Check your SQL permissions with your database administrator')
      raise


   for user in users.keys():
       passwd=users[user]
   
       grantCommand='GRANT UPDATE,SELECT,DELETE,INSERT ON '+dbName+'.* TO \''+user+'\'@\''+host+'\' IDENTIFIED BY \''+passwd+'\';'
   
       # update grant information.
   
       try:
           command='mysql '+updateData+' --exec "'+grantCommand+'"'
           stdin,stdout=os.popen4(command)
           y=''
           y=stdout.read()
           if y!='':
               raise
           print('')
           print('Provided access to database '+dbName+' for user profile '+user)
       except Exception,ex:
           print(str(y))
           print('Perhaps you do not have permission to grant access.')
           print('Check your SQL permissions with your database administrator')
   

