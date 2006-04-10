#!/opt/openpkg/bin/python
import ClarensDpe as Clarens
import xmlrpclib
import sys
import os
import string
import socket
import random
import time
import popen2
import signal
import select
from Rendezvous import *

# 
#"Multicast DNS Service Discovery " 
#
class MyListener(object):
    def __init__(self,r):
        self.r = r
        self.name = []
        pass

    def removeService(self, rendezvous, type, name):
        pass

    def addService(self, rendezvous, type, name):
        if string.count(name,"Clarens"):
            self.name.append(name)

def catchSIGTERM(signum,frame):
    global jobmonD
    jobmonD.unregisterJob()
    

class JobMonDaemon:
    """
    _JobMonDaemon_

    Start an clarens python client.
    """
    def __init__(self,jobname, certfile=None, keyfile=None, serverURL=None,debug=0):
        self.__jobname=jobname
        self.__clarensServerURL=serverURL       
        self.__certFILE=certfile
        self.__keyFILE =keyfile
        self.__path="/tmp/"

        self.__reconnect_time=300          
        self.__wakeup_method="UDPbroadcast"
        self.__tcpserver_hostname="local_host"
        self.__tcpserver_port=200  
        self.__clarensSvrList=[]
        self.__sock=None
        self.__fifo_in_server=None

        self.__hostname=None
        self.__port=None
        self.__debug=debug 
        self.__logfilename = "%s/JobMon_%s_%s.log"%(os.getcwd(),self.__jobname,os.getpid())
        self.__workpath="%s"%(os.getcwd())
        for i in xrange(10):
            try:
                self.__hostname=socket.gethostname()
                self.__port=random.randint(706,63500)
                sock= socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
                sock.bind((self.__hostname,self.__port))
            except socket.error,e:
                time.sleep(10)
                continue


    def myPrintf(self,mesg):
        #filename = "JobMon_%s_%s.log"%(self.__jobname,os.getpid())
        ff=open(self.__logfilename,"a")
        msg="[%s] %s\n"%(time.ctime(),mesg)
        ff.write(msg)
        ff.close()

    #
    # DNS Discovery service, not ready yet
    #      
    def getClarensServerURL():
     
        r = Rendezvous()
        type = "_https._tcp.local."
        listener = MyListener(r)
        r.addServiceListener(type, listener)
        r.close()

        count=0
        while 1:
            if not listener.name or count<60:
                time.sleep(1)
                count=count+1
            else:
                break

        self.__clarensSvrList=[]
        for site in listener.name:
            item0=string.split(site,"._https")
            item1=string.split(item0[0],"_")
            self.__clarensSvrList.append(item1[1])

        if self.__clarensSvrList: 
            return 0
        else:
	    return 1
	    
    def registerJob(self):
        status=1
        outmsg=None

        try:  
            #connect to clarens server
            dbsvr=Clarens.client(self.__clarensServerURL,certfile=self.__certFILE,keyfile=self.__keyFILE,debug=0)

            #register client
            status, message = dbsvr.execute("JobMon.registerJob",[self.__jobname,self.__hostname,self.__port,os.getpid()])
            dbsvr.execute("system.logout",[])
            
            if status==0:  
                if(message):
                    itemList=string.split(message," ")
                    for item in itemList:
                        key=string.split(item,"=")
                        if(key[0]=="JobMon_reconnect_time"):
                            self.__reconnect_time=int(key[1])
                        elif(key[0]=="JobMon_wakeup_method"):
                            self.__wakeup_method=key[1]
                        elif(key[0]=="JobMon_tcpserver_port"):
                            self.__tcpserver_port=int(key[1])
                        elif(key[0]=="JobMon_tcpserver_hostname"):
                            self.__tcpserver_hostname=key[1]
 
		    outmsg="JobMon.registerJob:\n " 
                    outmsg=outmsg+"reconnect_time=%d "%self.__reconnect_time
                    outmsg=outmsg+"wakeup_method=%s "%self.__wakeup_method
                    if(self.__wakeup_method=="UDPbroadcast"):
                        pass
		    elif(self.__wakeup_method=="TCPbroadcast" and self.__tcpserver_hostname and self.__tcpserver_port):
                        outmsg=outmsg+"tcpserver_hostname=%s "%self.__tcpserver_hostname
                        outmsg=outmsg+"tcpserver_port=%d "%self.__tcpserver_port
                    else:
                        status=1
		        outmsg= "JobMon.registerJob: registered " 
                        outmsg=outmsg+"but get wrong configuration '%s'"%message
                else:
                    status=1
                    outmsg= "JobMon.registerJob: registered but get no configuration"
            else:
                outmsg="JobMon.registerJob: %s\n"%(message)
	    
        except xmlrpclib.Fault,(errmsg):
            outmsg="JobMon.registerJob Error: %s\n"%str(errmsg)
        #except httplib.CannotSendRequest,(errmsg):
        #    outmsg="JobMon.registerJob Error: %s\n"%str(errmsg)
        except Exception,e:
            outmsg="JobMon.registerJob Error Server Connection failed: %s\n"%str(e)

        self.myPrintf(outmsg)

        if status==1:
            time. sleep(self.__reconnect_time)
            return 1
	else:
            return 0
    
    def prepareWakeup(self):

        if self.__sock:
            try:
                self.__sock.close()
            except socket.error,e:
                self.myPrintf("socket close error: %s"%str(e))

        self.__sock=None
        sock=None
        for i in xrange(10):
            if self.__wakeup_method=="TCPbroadcast":
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((self.__tcpserver_hostname,self.__tcpserver_port))
                    sock.send("register")
                    break
                except socket.error,e:
                    self.myPrintf("tcp socket error: %s"%str(e))
                    time.sleep(10)
                    continue
            
            elif self.__wakeup_method=="UDPbroadcast":
                try:
                    sock= socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
                    sock.bind((self.__hostname,self.__port))
 
                    break
                except socket.error,e:
                    self.myPrintf( "udp socket error (%s:%d) : %s"%(hostname,port,str(e)))
                    time.sleep(10)
                    continue
            else:
                self.myPrintf("Can't identify wakeup method: %s"%wakeup_method)
                break
        if sock:     
            self.__sock=sock
            return 0
        else:
            return 1

    def waitForCall(self):
        BUFSIZE=1024
        if self.__sock:
            r,w,e = select.select([self.__sock],[],[],self.__reconnect_time)
            if r:
                try:
                    msg, addr = self.__sock.recvfrom(BUFSIZE)
                except Exception,e:
                    self.myPrintf("received error: %s"%e)
            else:
                return 1
        else:
            return 1
        #parse wakeup message jobname=<jobname> fifo_usercommand=<command fifo file>
        jobname_in_server=None
        fifo_in_server=None

        itemList=string.split(msg," ")
        for item in itemList:
            key=string.split(item,"=")
            if key[0]=="jobname":
                jobname_in_server=key[1]
            elif(key[0]=="fifo_usercommand"):
                fifo_in_server=key[1]

        if (jobname_in_server==None and fifo_in_server==None):
            self.myPrintf("get wrong wakeup call: %s"%msg)  
            return 1
        elif(jobname_in_server!=self.__jobname):
            if self.__debug:
                self.myPrintf("not my job: %s"%msg)
            return 1
        else:
            self.__jobname_in_server=jobname_in_server
            self.__fifo_in_server=fifo_in_server
            return 0
#------------------------------------------------------------------------------
# Monitoring command
#-----------------------------------------------------------------------------

    def execute_command(self,rshCommand):
        childout, childin, childerr = popen2.popen3(rshCommand)
        childin.close()
        outmsg = childout.read()
        childout.close()
        returnmessage = outmsg
        errmsg = childerr.read()
        childerr.close()
        if len(errmsg) > 0:
            returnmessage = "Error:"+errmsg
        return returnmessage


    def executeJob(self,argv):
    #args = sys.argv[0:]
        status=0
        outMsg=""
        if (argv):
            #username will use to identify the user in the other clients
            jobToDo=argv
            msg = string.split(jobToDo," ")
            if (msg[0] == "help" or msg[0]=="HELP"):
                outMsg = "JobMon command:\n"
                outMsg = outMsg + " ls - list contents of directory\n"
                outMsg = outMsg + " ps - report process status\n"
                outMsg = outMsg + " du - summarize disk usage\n"
                outMsg = outMsg + " cat - concatenate and display files\n"
                outMsg = outMsg + " tail - deliver the last part of a file\n"
                outMsg = outMsg + " head - display first few lines of files\n"

                returnmessage = outMsg

            elif (msg[0] == "ls" or msg[0] == "cat" or
                msg[0] == "tail" or msg[0] =="head" or
                msg[0] == "du" or msg[0] == "ps"):
          
                rshCommand=jobToDo
                outMsg = self.execute_command(rshCommand)
            else:
                status=1
                outMsg = "\nUnrecognized command: %s \n"%jobToDo
                outMsg = outMsg+"Try command \"help\" to learn more information\n"

        return (status,outMsg)

    def getjoboutput(self):
        try:    

            #connect to clarens server
            dbsvr=Clarens.client(self.__clarensServerURL,certfile=self.__certFILE,keyfile=self.__keyFILE,debug=0)

            #get job todo
            status,jobToDo = dbsvr.execute("JobMon.getJobToDo",[self.__fifo_in_server])
            self.myPrintf( "JobMon.getJobToDo : %s"%(jobToDo))
 
            if status==0:
            #execute command
                status, returnMSG = self.executeJob(jobToDo)

            #output result back to user
            fifo_out_server= string.replace(self.__fifo_in_server,'.fo','.fi')
            status,output = dbsvr.execute("JobMon.outputJobResult",[fifo_out_server,returnMSG])
            self.myPrintf( "JobMon.outputJobResult: sendout %d %s"%(status,output))
       
            dbsvr.execute("system.logout",[])

        except Clarens.xmlrpclib.Fault,(errmsg):
            self.myPrintf( "Clarens Error: %s\nclient quit\n"%(errmsg))
            sock.close()
            #dbsvr.system.logout()
            dbsvr.execute("system.logout",[])

    def run(self):
        time_zero=time.time()-self.__reconnect_time-10
        while 1:
	    if (time.time()-time_zero) > self.__reconnect_time:
                time_zero=time.time()
                if self.registerJob():
                    continue
                if self.prepareWakeup():
                    continue

            if self.waitForCall():
                continue
	    self.getjoboutput()

    def unregisterJob(self):
        try:
            dbsvr=Clarens.client(self.__clarensServerURL,certfile=self.__certFILE,keyfile=self.__keyFILE,debug=0)

            #register client
            status, message = dbsvr.execute("JobMon.unregisterJob",[self.__jobname,self.__hostname,self.__port,os.getpid()])
            self.myPrintf (message)
            dbsvr.execute("system.logout",[])
            #os.kill(os.getpid(), signal.SIGKILL)
        except Exception,e:
            self.myPrintf ("JobMon unregister error: "%str(e)) 
	return         


def main():
  
    signal.signal(signal.SIGTERM,catchSIGTERM)

    jobname=None 
    certFILE=None
    keyFILE=None
    #serverURL="https://fcdfcaf019.fnal.gov:8443/clarens"
    serverURL="https://t2cms01.sdsc.edu:8443/clarens"
    BUFSIZE=1024
    i=1
    usage="Usage:python2 %s --jobname <jobname> [--certfile [certfile] --keyfile [keyfile] --serverURL [serverURL]] "%sys.argv[0]
    print sys.argv
    while(i<len(sys.argv)-1):
        print i,sys.argv[i] 
        if sys.argv[i] =="--certkeyfile" and i+1<len(sys.argv):
	    i=i+1
            certFILE=sys.argv[i]
            keyFILE=sys.argv[i]
            X509_CERTKEY=sys.argv[i]
        elif sys.argv[i] =="--certfile" and i+1<len(sys.argv):
	    i=i+1
            certFILE=sys.argv[i]
        elif sys.argv[i] =="--keyfile" and i+1<len(sys.argv):
	    i=i+1
            keyFILE=sys.argv[i]
        elif sys.argv[i] =="--jobname" and i+1<len(sys.argv):
	    i=i+1
            jobname=sys.argv[i]
        elif sys.argv[i] =="--serverURL" and i+1<len(sys.argv):
	    i=i+1
            serverURL=sys.argv[i]
        else:
            print usage
            sys.exit(1) 
        i=i+1

    if not jobname:
        print usage
        sys.exit(1)

    global jobmonD
    jobmonD=JobMonDaemon(jobname,certFILE,keyFILE,serverURL)
    jobmonD.run()

    #myPrintf ( "jobdeamon should not come to this step. something wrong") 
    sys.exit(0)

if __name__ == '__main__':
    main()
