import xmlrpclib,sys,traceback,exceptions


#  //
# // Client module for the SeedServer xmlrpc server
#//


class SeedClientException(exceptions.Exception):

    _Name = "SeedClient Exception"
    def __init__(self,**args):
        self._Data = args

    def __str__(self):
        str = '%s\n'%self._Name
        for key in self._Dict.keys():
            str = str +'%s : %s\n'%(key,self._Dict[key])
        return str



class SeedServerClient:


    def __init__(self,server_host='localhost',server_port=8080):
        self._Address = 'http://%s:%s'%(server_host,server_port)
        try:
            self._Server = xmlrpclib.Server(self._Address)
        except:
            raise SeedClientException(
                Message="Cannot Connect to Server",
                Address=self._Address,
                SysExcInfo0=sys.exc_info()[0],
                SysExcInfo1=sys.exc_info()[1],
                Traceback=traceback.print_tb(sys.exc_info()[2])
                )

    #  //
    # // Only one method needed...
    #//
    def getSeed(self):
        try:
            val = self._Server.create_seed()
            return int(val)
        except:
            raise SeedClientException(
                Message="Error Creating Seed",
                Address=self._Address,
                SysExcInfo0=sys.exc_info()[0],
                SysExcInfo1=sys.exc_info()[1],
                Traceback=traceback.print_tb(sys.exc_info()[2])
                )
            
            
    
#  //-------------Client test and example----------
# //
#//
if __name__ == '__main__':

    
    #  //
    # // Create a server client and generate some seeds...
    #//  How easy is this ;-)
    t = SeedServerClient('hood-clued0.fnal.gov',8080)
    for i in range(0,100):
        print t.getSeed()
        
