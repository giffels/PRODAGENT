#!/usr/bin/env python2.2
 
 
import string, re
import sys, os, commands
import getopt
import time
 
from xml.sax import saxutils
from xml.sax import make_parser
from xml.sax.handler import feature_namespaces
 

#################################################################
#                                                               #
#   PAmonBOSSDB.py                                              #
#                                                               #
#   A light weight monitoring for ProdAgent.                    #
#   Current job status, number of processed events              #
#   and return codes for finished jobs are extracted            #
#   from the ProdAgent BOSS database                            #
#                                                               #
#   Author: Jose Caballero ( CIEMAT )                           #
#           jose.caballero (at) cern.ch                         #
#                                                               #    
#################################################################


# ------------------------------------------------------- #
#                                                         #
#   Matrix, Table and NTuple classes                      #
#                                                         #
#   General purpose classes for data handling             #
#                                                         #
# ------------------------------------------------------- #

def converttonum(cad) :
    if cad.find('.') == -1 : return int(cad)
    else                   : return float(cad)
    
class Matrix:
    
    def __init__(self,nrows=0,ncolumns=0,value=0):

        self.__matrix = {}
        
        self.__nrows    = 0
        self.__ncolumns = 0

        if nrows > 0 and ncolumns > 0:
            
           for pos in [ (i,j) for i in range(nrows) for j in range(ncolumns) ] :
              i = pos[0]
              j = pos[1]
              self.__matrix[i,j] = value

           self.__nrows    = nrows
           self.__ncolumns = ncolumns

    def Read(self, filename):

        file=open(filename)

        i=0
        for line in file.readlines():
            j=0
            for item in line.split():
               self.__matrix[i,j]=converttonum(item)
               j+=1
            i+=1

        self.__nrows    = i
        self.__ncolumns = j

        file.close()

    """
    Checking methods
    """

    def CheckRow(self,i):

      if i not in range(self.__nrows):
          raise TypeError("\n\n    Invalid row index\n")
          return 0

      return 1

    def CheckColumn(self,j):

      if j not in range(self.__ncolumns):
          raise TypeError("\n\n    Invalid column index\n")
          return 0

      return 1

    def Check(self,i,j):

      return self.CheckRow(i) * self.CheckColumn(j)
        

    """
    read properties methods
    """

    def GetNRows(self):
        return self.__nrows
    
    def GetNColumns(self):
        return self.__ncolumns
    
    def GetMatrix(self):
        return self.__matrix
    
    def GetItem(self,i,j):

        if self.Check(i,j):
            return self.__matrix[i,j]

    def GetItems(self):

        return [ self.__matrix[i,j] for i in range(self.__nrows) for j in range(self.__ncolumns) ]
        

    def GetRow(self,i):

        if self.CheckRow(i):

            row=[]
            for j in range(self.__ncolumns):
                row.append( self.__matrix[i,j] )
            return row

    def GetColumn(self,j):
        
        if self.CheckColumn(j):
            
            column=[]
            for i in range(self.__nrows):
                column.append( self.__matrix[i,j] )
            return column

    def Copy(self):

        newmatrix = Matrix(0,0)
        for i in range( self.__nrows ):
            row = self.GetRow(i)
            newmatrix.AppendRow( row )
        return newmatrix
                
    def Print(self,just=0,prolog='',epilog=''):

        if isinstance(just,list):
           justlist=just
        else:
           if just==0:
              values=self.GetItems()
              just=max([len(str(val)) for val in values])
              justlist=[just]*self.__ncolumns
           elif just==-1:

              justlist=[]
              for j in range( self.__ncolumns ):
                  just=max([len(str(val)) for val in self.GetColumn(j)])
                  justlist.append(just)
               
           else:
              justlist=[just]*self.__ncolumns


        if prolog != '' : print prolog
        
        for i in range(self.__nrows):
          for j in range(self.__ncolumns):
              
              just=justlist[ j ]
              print str(self.__matrix[i,j]).rjust(just),
              
          print
          
        if epilog != '' : print epilog
     
    
    """
    Modifying matrix structure methods
    """

    def AppendRow(self,value=0):

        i = self.__nrows
        if i == 0:

           if isinstance(value,list):
              self.__init__(1,len(value))
              for j in range(len(value)) :
                 self.__matrix[0,j]=value[j]
           else:
              self.__init__(1,1,value)
         
        else:
            
           for j in range(self.__ncolumns):
              if isinstance(value,list): val=value[j]
              else:                      val=value
              self.__matrix[ i,j ] = val
            
           self.__nrows += 1

               
    def AppendColumn(self,value=0):

        j = self.__ncolumns
        if j == 0:

            if isinstance(value,list) :
               self.__init__(len(value),1)
               for i in range(len(value)):
                  self.__matrix[i,0]=value[i]
               
        else:
            
           for i in range(self.__nrows):
              if isinstance(value,list): val=value[i]
              else:                      val=value
              self.__matrix[ i,j ] = val
            
           self.__ncolumns += 1

    def InsertRow(self,i,value=0):

        if isinstance(value,list) and len(value) != self.__ncolumns :
            raise TypeError("\n\n    Invalid length of argument \n")
            return

        if i == self.__nrows:
             self.AppendRow(value)
        
        else:
           if self.CheckRow(i):

              self.AppendRow()

              for j in range(self.__ncolumns):
                 for I in range(self.__nrows-1,i,-1):
                     self.__matrix[I,j]=self.__matrix[I-1,j]
                     
                 if isinstance(value,list): val=value[j]
                 else:                      val=value
                 self.__matrix[I-1,j] = val
               
    def InsertColumn(self,j,value=0):

        if isinstance(value,list) and len(value) != self.__nrows :
            raise TypeError("\n\n    Invalid length of argument \n")
            return

        if j == self.__ncolumns:
            self.AppendColumn(value)
            
        else:
           if self.CheckColumn(j):

              self.AppendColumn()

              for i in range(self.__nrows):
                 for J in range(self.__ncolumns-1,j,-1):
                     self.__matrix[i,J]=self.__matrix[i,J-1]

                 if isinstance(value,list): val=value[i]
                 else:                      val=value    
                 self.__matrix[i,J-1] = val

    def DelRow(self,i):
        
        if self.CheckRow(i):
            
          for j in range(self.__ncolumns):
              for I in range(i,self.__nrows-1):
                self.__matrix[I,j] = self.__matrix[I+1,j]
              del self.__matrix[self.__nrows-1,j]
                                
          self.__nrows -= 1
                                
    def DelColumn(self,j):
        
        if self.CheckColumn(j):
            
          for i in range(self.__nrows):
              for J in range(j,self.__ncolumns-1):
                self.__matrix[i,J] = self.__matrix[i,J+1]
              del self.__matrix[i,self.__ncolumns-1]
              
          self.__ncolumns -= 1
          
    def SwapRows(self,i1,i2):
        
       if self.CheckRow(i1)*self.CheckRow(i2):
           
          for j in range(self.__ncolumns):
               tmp=self.__matrix[i1,j]
               self.__matrix[i1,j]=self.__matrix[i2,j]
               self.__matrix[i2,j]=tmp
               
    def SwapColumns(self,j1,j2):
        
       if self.CheckColumn(j1)*self.CheckColumn(j2):
           
          for i in range(self.__nrows):
               tmp=self.__matrix[i,j1]
               self.__matrix[i,j1]=self.__matrix[i,j2]
               self.__matrix[i,j2]=tmp

               
    """
    modifiying values methods
    """

    def SetItem(self,i,j,value=0):
        
        if self.Check(i,j):
            self.__matrix[i,j]=value

    def ModifyItem(self,i,j,f=lambda x:x):
        
        if self.Check(i,j):
            
            oldvalue = self.__matrix[i,j]
            newvalue = f( oldvalue )
            self.__matrix[i,j] = newvalue

    def ModifyItems(self,f=lambda x:x):
        
        for i in range(self.__nrows):
            for j in range(self.__ncolumns):
                self.ModifyItem(i,j,f)

    def IncItem(self,i,j,inc=1):

        if self.Check(i,j):
            self.__matrix[i,j] += inc            

    def IncItems(self,inc=1):
        
        for i in range(self.__nrows):
            for j in range(self.__ncolumns):
                self.IncItem(i,j,inc)

    def ModifyRow(self,i,f=lambda x:x):

        if self.CheckRow(i):

            for j in range(self.__ncolumns):
                newvalue = f( self.__matrix[i,j])
                self.__matrix[i,j] = newvalue

    def ModifyColumn(self,j,f=lambda x:x):

        if self.CheckColumn(j):

            for i in range(self.__nrows):
                newvalue = f( self.__matrix[i,j])
                self.__matrix[i,j] = newvalue

    def ReplaceRow(self,i,row):
        
        if self.CheckRow(i):
            if len(row) != self.__ncolumns:
                raise TypeError("\n\n   Invalid row length \n")
                return
            for j in range(len(row)):
                self.__matrix[i,j] = row[j]

    def ReplaceColumn(self,j,column):
        
        if self.CheckColumn(j):
            if len(column) != self.__nrows:
                raise TypeError("\n\n   Invalid column length \n")
                return
            for i in range(len(column)):
                self.__matrix[i,j] = column[i]



class Table:
    
    def __init__(self,row_headers=[],column_headers=[],value=0):

        self.__row_headers    = []
        self.__column_headers = []
        self.__Matrix = Matrix()

        nnewrows    = len( row_headers )
        nnewcolumns = len( column_headers )

        if nnewrows != 0 and nnewcolumns != 0 :

            self.__row_headers = row_headers
            self.__column_headers = column_headers
            self.__Matrix = Matrix( nnewrows , nnewcolumns , value )
        


    def Read(self, filename):

        file = open(filename)
        line=file.readline().split('\n')[0]
        self.__column_headers=[header.strip() for header in line.split()]

        i=0
        for line in file.readlines():

            words=line.split()
            self.__row_headers.append( words[0].strip() )
            list = [converttonum(word.strip()) for word in words[1:]]
            self.__Matrix.AppendRow(list)
            i+=1

        file.close()        


    """
    check methods
    """

    def CheckRowName(self,row):

        if row not in self.__row_headers:
           raise TypeError("\n\n    The specified one is not a valid row name\n")
           return 0
       
        return 1

    def CheckColumnName(self,column):

        if column not in self.__column_headers:
           raise TypeError("\n\n    The specified one is not a valid column name\n")
           return 0
       
        return 1


    def CheckNames(self,row,column):
        return self.CheckRowNane(row)*self.CheckColumnName(column)
    

    """
    read properties methods
    """

    def RowIndex(self,row):

        if isinstance(row,str):
           if self.CheckRowName(row):
              index = self.__row_headers.index(row)  
              return index
        else:
           if self.__Matrix.CheckRow(row):
              return row

    def RowName(self,row):

        if isinstance(row,str):
           if self.CheckRowName(row):
              return row
        else:
           if row<len(self.__row_headers):
              name = self.__row_headers[row] 
              return name

    def ColumnIndex(self,column):

        if isinstance(column,str):
           if self.CheckColumnName(column):
              index = self.__column_headers.index(column)
              return index
        else:
           if self.__Matrix.CheckColumn(column):
              return column
        
    def ColumnName(self,column):

        if isinstance(column,str):
           if self.CheckColumnName(column):
              return column
        else:
           if column<len(self.__column_headers):
              name = self.__column_headers[column] 
              return name


    def GetNRows(self):
        return len( self.__row_headers )
    
    def GetNColumns(self):
        return len( self.__column_headers )

    def GetMatrix(self):
        return self.__Matrix

    def GetItem(self,row,column):
        return self.__Matrix.GetItem( self.RowIndex(row), self.ColumnIndex(column) )

    def GetItems(self):
        return self.__Matrix.GetItems()

    def GetRow(self,row):

        if self.CheckRowName(row):
           r = {}
           r[row] = self.__Matrix.GetRow(self.RowIndex(row) )
           return r

    def GetRowValues(self,row):

        if self.CheckRowName(row):
           return self.__Matrix.GetRow( self.RowIndex(row) )    

    def GetColumn(self,column):

        if self.CheckColumnName(column):
           c = {}
           c[column] = self.__Matrix.GetColumn(self.ColumnIndex(column) )
           return c

    def GetColumnValues(self,column):

        if self.CheckColumnName(column):
           return self.__Matrix.GetColumn(self.ColumnIndex(column) )

    def GetRowHeaders(self):
        return [h for h in self.__row_headers]

    def GetColumnHeaders(self):
        return [h for h in self.__column_headers]

    def Copy(self):

        new_row_headers = [h for h in self.GetRowHeaders()]
        new_column_headers = [h for h in self.GetColumnHeaders() ] 
        newtable = Table( new_row_headers,new_column_headers)
        for i in range( self.__Matrix.GetNRows() ):
            for j in range( self.__Matrix.GetNColumns() ):
                  v = self.__Matrix.GetItem(i,j)
                  newtable.__Matrix.SetItem(i,j,v)
            
        return newtable

    def Print(self,just=0,prolog='',epilog=''):

        if isinstance(just,list)==1:
           justlist=just
        else:
            if just==0:
                
                values=map(str,self.GetItems())
                values += self.GetRowHeaders()
                values += self.GetColumnHeaders()
                just=max([len(val) for val in values])
                justlist=[just]*(self.GetNColumns()+1)

            elif just == -1:

               justlist=[]
               justlist.append( max([len(val) for val in self.GetRowHeaders() ]) )
               for j in self.GetColumnHeaders() :
                   col = self.GetColumn(j)
                   values = [ str(val) for val in col[j]]
                   values.append( j )
                   justlist.append( max([len(val) for val in values]) )
            
            else:
               justlist=[just]*(self.GetNColumns()+1)

        if prolog != '' : print prolog

        print ''.rjust(justlist[0]),
        for h in self.__column_headers:
            print h.rjust(justlist[ self.__column_headers.index(h)+1 ]),
        print
        for row in self.__row_headers:
            r = self.GetRow(row)
            print row.rjust(justlist[0]),
            k=1
            for v in r[row]:
                print str(v).rjust( justlist[k ] ),
                k+=1
            print
        
        if epilog != '' : print epilog

    
    """
    Modifying matrix structure methods
    """

    def AppendRow(self,name,value=0):

        self.__row_headers.append( name )
        
        nrows    = len( self.__row_headers )
        ncolumns = len( self.__column_headers )

        if ncolumns == 0 :
            pass
        
        if nrows == 1:
           self.__Matrix = Matrix( nrows, ncolumns, value )
        else:
           self.__Matrix.AppendRow( value ) 


    def AppendColumn(self,name,value=0):

        self.__column_headers.append(name)
        
        nrows    = len( self.__row_headers )
        ncolumns = len( self.__column_headers )

        if nrows == 0:
            pass
        
        if ncolumns == 1 :
           self.__Matrix = Matrix( nrows, ncolumns, value )
        else:
           self.__Matrix.AppendColumn( value ) 

        

    def InsertRow(self,i,name,value=0):

        if isinstance(name,dict):
           value = name.values()[0]
           name  = name.keys()[0] 

        if isinstance(value,list):
          if len(value) != len(self.__column_headers):
             raise TypeError("\n\n   Invalid list lenght \n")
             return
         
        self.__row_headers.insert(i,name)
        self.__Matrix.InsertRow(i,value)

    def InsertColumn(self,j,name,value=0):

        if isinstance(name,dict):
           value = name.values()[0]
           name  = name.keys()[0] 

        if isinstance(value,list):
          if len(value) != len(self.__row_headers):
             raise TypeError("\n\n   Invalid list lenght \n")
             return

        self.__column_headers.insert(j,name)
        self.__Matrix.InsertColumn(j,value)

    def DelRow(self,row):
        
        index = self.RowIndex(row)
        name  = self.RowName(row)
        
        self.__row_headers.remove(name)
        self.__Matrix.DelRow(index)

    def DelColumn(self,column):

        index = self.ColumnIndex(column)
        name  = self.ColumnName(column)
        
        self.__column_headers.remove(name)
        self.__Matrix.DelColumn(index)


    def SwapRows(self,row1,row2):

        index1 = self.RowIndex( row1 )
        index2 = self.RowIndex( row2 )

        name1 = self.RowName( row1 )
        name2 = self.RowName( row2 )
        
        self.__row_headers[index1] = name2
        self.__row_headers[index2] = name1

        self.__Matrix.SwapRows(index1,index2)

    def SwapColumns(self,column1,column2):

        index1 = self.ColumnIndex( column1 )
        index2 = self.ColumnIndex( column2 )

        name1 = self.ColumnName( column1 )
        name2 = self.ColumnName( column2 )
        
        self.__column_headers[index1] = name2
        self.__column_headers[index2] = name1

        self.__Matrix.SwapColumns(index1,index2)


                
    def SortRows(self, function=0):

        old_row_headers = [ i for i in self.__row_headers ]
        
        if function==0:
          self.__row_headers.sort()
        else:
          self.__row_headers.sort(function)

        newmatrix = Matrix(0,0)

        for name in self.__row_headers:
            oldindex = old_row_headers.index(name)
            row = self.__Matrix.GetRow(oldindex)
            newmatrix.AppendRow( row )
            
        self.__Matrix = newmatrix
                
    def SortColumns(self,function=0):

        old_column_headers = [ i for i in self.__column_headers ]
        
        if function == 0:
          self.__column_headers.sort()
        else:
          self.__column_headers.sort(function)

        newmatrix = Matrix(0,0)

        for name in self.__column_headers:
            oldindex = old_column_headers.index(name)
            column = self.__Matrix.GetColumn(oldindex)
            newmatrix.AppendColumn( column )
            
        self.__Matrix = newmatrix

 

    """
    modifying values methods
    """

    def SetItem(self,row,column,value=0):
        self.__Matrix.SetItem( self.RowIndex(row), self.ColumnIndex(column), value )

    def IncItem(self,row,column,inc=1):
        self.__Matrix.IncItem( self.RowIndex(row), self.ColumnIndex(column), inc )

 
    """
    read-only operations 
    """
       
    def OperationTable(self,table,f=lambda x,y:x+y, individualf1=lambda x:x, individualf2=lambda x:x):
    
        if isinstance(table,Table)!=1:
           raise TypeError("argument is not a Table object")
           return
            
        ###if self.Check(matrix.GetNRows(),matrix.GetNColumns()): 

        newtable = Table(self.__row_headers,self.__column_headers,0)
        
        for i in range(len(self.__row_headers)):
            for j in range(len(self.__column_headers)):
               op1 = individualf1(self.__Matrix.GetItem(i,j))
               op2 = individualf2(table.GetMatrix().GetItem(i,j))
               value = f(op1,op2) 
               newtable.__Matrix.SetItem(i,j,value)
     
        return newtable


class NTuple:

    def __init__(self,headers,nrows,value=0):
        
        self.__headers = headers
        self.__ncolumns = len(headers)
        
        self.__nrows   = nrows

        self.__rows = []

        if isinstance(value,list) != 1:        # if value is a list, it is inserted in the ntuple
            value=[value]*self.__ncolumns      # if value is a variable, a list is created, and inserted
            
        for row in range( self.__nrows ) :
            self.__rows.append( value )

        
    def Append(self,value):
 
        if isinstance(value,list) == 1:
            if len(value) != self.__ncolumns :
                print "\n error \n"
                sys.exit()
            else:
                self.__rows.append(value)
                self.__nrows += 1
                return

        elif isinstance(value,NTuple) == 1:

            rows = value.GetRows()
            for row in rows:
                self.__rows.append( row )
            self.__nrows += len(rows)

        else:

            value = [value]*self.__ncolumns
            self.__rows.append( value )
            self.__nrows += 1
            
    def GetHeaders(self):
  
        return self.__headers
                
    def GetRow(self,index):
        
        if isinstance(index,int) != 1:
            print '\n\n    the specified index is not an integer   \n\n'
            sys.exit()

        if index >= self.__nrows :
            print '\n\n    the specified index is not valid    \n\n'
            sys.exit()

        return self.__rows[index]


    def GetRows(self):
        # returns a list of lists
        
        return self.__rows

    def GetNRows(self):

        return len( self.__rows )


    def Print(self,just=5):

        for h in self.__headers:
            print h.rjust( just ),
        print
        
        for row in self.__rows:
            for item in row:
                print str(item).rjust( just ),
            print


# ------------------------------------------------------- #
#                                                         #
#    MySQL classes                                        #
#                                                         #
#    A set of classes and functions conceived to build    #
#    simple mysql queries in an easy way,                 #
#    and to query a available Data Base                   #
#                                                         #
# ------------------------------------------------------- #

class MySQL_Connection:

    def __init__(self):
        
        self.__user     = '' 
        self.__password = ''
        self.__socket   = ''
        self.__host     = ''
        self.__database = ''  

    def SetUser(self, value):
        self.__user = value

    def SetPassword(self, value):
        self.__password = value

    def SetSocket(self, value):
        self.__socket = value

    def SetHost(self, value):
        self.__host = value
    
    def SetDataBase(self, value):
        self.__database = value

    def GetUser(self):
        return self.__user

    def GetPassword(self):
        return self.__password

    def GetSocket(self):
        return self.__socket

    def GetHost(self):
        return self.__host

    def GetDataBase(self):
        return self.__database

    def Print(self):
        print
        print "user     : ", self.__user
        print "password : ", self.__password
        print "socket   : ", self.__socket
        print "host     : ", self.__host
        print "database : ", self.__database
        print


class MySQL_Query:

    def __init__(self):
        
        self.__user     = ''
        self.__password = ''
        self.__socket   = ''
        self.__host     = ''
        self.__database = ''

        self.__nonull = 0
        
        self.tables       = []
        self.columns      = []
        self.requirements = []
        self.joins        = [] 

        self.__query = ''

    def Copy(self):

        new = MySQL_Query()
        
        new.User(     self.__user     )
        new.Password( self.__password )
        new.Socket(   self.__socket   )
        new.Host(     self.__host     )
        new.DataBase( self.__database )

        new.tables       = self.tables
        new.requirements = self.requirements
        new.joins        = self.joins

        return new
        

    def User(self, value):
        self.__user = value

    def Password(self, value):
        self.__password = value

    def Socket(self, value):
        self.__socket = value

    def Host(self, value):
        self.__host = value

    def DataBase(self, value):
        self.__database = value

    def Connect(self, connect):
        
        self.__user     = connect.GetUser()
        self.__password = connect.GetPassword()
        self.__socket   = connect.GetSocket()
        self.__host     = connect.GetHost()
        self.__database = connect.GetDataBase()
        

    def EnableNoNullReq(self):
        self.__nonull = 1

    def DisableNoNullReq(self):
        self.__nonull = 0

    def AddTable(self, table):
        # table is a MySQL_Table object
        # returns the index in the list of this new Table object

        self.tables.append( table )

        return len(self.tables)-1

    def RemoveTable(self,table):
        # table is a MySQL_Table object

        self.tables.remove( table )

    def AddRequirement(self, req):
        # req is a MySQL_Req object
        # returns the index in the list of this new Requirement object
        
        self.requirements.append( req )
    
        return len(self.requirements)-1    

    def RemoveRequirement(self, req):
        # req is a MySQL_Req object

        self.requirements.remove( req )
    

    def AddJoin(self, join ):
        # join is a MySQL_Join object
        # returns the index in the list of this new Join object

        self.joins.append( join )

        return len(self.joins)-1

    def RemoveJoin(self, join ):
        # join is a MySQL_Join object

        self.joins.remove( join )

        
    def Query(self):

        #
        #  Checking that the variables has a no void value
        #
        if self.__user == '' :
            print '\n\n   User variable has not been set \n\n'
            sys.exit()
        if self.__password == '' :
            print '\n\n   Password variable has not been set \n\n'
            sys.exit()
        if self.__host == '' and self.__socket == '' :
            print '\n\n   Neither host nor socket variables have been set \n\n'
            sys.exit()
        if self.__database == '' :
            print '\n\n   Database variable has not been set \n\n'
            sys.exit()

        #
        #  Basic MySQL command
        #
        mysql_command = 'mysql '
        mysql_command += ' -u ' + self.__user
        mysql_command += ' -p'  + self.__password
        if self.__socket != '' :
            mysql_command += ' --socket=' + self.__socket
        if self.__host != '' :
            mysql_command += ' -h ' + self.__host
        mysql_command += ' -D ' + self.__database
        mysql_command += ' -s -e \"'

        self.columns = []
        for table in self.tables:
            for column in table.GetColumns():
               self.columns.append( column )

        
        if self.__nonull == 1:
           for column in self.columns:
              newreq = column.ISNOTNULL()
              self.requirements.append( newreq )

        Select = ' select '
        Select += reduce(lambda x,y: x+','+y , [ c() for c in  self.columns] )

        From = ' from '
        From += reduce(lambda x,y:x+' JOIN '+y,[t.GetNameAlias() for t in self.tables])
        
        if len(self.joins) != 0:
          On = ' on '
          On += reduce( lambda x,y: x+' and '+y, [j() for j in self.joins ] )
        else:
          On = ''

        if len(self.requirements) != 0:
          Where = ' where '
          Where += reduce(lambda x,y: x+' and '+y, [r() for r in self.requirements] )  
        else:
          Where = ''

        mysql_command += Select
        mysql_command += From
        mysql_command += On
        mysql_command += Where
        mysql_command += '\"'

        self.__query = mysql_command

        return 1

    def Execute(self):
       out = commands.getoutput( self.__query + ' 2> /dev/null' ).split('\n')
       # stderr is redirected to /dev/null

       column_headers = [x() for x in self.columns]

       ntuple = NTuple( column_headers, 0 )

       if out != [''] :
           for line in out:
                words = line.split('\t')
                ntuple.Append(words)

       return ntuple
        
    def __call__(self): return self.__query

    def __str__(self) : return self.__query


class MySQL_Table:

    def __init__(self, tablename):
        
        self.table = tablename
        self.alias = '_'+tablename+'_'

        self.columns = []

    def __call__(self):
        
        return self.table

    def Alias(self):
        return self.alias

    def GetNameAlias(self):
        #returns a string like "JOB _JOB_"

        return self.table+' '+self.alias

    def AddColumn(self, column):

        newcolumn = MySQL_Column( self, column )
        self.columns.append( newcolumn )
        return newcolumn

    def GetColumns(self):
        return self.columns

class MySQL_Column:

     def __init__(self,table,column):

        self.table = table
        self.name = column
        self.variable = table.Alias()+'.'+column
        self.requirement = MySQL_Req('')

     def __call__(self):
         return self.variable

     def GetRequirement(self):
         return self.requirement


     def GT(self, value):
        self.requirement = MySQL_Req( self.variable + ' > \''        + value + '\'' )
        return self.requirement

     def GE(self, value):
        self.requirement = MySQL_Req( self.variable + ' >= \''       + value + '\'' )
        return self.requirement 
            
     def LT(self, value):
        self.requirement = MySQL_Req( self.variable + ' < \''        + value + '\'' )
        return self.requirement 

     def LE(self, value):
        self.requirement = MySQL_Req( self.variable + ' <= \''       + value + '\'' )
        return self.requirement 

     def EQ(self, value):
        self.requirement = MySQL_Req( self.variable + ' = \''        + value + '\'' )
        return self.requirement 

     def NE(self, value):
        self.requirement = MySQL_Req( self.variable + ' != \''       + value + '\'' )
        return self.requirement 

     def IS(self, value):
        self.requirement = MySQL_Req( self.variable + ' is \''       + value + '\'' )
        return self.requirement 

     def ISNOT(self, value):
        self.requirement = MySQL_Req( self.variable + ' is not \''   + value + '\'' )
        return self.requirement 

     def ISNULL(self):
        self.requirement = MySQL_Req( self.variable + ' is NULL' )
        return self.requirement 

     def ISNOTNULL(self):
        self.requirement = MySQL_Req( self.variable + ' is not NULL' )
        return self.requirement 

     def LIKE(self, value):
        self.requirement = MySQL_Req( self.variable + ' like \''     + value + '\'' )
        return self.requirement 

     def NOTLIKE(self, value):
        self.requirement = MySQL_Req( self.variable + ' not like \'' + value + '\'' )
        return self.requirement 

     def Modify(self, value1, value2):
        """
        to modify the select statement:
    
        e.g. select N/1024 instead of select N
        
        """
        old = self.variable
        self.variable = value1 + old + value2


class MySQL_Req:
    """
    MySQL query requirement on a specified TABLE/COLUMN
    """

    def __init__(self, req):
        self.req = '('+req+')'
    
    def __call__(self):
        """
        returns the complete requirement
        """
        return self.req

    
    def AND(self, *newreqs):
        """
        creates a new MySQL_Req object
        from the current one plus a list of new MySQL_Req objects,
        implementing the AND operation,
        and returns it
        """

        req = self.req
        
        for newreq in newreqs:
            req += ' and ' + newreq()

        return MySQL_Req(req)


    def OR(self, *newreqs):
        """
        creates a new MySQL_Req object
        from the current one plus a list of new MySQL_Req objects,
        implementing the AND operation,
        and returns it
        """

        req = self.req
        
        for newreq in newreqs:
            req += ' or ' + newreq()

        return MySQL_Req(req)
        

class MySQL_Join:

    def __init__(self, column1, column2):
       """
       column1 and column2 must be MySQL_Column objects
       """
       
       self.__column1 = column1
       self.__column2 = column2

    def __call__(self):

       join = self.__column1() + '=' + self.__column2()
       return join 
        

def Req_AND( *reqs ):
    """
    create a new requirement object from a list of requirements
    """

    newreq = reqs[0]()
    for req in reqs[1:]:
        newreq += ' and '+req()

    return MySQL_Req( newreq )

def Req_OR( *reqs ):
    """
    create a new requirement object from a list of requirements
    """

    newreq = reqs[0]()
    for req in reqs[1:]:
        newreq += ' or '+req()

    return MySQL_Req( newreq )


# ------------------------------------------------------- #
#                                                         #
#    Classes for the arguements parsing                   #
#    including tools to read data cards in XML format     #
#                                                         #
# ------------------------------------------------------- #

class Params:
    """
    class containing the needed parameters to perform
    a complete atomic query to a DataBase.
    
      - Connections is a list of MySQL_Connection objects
      - Workflows is a list of Workflows
      - Sites is al list of Sites
      - From is the initial date
      - Until is the final date
      - Last is used to perform a query only for the last N seconds
      - Type is status or codes
      - Merge is yes or no
      - Verbose options: 1 is default,
                         2 display the efficiencies
                         3 is for debugging
      - URL is a html file with the output 
      - Plugin is the BOSS scheduler used
    """
    
    def __init__(self):

        self.Connections = []  
        self.Workflows   = []
        self.Sites       = []
        
        self.From  = ""
        self.Until = ""
        self.Last  = ""

        self.Classify   = ""
        self.Type       = ""
        self.Merge      = ""
        
        self.Verbose  = ""
        self.URL      = ""

        self.Plugin   = ""

    def Copy( self ):

        newparams = Params()

        newparams.Connections = self.Connections
        newparams.Workflows   = self.Workflows
        newparams.Sites       = self.Sites

        newparams.From        = self.From
        newparams.Until       = self.Until
        newparams.Last        = self.Last

        newparams.Classify    = self.Classify
        newparams.Type        = self.Type
        newparams.Merge       = self.Merge

        newparams.Verbose     = self.Verbose
        newparams.URL         = self.URL
        
        newparams.Plugin      = self.Plugin        

        return newparams

    def Print(self):

        print "Connection parameters: "
        for c in self.Connections:
            c.Print()

        print
        print "Workflows : "
        for w in self.Workflows:
            print w

        print
        print "Sites : "
        for s in self.Sites:
            print s

        print
        print "Time Options: "
        print "From    : ", self.From
        print "Until   : ", self.Until
        print "Last    : ", self.Last

        print
        print "Query options : "
        print "Classify  : ", self.Classify
        print "Type      : ", self.Type
        print "Merge     : ", self.Merge
        print "Plugin    : ", self.Plugin
        print
        print "Print options : "
        print "Verbose : ", self.Verbose
        print "URL     : ", self.URL  

        print
        

    def Active(self):

        if len( self.Connections ) != 0  : return 1
        if len( self.Workflows )   != 0  : return 1
        if len( self.Sites )       != 0  : return 1
        if self.From               != "" : return 1
        if self.Until              != "" : return 1
        if self.Last               != "" : return 1
        if self.Classify           != "" : return 1
        if self.Type               != "" : return 1
        if self.Merge              != "" : return 1
        if self.Verbose            != "" : return 1
        if self.URL                != "" : return 1
        if self.Plugin             != "" : return 1

        return 0

    def Update(self, refparams ) :

        #
        #  A method to create an updated version of the current object
        #  taking into account the content of a reference object (refparams)
        #
        #  The rules are:
        #         - Connections, Workflows and Sites values are added
        #         - The rest of values are overriden
        #

        newparams = Params()
        
        #  ---  Connections
        for connection in self.Connections:
            newparams.Connections.append(connection)
        for connection in refparams.Connections:
            newparams.Connections.append(connection)
        
        #  ---  Workflows
        for workflow in self.Workflows:
            newparams.Workflows.append(workflow)
        for workflow in refparams.Workflows:
            newparams.Workflows.append(workflow)
            
        #  ---  Sites
        for site in self.Sites:
            newparams.Sites.append(site)
        for site in refparams.Sites:
            newparams.Sites.append(site)

        #  ---  Time options
        if refparams.From != "":
            newparams.From = refparams.From
        else:
            newparams.From = self.From

        if refparams.Until != "":
            newparams.Until = refparams.Until
        else:
            newparams.Until = self.Until

        if refparams.Last != "":
            newparams.Last = refparams.Last
        else:
            newparams.Last = self.Last

        #  ---  Classify
        if refparams.Classify != "":
            newparams.Classify = refparams.Classify
        else:
            newparams.Classify = self.Classify

        #  ---  Type 
        if refparams.Type != "":
            newparams.Type = refparams.Type
        else:
            newparams.Type = self.Type

        #  ---  Merge
        if refparams.Merge != "":
            newparams.Merge = refparams.Merge
        else:
            newparams.Merge = self.Merge

        #  ---  Verbose
        if refparams.Verbose != "":
            newparams.Verbose = refparams.Verbose
        else:
            newparams.Verbose = self.Verbose

        #  --- URL
        if refparams.URL != "":
            newparams.URL = refparams.URL
        else:
            newparams.URL = self.URL

        #  --- Plugin
        if refparams.Plugin != "":
            newparams.Plugin = refparams.Plugin
        else:
            newparams.Plugin = self.Plugin

            
        return newparams

    def Duplicate( self ) :

        if self.Merge == "both":

           params1 = self.Copy()
           params2 = self.Copy()

           params1.Merge = "no"
           params2.Merge = "yes"

           return params1, params2

        else:
           return self



class ProdmonXMLParser(saxutils.DefaultHandler):
    """
    class to parser XML files containing the arguments
    """
    
    def __init__(self):

        self.List_of_params = []
        
        self.in_connection = 0
        self.in_default = 1

        self.default = Params()
        self.tmp     = Params()

        self.tmp = self.default

    def startElement(self, name, attrs):

        value = attrs.get('value',None)
        
        if self.in_connection == 1:
            
            if name == 'user'     :
                self.C.SetUser    ( value )
            if name == 'password' :
                self.C.SetPassword( value )
            if name == 'socket'   :
                self.C.SetSocket  ( value )
            if name == 'host'     :
                self.C.SetHost    ( value )
            if name == 'database' :
                self.C.SetDataBase( value )

            
        if name == 'connection':
            self.in_connection = 1
            self.C = MySQL_Connection()

        if name == 'workflow':
            self.tmp.Workflows.append( value )
        if name == 'site':
            self.tmp.Sites.append    ( value )

        if name == 'from':
            self.tmp.From    = value
        if name == 'until':
            self.tmp.Until   = value
        if name == 'last':
            self.tmp.Last    = value

        if name == 'classify':
            self.tmp.Classify = value
        if name == 'type':
            self.tmp.Type     = value
        if name == 'merge':
            self.tmp.Merge    = value
            
        if name == 'verbose':
            self.tmp.Verbose = value
        if name == 'url':
            os.system( "rm -f "+value )
            self.tmp.URL     = value

        if name == 'plugin':
            self.tmp.Plugin    = value


        if name == 'query' :
  
            self.in_default = 0
            self.P = Params()
            self.tmp = self.P
    

    def endElement(self,name):
  
        if name == 'connection':
            self.in_connection = 0
            self.tmp.Connections.append( self.C )

        if name == 'query' :
            
            self.in_default = 1
            if self.P.Active() == 1:
              self.List_of_params.append( self.P ) 
            self.tmp = self.default

        if name == 'options' :
           pass


def ParseXMLFile(XMLFile):
    """
    Function to parse an XML File
    Returns a list of Params objects ( List_of_arguments )
    """

    # Create a parser
    parser = make_parser()

    # Tell the parser we are not interested in XML namespaces
    parser.setFeature(feature_namespaces, 0)

    # Create the handler
    xmlparser = ProdmonXMLParser() 

    # Tell the parser to use our handler
    parser.setContentHandler(xmlparser)

    # Parse the input
    parser.parse( XMLFile )

    return xmlparser.default, xmlparser.List_of_params

def Usage():

    manfile = open( "/tmp/manfile", "w" )

    manfile.write("=head1 USAGE                                                                                                                  \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("B<PAmonBOSSDB.py>                                                                                                             \n")
    manfile.write("B<--user>=I<DB_user_ID_1> B<--password>=I<ID_passwd_1> B<--socket>=I<socket_1>|B<--host>=I<DB_host_1> B<--database>=I<DB_1>   \n")
    manfile.write("[B<--user>=I<DB_user_ID_2> B<--password>=I<ID_passwd_2> B<--socket>=I<socket_2>|B<--host>=I<DB_host_2> B<--database>=I<DB_2> ... \n")
    manfile.write("B<--user>=I<DB_user_ID_N> B<--password>=I<ID_passwd_N> B<--socket>=I<socket_N>|B<--host>=I<DB_host_N> B<--database>=I<DB_N>]  \n")
    manfile.write("[B<--classify>=sites|workflows] [B<--type>=status|codes] [B<--merge>=yes|no|both]                                             \n")
    manfile.write("[B<--workflow>=I<workflow1> ...  B<--workflow>=I<workflowN>]                                                                  \n")
    manfile.write("[B<--site>=I<site1> ...  B<--site>=I<siteN>]                                                                                  \n") 
    manfile.write("[B<--from>=I<initial_time> [B<--until>=I<final_time> ] ] [B<--last>=I<period> ]                                               \n")
    manfile.write("[B<--verbose>=I<verbose-level>]                                                                                               \n")
    manfile.write("[B<--url>=I<filename>]                                                                                                        \n")
    manfile.write("[B<--cfg-file>=I<filename>]                                                                                                   \n")
    manfile.write("[B<--plugin>=I<name of the BOSS plugin scheduler>]                           \n")
    manfile.write("[B<--help>]                                                                                                                   \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("=head1 DESCRIPTION                                                                                                            \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("PAmonBOSSDB.py returns job status, return codes and processed number of events extracting this information                    \n")
    manfile.write("from the ProdAgent BOSS database.                                                                                             \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("This information can be classified either as a function of the sites running production jobs or                               \n")
    manfile.write("as a function of the workflows.                                                                                               \n")
    manfile.write("B<classify> argument is used to select the type of information,                                                               \n")
    manfile.write("using the I<sites> value (default) for the former or I<workflows> for the latter option.                                      \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("B<type> argument is needed to select the kind of information to be presented.                                                 \n")
    manfile.write("With the I<status> value (default),                                                                                           \n")
    manfile.write("the number of running, scheduled, aborted, cancelled, waiting and submitted jobs is presented.                                \n")
    manfile.write("For the already finished jobs, the total number is splitted in successful and failed jobs.                                    \n")
    manfile.write("The total number of processed events is shown also.                                                                           \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("With the I<codes> value a table with the number of jobs finished reporting the different Return Codes is displayed.           \n")
    manfile.write("An aditional column showing the total number of jobs with RC different from 0 is included.                                    \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("B<merge> argument is needed to select the kind of jobs. Possible values are I<no> (default), I<yes> or I<both>.               \n")
    manfile.write("When the I<both> value is set, the query is duplicated into two queries, with I<yes> and I<no> values respectively,           \n")
    manfile.write("and inheriting the rest of the options.                                                                                       \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("B<plugin> argument is needed to select the kind of BOSS scheduler plugin used. Possible values are I<edg> (default), I<gliteCollection>,  I<gliteParam>.               \n")
    manfile.write("=head1 OPTIONS                                                                                                                \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("=over 8                                                                                                                       \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("=item B<DataBases>:                                                                                                           \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("Connection parameters for the BOSS database (username, password, hostname and database name) must be specified through        \n")
    manfile.write("the options B<--user>, B<--password>, B<--socket> or  B<--host>, and B<--database>,                                           \n")
    manfile.write("always in this order.                                                                                                         \n")
    manfile.write("B<--socket> and B<--host> arguments are both optional. However, at least one of them has to be specified each time.           \n")
    manfile.write("Several databases can be queried simultaneously by simply giving the corresponding sets of parameters                         \n")
    manfile.write("repeating the previous options.                                                                                               \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("=item B<Workflows>:                                                                                                           \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("String matching workflow name(s). Aggregated results for all matching workflows will be shown.                                \n")
    manfile.write("One or more B<--workflow> options can be specified.                                                                           \n")      
    manfile.write("                                                                                                                              \n")
    manfile.write("=item B<Sites>:                                                                                                               \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("Computing Element name.                                                                                                       \n")
    manfile.write("One or more B<--site> options can be specified.                                                                               \n")     
    manfile.write("                                                                                                                              \n")
    manfile.write("=item B<Time>:                                                                                                                \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("The query can be restricted to a given period in time.                                                                        \n")
    manfile.write("The initial and final dates can be specified with the  B<--from>=I<date> and B<--until>=I<date> options,respectively          \n")
    manfile.write("In both cases, the arguments are expressed in seconds since epoch.                                                            \n")
    manfile.write("If the final date is not specified, the query extends until the current date.                                                 \n") 
    manfile.write("                                                                                                                              \n")
    manfile.write("If the B<--last>=I<N> option is used, only those jobs which either were submitted or have finished within the last N seconds  \n")
    manfile.write("are taken into account                                                                                                        \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("=item B<Print>:                                                                                                               \n")
    manfile.write("                                                                                                                              \n")    
    manfile.write("Several verbose options are available through the B<--verbose> option:                                                        \n")
    manfile.write(" 1 is default value.                                                                                                          \n")
    manfile.write(" 2 returns different efficiencies if the I<codes> option is also specified.                                                   \n")
    manfile.write(" 3 is for debugging purposes.                                                                                                 \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("=item B<Config file>:                                                                                                         \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("All the options can be specified via a configuration file, written in XML format:                                             \n")
    manfile.write(" The root tag is <I<options>>.                                                                                                \n")
    manfile.write(" The parameters to access to the DataBases are set inside <I<connection>>...<I</connection>> blocks,(one for each DB)         \n")
    manfile.write(" containing the <I<user/>>, <I<password/>>, <I<host/>>, <I<sockeet/>>, and <I<database/>> elements.                           \n")
    manfile.write(" The workflow names, sites, classify, type, merge, time, and verbose options can be set with their corresponding elements.    \n")
    manfile.write(" For all cases, the attribute to specify the desired option is named C<value>.                                                \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("Several queries can be requested by simply enclosing the corresponding options between <I<query>>...<I<query/>> blocks.       \n")
    manfile.write("The one value options override the variables specified outside any <I<query>>...<I<query/>> pair, which are taken as global.  \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("XML file and command line options can be combined following the above described rules.                                        \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("=item B<help>:                                                                                                                \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("With B<--help> option this page is shown, and exits.                                                                          \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("contact: Jose.Hernandez@ciemat.es, Jose.Caballero@cern.ch                                                                     \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("=head1 AUTHORS                                                                                                                \n")
    manfile.write("                                                                                                                              \n")
    manfile.write("Jose Hernandez, Jose Caballero (CIEMAT)                                                                                       \n")

    manfile.close()

    os.system( "pod2man --center=\" \" --release=\" \"  /tmp/manfile | nroff -man | less" )
    os.system( "rm /tmp/manfile" )

    sys.exit()
    
def WarningMessage():
    
        print
        print " WARNING:"
        print 
        print " PAmonBOSSDB.py has been called without arguments."
        print " Several options are mandatory."
        print ""
        print " USAGE:"
        print 
        print " PAmonBOSSDB.py",
        print " --user=DB_user_D_1 --password=D_passwd_1 --host=DB_host_1 --database=DB_1",
        print "[--user=DB_user_D_2 --password=D_passwd_2 --host=DB_host_2 --database=DB_2 ... ",
        print "--user=DB_user_D_N --password=D_passwd_N --host=DB_host_N --database=DB_N]",
        print "[--type=status|codes] [--merge=yes|no] [--workflow=workflow1 ...  --workflow=workflowN]",
        print "[--site=site1 ...  --site=siteN]", 
        print "[--from=initial_time [--until=final_time]] [--last=period]",
        print "[--verbose=verbose-level]",
        print "[--url=filename]",
        print "[--cfg-file=filename]",
        print "[--help]"
        print
        print " A detailed description of the possible options can be found in the PAmonBOSSDB-HowTo.pdf document, or typing ./PAmonBOSSDB.py --help"
        print    

def ParseArguments():
    """
    Function to parse the arguments:

      - from the command line
      - from a XML file

    List_of_params is a list of Params objects
    """

    List_of_params = []

    default       = Params()
    shell_params  = Params()
       
    valid = [                    \
              'help'       ,     \
              'user='      ,     \
              'password='  ,     \
              'socket='    ,     \
              'host='      ,     \
              'database='  ,     \
              'classify='  ,     \
              'type='      ,     \
              'merge='     ,     \
              'workflow='  ,     \
              'site='      ,     \
              'from='      ,     \
              'until='     ,     \
              'last='      ,     \
              'cfg-file='  ,     \
              'verbose='   ,     \
              'plugin='    ,     \
              'url='             \
              ]
    
    opts, args = getopt.getopt(sys.argv[1:], "", valid)

    if len(opts) == 0 :
        WarningMessage()
 
    
    for opt, arg in opts:
       if opt == "--help":
           Usage()

       if opt == "--cfg-file":
           # ---------------------------------------- #
           #   Arguments are given in a XML file      #
           # ---------------------------------------- #

           XMLFile = arg
           default, List_of_params = ParseXMLFile(XMLFile)
           

       # ---------------------------------------- #
       #   Parsing arguments given from           #
       #   the command line                       #
       # ---------------------------------------- #
       
       #
       #  Connections arguments: user, password, host and database
       #
       if opt == "--user":
            Connection = MySQL_Connection()
            user = arg
            Connection.SetUser( user )
       if opt == "--password":
            password = arg
            Connection.SetPassword( password )
       if opt == "--socket":
            socket = arg
            Connection.SetSocket( socket )
       if opt == "--host":
            host = arg
            Connection.SetHost( host )
       if opt == "--database":
            database = arg
            Connection.SetDataBase( database )
            
            shell_params.Connections.append( Connection )

       if opt == "--classify":
          classify = arg
          if classify != 'sites' and classify != 'workflows' :
              Usage()
          shell_params.Classify = classify
       
       if opt == "--type":
          type = arg
          if type != 'status' and type != 'codes' :
              Usage()
          shell_params.Type = type
          
       if opt == "--merge":
          merge = arg
          if merge != 'yes' and merge != 'no' and merge != "both":
              Usage()
          shell_params.Merge = merge

       if opt == "--plugin":
          plugin= arg
          if plugin != 'edg' and plugin != 'gliteCollection' and plugin != 'gliteParam' :
              Usage()
          shell_params.Plugin= plugin

       if opt == "--workflow":
           shell_params.Workflows.append( arg )
           
       if opt == "--site":
           shell_params.Sites.append( arg )

       if opt == "--from":
           shell_params.From = arg

       if opt == "--until":
           shell_params.Until = arg
           
       if opt == "--last":
           shell_params.Last = arg

       if opt == "--verbose":
           shell_params.Verbose = arg
           
       if opt == "--url":
           os.system( "rm -f "+arg )
           shell_params.URL = arg
           

    #
    #   Merging options given from command line and/or XML file
    #

    if shell_params.Active() == 1 :
        if len( List_of_params ) != 0 :
            
              tmp_list_of_params = []
              for param in List_of_params:
                  tmp_list_of_params.append( param.Update(shell_params) )

              List_of_params = tmp_list_of_params

        else :
              List_of_params.append( shell_params )

    if len( List_of_params ) != 0 :
        if default.Active() == 1 :
            tmp_list_of_params = []
            for param in List_of_params:
                tmp_list_of_params.append(  default.Update(param)  )

            List_of_params = tmp_list_of_params
            
    else :
        List_of_params.append( default )

    #
    #  Searching for params with merge="both"
    #

    tmp_list_of_params=[]
    for param in List_of_params:
        if param.Merge == "both" :
            pair = param.Duplicate()
            tmp_list_of_params.append( pair[0] )
            tmp_list_of_params.append( pair[1] )
        else:
            tmp_list_of_params.append( param )

    List_of_params = tmp_list_of_params
        
    return List_of_params

# ------------------------------------------------------- #
#                                                         #
#   Creation of the query matching the input arguments    #
#                                                         #
# ------------------------------------------------------- #


def SingleQuery( params, i):
   # params is a Params object
   # i is the index with the Connection specifications

   workflows = []
   sites     = []
   
   classify = 'sites'
   type     = 'status'
   merge    = 'no'
   
   itime   = ' '
   ftime   = ' '
   
   verbose = '1'
   url     = ''

   if params.From != "" :
          itime = params.From
          
   if params.Until != "" :
          ftime = params.Until
          
   if params.Last != "" :
          nseconds = int( params.Last )
          ftime = int( time.time( ) )
          itime = ftime - nseconds
          ftime = str(ftime)
          itime = str(itime)       
            
   if ( itime == ' ' and ftime != ' ' ) :
       print "initial time not specified"
       sys.exit( -1 )

   if ( itime != ' ' and ftime == ' ' ) :
       ftime = str(int( time.time( ) ) )
   
   if len( params.Workflows ) != 0 :
      workflows = params.Workflows
      
   if len( params.Sites ) != 0 :
      sites = params.Sites

   if params.Classify != "" :
      classify = params.Classify 
   
   if params.Type != "" :
      type = params.Type
      
   if params.Merge != "" :
      merge = params.Merge

   if params.Verbose != "" :
      verbose = params.Verbose
      
   if params.URL != "" :
      url = params.URL

   if params.Plugin != "" :
      plugin = params.Plugin
   else: 
      plugin = 'edg'

   # --------------------------------------------------------------------------- #
   #      QUERY OBJECTS : CREATON AND ATTRBUTES                                  #
   # --------------------------------------------------------------------------- #
 
   q       = MySQL_Query()
   q_ended = MySQL_Query()

   q.User(     params.Connections[i].GetUser()     )
   q.Password( params.Connections[i].GetPassword() )
   q.Socket(   params.Connections[i].GetSocket()   )
   q.Host(     params.Connections[i].GetHost()     )
   q.DataBase( params.Connections[i].GetDataBase() )

   q_ended.User(     params.Connections[i].GetUser()     )
   q_ended.Password( params.Connections[i].GetPassword() )
   q_ended.Socket(   params.Connections[i].GetSocket()   )
   q_ended.Host(     params.Connections[i].GetHost()     )
   q_ended.DataBase( params.Connections[i].GetDataBase() )

   # ------------------------------
   #  usual tables
   # ------------------------------
 
   t_cmssw           = MySQL_Table( 'cmssw'           )

   SCHED_plugin="SCHED_%s"%plugin
   t_sched_edg = MySQL_Table( SCHED_plugin )
   t_job             = MySQL_Table( 'JOB'             )     
 
   t_ended_cmssw     = MySQL_Table( 'ENDED_cmssw'     )
   ENDED_SCHED_plugin="ENDED_SCHED_%s"%plugin
   t_ended_sched_edg = MySQL_Table( ENDED_SCHED_plugin)
   t_ended_job       = MySQL_Table( 'ENDED_JOB'       )
 
   t_chain           = MySQL_Table( 'CHAIN'            )
 
   # ------------------------------
   #  usual id's columns
   # ------------------------------
 
   c_cmssw_id                = MySQL_Column( t_cmssw,           'ID'      )
   c_cmssw_task_id           = MySQL_Column( t_cmssw,           'TASK_ID' )
   c_cmssw_chain_id          = MySQL_Column( t_cmssw,           'CHAIN_ID' )
   c_ended_cmssw_id          = MySQL_Column( t_ended_cmssw,     'ID'      )
   c_ended_cmssw_task_id     = MySQL_Column( t_ended_cmssw,     'TASK_ID' )
   c_ended_cmssw_chain_id     = MySQL_Column( t_ended_cmssw,     'CHAIN_ID' )
                                                                           
   c_sched_edg_id            = MySQL_Column( t_sched_edg,       'ID'      )
   c_sched_edg_task_id       = MySQL_Column( t_sched_edg,       'TASK_ID' )
   c_sched_edg_chain_id      = MySQL_Column( t_sched_edg,      'CHAIN_ID')
   c_ended_sched_edg_id      = MySQL_Column( t_ended_sched_edg, 'ID'      )
   c_ended_sched_edg_task_id = MySQL_Column( t_ended_sched_edg, 'TASK_ID' )
   c_ended_sched_edg_chain_id = MySQL_Column( t_ended_sched_edg, 'CHAIN_ID')
 
   c_job_id                  = MySQL_Column( t_job,             'ID'      )
   c_job_task_id             = MySQL_Column( t_job,             'TASK_ID' )
   c_job_chain_id            = MySQL_Column( t_job,             'CHAIN_ID')
   c_ended_job_id            = MySQL_Column( t_ended_job,       'ID'      )
   c_ended_job_task_id       = MySQL_Column( t_ended_job,       'TASK_ID' )
   c_ended_job_chain_id      = MySQL_Column( t_ended_job,       'CHAIN_ID')

   c_chain_id                = MySQL_Column( t_chain,           'TASK_ID' )  

   # ------------------------------
   #  other usual columns
   # ------------------------------
 
   c_job_stop_t              = MySQL_Column( t_job,       'STOP_T'    )
   c_job_sub_t               = MySQL_Column( t_job,       'SUB_T'     )
   c_ended_job_stop_t        = MySQL_Column( t_ended_job, 'STOP_T'    )
   c_ended_job_sub_t         = MySQL_Column( t_ended_job, 'SUB_T'     )
 
   c_chain_name              = MySQL_Column( t_chain,     'NAME' ) 

   # ------------------------------
   #  usual required columns
   # ------------------------------

   c__chain_name             = t_chain.AddColumn( 'NAME' ) 

   c_task_exit               = t_cmssw.AddColumn(           'TASK_EXIT' )
   c_ended_task_exit         = t_ended_cmssw.AddColumn(     'TASK_EXIT' )
 
   c_sched_dest_ce           = t_sched_edg.AddColumn(       'DEST_CE'   )
   c_ended_sched_dest_ce     = t_ended_sched_edg.AddColumn( 'DEST_CE'   )
 
   # ------------------------------
   #  complete query creation: TYPE
   # ------------------------------
 
   q.AddTable( t_chain      )
   q.AddTable( t_cmssw     )
   q.AddTable( t_sched_edg )

 
   q.AddJoin( MySQL_Join( c_cmssw_id,      c_sched_edg_id      ) )
   q.AddJoin( MySQL_Join( c_cmssw_task_id, c_sched_edg_task_id ) )
   q.AddJoin( MySQL_Join( c_job_chain_id,       c_sched_edg_chain_id ) )
   q.AddJoin( MySQL_Join( c_cmssw_chain_id,       c_sched_edg_chain_id ) )
   q.AddJoin( MySQL_Join( c_chain_id,       c_sched_edg_task_id ) )
   if plugin != 'edg':
      q.AddJoin( MySQL_Join( c_chain_id,       c_sched_edg_chain_id ) )

   q_ended.AddTable( t_chain            )
   q_ended.AddTable( t_ended_cmssw     )
   q_ended.AddTable( t_ended_sched_edg )

 
   q_ended.AddJoin( MySQL_Join( c_ended_cmssw_id,      c_ended_sched_edg_id      ) )
   q_ended.AddJoin( MySQL_Join( c_ended_cmssw_task_id, c_ended_sched_edg_task_id ) )
   q_ended.AddJoin( MySQL_Join( c_ended_job_chain_id,      c_ended_sched_edg_chain_id ) )
   q_ended.AddJoin( MySQL_Join( c_ended_cmssw_chain_id,      c_ended_sched_edg_chain_id ) )
   q_ended.AddJoin( MySQL_Join( c_chain_id,             c_ended_sched_edg_task_id ) )
   if plugin != 'edg':
      q_ended.AddJoin( MySQL_Join( c_chain_id,             c_ended_sched_edg_chain_id ) )

   if type == 'codes':
      
      q.AddTable(       t_job       )
      q_ended.AddTable( t_ended_job )
      
      q.AddJoin( MySQL_Join(       c_cmssw_id,            c_job_id            ) )
      q.AddJoin( MySQL_Join(       c_cmssw_task_id,       c_job_task_id       ) )
      
      q_ended.AddJoin( MySQL_Join( c_ended_cmssw_id,      c_ended_job_id      ) )
      q_ended.AddJoin( MySQL_Join( c_ended_cmssw_task_id, c_ended_job_task_id ) )

 
   elif type == 'status' :
 
      # ------------------------------
      #  specific required columns
      # ------------------------------
 
      c_cmssw_nevt         = t_cmssw.AddColumn(       'N_EVT' )
      c_ended_cmssw_nevt   = t_ended_cmssw.AddColumn( 'N_EVT' )
      
      c_sched_status       = t_sched_edg.AddColumn(       'SCHED_STATUS' )  
      c_ended_sched_status = t_ended_sched_edg.AddColumn( 'SCHED_STATUS' )

      c_sched_id       = t_job.AddColumn(       'SCHED_ID' )
      c_ended_sched_id = t_ended_job.AddColumn( 'SCHED_ID' )
 
      q.AddTable(       t_job       )
      q_ended.AddTable( t_ended_job )
       
      q.AddJoin( MySQL_Join(       c_cmssw_id,            c_job_id            ) )
      q.AddJoin( MySQL_Join(       c_cmssw_task_id,       c_job_task_id       ) )
      q_ended.AddJoin( MySQL_Join( c_ended_cmssw_id,      c_ended_job_id      ) )
      q_ended.AddJoin( MySQL_Join( c_ended_cmssw_task_id, c_ended_job_task_id ) )

  
   # ------------------------------
   #  parsing the rest of arguments
   # ------------------------------
 
   if len( workflows ) > 0 :
 
      reqs = []
      for workflow in workflows :
         reqs.append( c_chain_name.LIKE( '%'+workflow+'%' ) )
 
      q.AddRequirement(       Req_OR( *reqs ) )
      q_ended.AddRequirement( Req_OR( *reqs ) )
 
      q.AddJoin(       MySQL_Join( c_chain_id, c_cmssw_task_id       ) )
      q_ended.AddJoin( MySQL_Join( c_chain_id, c_ended_cmssw_task_id ) )
 


 
   if len( sites ) > 0 :
     
      reqs       = []
      reqs_ended = []
      
      for site in sites:
 
         reqs.append(       c_sched_dest_ce.LIKE( '%'+site+'%'       ) )
         reqs_ended.append( c_ended_sched_dest_ce.LIKE( '%'+site+'%' ) )
 
      q.AddRequirement(       Req_OR( *reqs       )  )
      q_ended.AddRequirement( Req_OR( *reqs_ended )  )
 
 
   if ( itime != ' ' ) :

       if type == 'codes':
           
          #
          #  Time conditions:
          #     either the job must have been submitted after itime and before ftime
          #     or it has finished after itime and before ftime
          #
          req_sub_time  = Req_AND( c_job_sub_t.GT(  itime ), c_job_sub_t.LT(  ftime) )
          req_stop_time = Req_AND( c_job_stop_t.GT( itime ), c_job_stop_t.LT( ftime) )
          req = Req_OR( req_sub_time, req_stop_time )
          q.AddRequirement( req )
          
          req_ended_sub_time  = Req_AND( c_ended_job_sub_t.GT(  itime ), c_ended_job_sub_t.LT(  ftime) )
          req_ended_stop_time = Req_AND( c_ended_job_stop_t.GT( itime ), c_ended_job_stop_t.LT( ftime) )
          req_ended = Req_OR( req_ended_sub_time, req_ended_stop_time )
          q_ended.AddRequirement( req_ended )
          
       elif type == 'status':
           
          #
          #  Time conditions:
          #     either the job must have been submitted after itime and before ftime
          #     or it has finished after itime and before ftime
          #
          req_sub_time  = Req_AND( c_job_sub_t.GT(  itime ), c_job_sub_t.LT(  ftime) )
          req_stop_time = Req_AND( c_job_stop_t.GT( itime ), c_job_stop_t.LT( ftime) )
          req = Req_OR( req_sub_time, req_stop_time )
          q.AddRequirement( req )

          req_ended_sub_time  = Req_AND( c_ended_job_sub_t.GT(  itime ), c_ended_job_sub_t.LT(  ftime) )
          req_ended_stop_time = Req_AND( c_ended_job_stop_t.GT( itime ), c_ended_job_stop_t.LT( ftime) )
          req_ended = Req_OR( req_ended_sub_time, req_ended_stop_time )
          q_ended.AddRequirement( req_ended )

 
   if merge == 'yes' :
      
       q.AddRequirement(       c_chain_name.LIKE('%mergejob%') )
       q_ended.AddRequirement( c_chain_name.LIKE('%mergejob%') )


       
   elif merge == 'no' :
      
       q.AddRequirement(       c_chain_name.NOTLIKE('%mergejob%') )
       q_ended.AddRequirement( c_chain_name.NOTLIKE('%mergejob%') )
 

   # ------------------------------
   #      QUERY                    
   # ------------------------------

   if type == 'codes' :
       q_ended.AddRequirement ( c_ended_task_exit.ISNOTNULL() )

   q.Query()
   q_ended.Query()

   if int(verbose) == 3 :
      print q
      print q_ended

   n = q_ended.Execute()
   N = NTuple( n.GetHeaders(), 0 )
   N.Append( n )

   if type == 'status' :
   
     n2 = q.Execute()
     N.Append( n2 )

   return N

    
def Query( params ):

   nconnections = len( params.Connections )
   if nconnections == 0:
       print "no connection parameters have been specified"
       sys.exit( -1 )

   ntuples=[]
   for i in range(nconnections) :
      ntuple = SingleQuery(params,i)
      ntuples.append( ntuple )

   #
   #  -- adding all ntuples in just one --
   #
   N = ntuples[0]
   for n in ntuples[1:] :
     N.Append( n )

   return N
      
 

# ------------------------------------------------------- #
#                                                         #
#    Functions to print the results obtained              #
#    with the desired format                              #
#                                                         #
# ------------------------------------------------------- #

def TableCodes( ntuple, params, workflows=[] ):


       # the default values
       verbose = params.Verbose
       if verbose == '' : verbose ='1'
    
       nrows = ntuple.GetNRows()
       if nrows == 0:
          print '\n\n   No Results   \n\n'
          sys.exit()
       
       elements = []   # elements can be sites or workflows
                       # is the variable in the rows of the output table
       codes = ['0','No 0']
       
       table=Table()
       table.AppendColumn( '0' )
       
       for row in ntuple.GetRows():

          task = row[0]
          code = row[1]
          site = row[2]


          if params.Classify == "sites" or params.Classify == "":
              
            if site == 'NULL' :
                 site = 'unknown'
            else :
                 site = site[ site.index('.')+1:]  

            element = site

          else : # Classify == "workflows"

              element = SelectWorkflow( workflows, task )
       
          if element not in elements :
               elements.append( element )
               table.AppendRow( element )
          
             
          if code not in codes :
             codes.append( code )
             table.AppendColumn( code )
       
          table.IncItem( element, code )
    
       table.SortColumns( lambda x,y: int(x)-int(y) )
    
       #
       #  computing the total number of jobs with RC != 0
       #
       columnNo0=[0]*table.GetNRows()
       columnheaders = table.GetColumnHeaders()
       for header in columnheaders[1:] : # only those headers different from '0'
          column = table.GetColumnValues(header)
          columnNo0 = [x[0]+x[1] for x in zip(columnNo0, column) ]
       
       table.InsertColumn(1,'No 0',columnNo0 )                                      

       table.SortRows()
    
       return table    
    
def TableStatus( ntuple, params, workflows=[] ):

       # the default values
       verbose = params.Verbose
       if verbose == '' : verbose ='1'
       
       nrows = ntuple.GetNRows()
       if nrows == 0:
          print '\n\n   No Results   \n\n'
          sys.exit()
    
       table=Table()
    
       elements = []
       status = [              \
                 'Evt',        \
                 'Success',    \
                 'Failed',     \
                 'Done',       \
                 'Running',    \
                 'Sched',      \
                 'Waiting',    \
                 'Aborted',    \
                 'Cancel',     \
                 'Submitted'   \
                ]

       for st in status : table.AppendColumn( st )
       
       for row in ntuple.GetRows():

          task   = row[0]
          exit   = row[1]
          nevt   = row[2]
          site   = row[3]
          status = row[4]
          submit = row[5]

          if params.Classify == "sites" or params.Classify == "":

             if site == 'NULL' :
                 site = 'unknown'
             else :
                 site = site[ site.index('.')+1:]              

             element = site

          else:  # Classify == "workflows"

              element = SelectWorkflow( workflows, task )
       
          if element not in elements :
               elements.append( element )
               table.AppendRow( element )
          
          if element not in elements :
             elements.append( element )
             table.AppendRow( element )
             
          if status == 'Done'      : table.IncItem( element, 'Done'    )
          if status == 'Scheduled' : table.IncItem( element, 'Sched'   )
          if status == 'Waiting'   : table.IncItem( element, 'Waiting' )
          if status == 'Submitted' : table.IncItem( element, 'Waiting' )
          if status == 'Ready'     : table.IncItem( element, 'Waiting' )
          if status == 'Running'   : table.IncItem( element, 'Running' )
          if status == 'Aborted'   : table.IncItem( element, 'Aborted' )
          if status == 'Cancelled' : table.IncItem( element, 'Cancel'  )

          if exit != 'NULL':
             if exit == '0' : table.IncItem( element, 'Success' )
             else           : table.IncItem( element, 'Failed'  )
    
          if exit == '0':
             if nevt != 'NULL' :
                  table.IncItem( element, 'Evt', int(nevt) )

          if submit != 'NULL':
                 table.IncItem( element, 'Submitted' )

       table.SortRows()
         
       return table


def Totals( table ):

    totales=[]
    columnheaders = table.GetColumnHeaders()
    for header in columnheaders:
      column = table.GetColumnValues( header )
      total = reduce( lambda x,y:x+y, column )
      totales.append( total )
 
    table.AppendRow('TOTAL', totales)

    return table

def AddEfficiencies( table ) :

         #
         #   StageOut_Eff =  1 -  TOTAL_60311/( TOTAL_60311 + TOTAL_0 )
         #   Job_Eff      =  TOTAL_0 / TOTAL_NoNULL
         #

         Cero   = table.GetColumnValues( '0' )
         NoCero = table.GetColumnValues( 'No 0' )
         Error60311 = [0]*len( Cero )
         if '60311' in table.GetColumnHeaders() :
             Error60311 = table.GetColumnValues( '60311' )
             
         Total = [x+y for (x,y) in zip(Cero,NoCero) ]

         Stage_eff = [ round( 1 - float(x)/(x+y), 2 ) for (x,y) in zip( Error60311, Cero) ]
         Job_eff   = [ round( float(x)/y,         2 ) for (x,y) in zip( Cero, Total) ]

         table.AppendColumn( 'StageOut_eff', Stage_eff )
         table.AppendColumn( 'Job_eff',      Job_eff   )

         return table


def PrintToURL( table , url ) :
       
       htmlfile = open( url, "w" )

       htmlfile.write( '<html>  \n' )
       htmlfile.write( '<head>  \n' )
       htmlfile.write( '<title PAMON </title>  \n' )
       htmlfile.write( '<meta HTTP-EQUIV="Refresh" CONTENT="300">  \n' )
       htmlfile.write( '</head>  \n' )
       htmlfile.write( '<body BGCOLOR="#FFFFE8">  \n' )
       htmlfile.write( '<center><h3><font color="LightBlue">Production Monitor </font></h3></center>  \n' )
       htmlfile.write( '<center>  \n' )
       htmlfile.write( '<center>  \n' )
       htmlfile.write( '<table border="1">  \n' )
       htmlfile.write( '<tr  BGCOLOR="#99FF00">  \n' )

       htmlfile.write( '<td></td>  \n' )

       for header in table.GetColumnHeaders():
            htmlfile.write( '<td><b>' + header + '</b></td>  \n' )
       htmlfile.write( '</tr>  \n' )

       for site in table.GetRowHeaders():
          htmlfile.write( '<tr> ' )
          htmlfile.write( '<td>' + site + '</td>  \n' )
          values = table.GetRowValues( site )
          for value in values :
             htmlfile.write( '<td>' + str(value) + '</td>  \n' )
          htmlfile.write( '</tr>  \n' )
       
       htmlfile.write( '</table>  \n' )

       htmlfile.write( '</body>  \n' )
       htmlfile.write( '</html>  \n' )

       htmlfile.close()
      

def Print( ntuple, params ):

    classify = params.Classify
    type     = params.Type
    verbose  = params.Verbose
    url      = params.URL
    
    # the default values
    if classify == '' : classify="sites"
    if type == ''     : type="status"
    if verbose == ''  : verbose ='1'

    workflows=[]
    if classify=='workflows' :
       workflows = Workflows(params)


    if type=='codes':
       table = TableCodes( ntuple, params, workflows )
    
    elif type=='status':
       table = TableStatus( ntuple, params, workflows )

    table = Totals(table)  

    if type == 'codes' and int(verbose) == 2 :
       table = AddEfficiencies( table )
       
    print
    if int(verbose) == 3: # verbose = 3 is the DEBUG value
       params.Print()
       ntuple.Print()
       print
    table.Print(-1)
    print

    if url != '' :
      PrintToURL( table, url )


def Workflows( params ) :

    workflows = []

    for connection in params.Connections :

       query = MySQL_Query()
       query.Connect( connection )

       chain_table = MySQL_Table( "CHAIN" )
       chain_name = chain_table.AddColumn( "NAME" )
       query.AddRequirement( chain_name.NOTLIKE( "%mergejob%" ) )
       
       query.AddTable( chain_table )
       query.Query()
       
       ntuple = query.Execute()

       for row in ntuple.GetRows():
           wkf = row[0]                # workflow name
           wkf = wkf[: wkf.rfind("-")] # erase the last part of the name: -####
           if wkf not in workflows:
               workflows.append( wkf )

       workflows.sort( lambda x,y : len(y)-len(x) )

    return workflows

def SelectWorkflow( list, item ) :

    for x in list:
        if item.find( x ) == 0 :
          return x


# ======================================================= #
#                                                         #
#                 M   A   I   N                           #
#                                                         #
# ======================================================= #

 
if __name__ == '__main__':
    
   #  Parsing the arguments: List_of_params is List of Params objects
   List_of_params = ParseArguments()  

   for params in List_of_params:
      
       #  Single query for each Params object
       ntuple = Query( params )
       
       #  Print the results from the Query
       Print( ntuple, params )
