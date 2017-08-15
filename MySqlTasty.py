import pymysql
import traceback

#********************************************************************sqlitetasty**********************************************************************************    

class MySqlTasty():
            
    def __PopulateDataSetFields(self,cursor):
        try:           
            self.DataSetFields = [None] * len(cursor.description)
            for i in range(len(cursor.description)):
                self.DataSetFields[i] = cursor.description[i][0]
        except:
            self.DataSetFields = []
    def __CreateDictFromFieldListAndRowData(self,row):
        Dict = {}
        for i in range(len(self.DataSetFields)):
            Dict[self.DataSetFields[i]] = row[i]
        return Dict
        
    def __CreateListFromRowData(self,row):
        list = []
        for i in row:
            list.append(i)
        return list
        
    def __PopulateResultData(self,cursor):
        self.resultsDictArray = []
        __resultsIndexArray = []
        for row in cursor:
            dict = self.__CreateDictFromFieldListAndRowData(row)
            list = self.__CreateListFromRowData(row)
            self.resultsDictArray.append(dict)
            self.resultsIndexArray.append(list)

                           
    def __SetLoginInfo(self,DatabaseName,UserName,Password,Host):
        
        self.DBName = DatabaseName
        self.UserName = UserName
        self.Password = Password
        self.Host = Host
        
    def __PrintError(self,err):
        try:
            traceback.print_exc()
        except:
            print(err)
            
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - PUBLIC FUNCTIONS - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 
    def __init__(self, DB, User, Passwd, Host):        
        self.connect(DB, User, Passwd, Host)
		self.DataSetFields =[]
        self.SetAutoCommit(True)
        self.SetAutoClose(True)
        self.close();
        
    def SetAutoClose(self, boolean):
        self.CloseAfterExecute = boolean
       
    def SetAutoCommit(self, boolean):
        self.CommitAfterExecute = boolean
   
    def connect(self,DB,User,Passwd,Host):
        self.__SetLoginInfo(DB,User,Passwd,Host)
        try:
            self.conn = pymysql.connect(
            db=DB,
            user=User,
            passwd=Passwd,
            host=Host)
        except pymysql.Error as err:
            self.__PrintError(err)
       
    def close(self):
        try:
            self.conn.close()
        except pymysql.Error as err:
            self.__PrintError(err)
       
    def commit(self):
        try:
            self.conn.commit()
        except pymysql.Error as err:
            self.__PrintError(err)

    def GetRowCount(self):
        try:
            return len(__resultsDictArray)
        except:
            return 0
    def GetFieldCount(self):
        try:
            return  len(__resultsDictArray[0])
        except:
            return 0
            
    def GetFieldsList(self):
        try:
            return self.DataSetFields
        except:
            return []
            
    def GetResultsDictArray(self):
        try:
            return __resultsDictArray
        except Exception as err:
            return []
            
    def GetResultsIndexArray(self):
        try:
            return __resultsIndexArray
        except Exception as err:
            return []
   
    def execute(self,sqlstring):        
        self.connect(__DBName,__UserName,__Password,__Host)
        try:
            c  = self.conn.cursor()           
            c.execute(sqlstring)
            self.__PopulateDataSetFields(c)
            self.__PopulateResultData(c)
            if self.CommitAfterExecute is True:__conn.commit()                    
            if self.CloseAfterExecute  is True:__conn.close()              
        except pymysql.Error as err:
            self.__PrintError(err)
