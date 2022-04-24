import Obfuscator as ob
import pyodbc

class DBConnection:
    """ The purpose of this class is to establish a connection to the database based on the passed in parameters used to instantiate the class. 
        The connection can be retrieved by calling get_connection() which returns the pyodbc connection object to the caller.
    """
    #This function sets the parameters for the database connection string and then attempts to create a pyodbc connection.
    #If successful it sets the 'connected' boolean to True whose value can be retrieved using the is_connected() function.
    #The database connection can be retrieved from a DBConnection object using get_connection().
    def __init__(self: object, dsn: str, server: str, db_name: str, username: str, password: str, trusted_mode: bool, viewname: str):
        self.dsn: str = dsn
        self.server: str = server
        self.db_name: str = db_name
        self.username: str = username
        self.__password: str = password
        self.trusted_mode: bool = trusted_mode
        self.viewname: str = viewname

        #Constant string to find suitable MS SQL driver
        self._MSSQL_DRIVER_STRING:str = ' for SQL Server'

        #Get Oracle driver for MS SQL Server
        self._get_mssql_oracle_driver()    
        
        #Create an obfuscator object to deobfuscate the password in the pyodbc connection string below
        obfuscator:Obfuscator = ob.Obfuscator()

        self.connection:pyodbc.Connection = None
        try:
            self.connection = pyodbc.connect(
                'DRIVER={'+ self._driver_name + '}'+ \
                ';SERVER='+ self.server + \
                ';DATABASE='+ self.db_name + \
                ';UID='+ self.username + \
                ';PWD='+ obfuscator.deObfuscate(self.__password) +';'
                )        

            print('Connected to DB as:', self.__repr__())
            pyodbc.pooling = False

        #Nested exceptions as there exists many possible types of error when trying to establish a database connection
        except pyodbc.InterfaceError as err:
            raise Exception("DBConnection:__init__-> InterfaceError: An incorrect password may have been entered {} ".format(err))
        except pyodbc.DatabaseError as err:
            raise Exception("DBConnection:__init__-> DatabaseError {} ".format(err))
        except pyodbc.Error as err: #Catch any other type of exception
            raise Exception("DBConnection:__init__-> An error has occurred while trying to establish a database connection {} ".format(err))

        #If the connection was established set the boolean parameter 'connected' to True.
        #The function is_connected() returns the value of this parameter when called.
        if(self.connection is not None):
            self.connected: bool = True

    #Sets the suitable driver name for MS SQL Server
    def _get_mssql_oracle_driver(self: object)->None:
        _driver_names = [x for x in pyodbc.drivers() if x.endswith(self._MSSQL_DRIVER_STRING)]
        if _driver_names:
            self._driver_name = _driver_names[0]

    #Used to return database connection information 
    def __repr__(self: object)->str:
        return f"MS-SQLServer('{self.db_name}', '{self.username}', <password hidden>, '{self.server}')"

    #Closes the database connection
    def __del__(self: object)->None:
        if(self.connection is not None):
            self.connection.close()           
            print("Database connection closed.")

    #Returns boolean indicating whether there is a connection to the database
    def is_connected(self: object)->bool:
        return self.connected

    #Returns the pyodbc connection
    def get_connection(self: object)->pyodbc.Connection:
        return self.connection