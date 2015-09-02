import mysql.connector as m
import csv, os, sys, shutil, inspect
import __main__ as main

def is_run_from_commandline():
    '''NoneType -> Bool
    Return true if the script calling this method was run from command line, false otherwise (i.e. in an IDE).
    '''
    if inspect.getouterframes(inspect.currentframe())[1][1] == main.__file__:
        return True
    return False

def prompt_yes_no(question, default="no"):
    '''(Str, Str) -> Bool
    Ask a yes/no question via sys.stdin.readline() and return the answer; True for "yes" or False for "no".

    Parameters:
    - question is a string that is presented to the user.
    - default is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).
    '''
    valid = {"yes\n": True, "y\n": True,
             "no\n": False, "n\n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = sys.stdin.readline().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

class DatabaseConnection(object):
    '''Establish a connection with the specified database.
    
    Attributes:
    - self.silent (bool): indicate whether progress messages will be printed to sys.stout. Exceptions will always be printed.
    - self._database (str): name of database to connect to. Changing this value after connection established will have no effect.
    - self.con (object): database connection object
    - self.cur (object): database cursor object
    '''
    
    def __init__(self, sysargs, silent=False,database='mrspec'):
        '''(DatabaseConnection, List, Bool, Str) -> NoneType
        Initialize a database connection.
        
        Parameters:
        - sysargs (list): commandline arguments. Usage: python <scriptname>.py <user> <password> <port>
        - silent (bool): whether to print progress messages. Exceptions and errors will always be printed.
        - database (str): name of database to connect to
        '''
        self.silent = silent
        self._database=database
        self.con, self.cur = self._establish_connection(sysargs)
    
    def __enter__(self):
        '''DatabaseConnection -> Object, Object
        When called using 'with', return the database connection and cursor objects.
        '''
        return self.con, self.cur
    
    def __exit__(self, exception_type, value, traceback):
        '''(DatabaseConnection, Exception, Str, Object) -> NoneType
        Close database connection when used with 'with'.
        '''
        self.con.close()
        if not self.silent: print('\nConnection to database closed.')    
        
    def _establish_connection(self, args):
        '''(DatabaseConnection, List) -> Object, Object
        Establish database connection using the specified parameters. 
        
        If run from commandline, use commandline arguments as parameters. If not, search for file with credentials 'credentials.txt' in working directory. If file does not exist, prompt user for credentials. Return the database connection and cursor objects if successful.
        
        Parameters:
        - args (list): commandline arguments. Usage: python <scriptname>.py <user> <password> <port>
        '''
        #inspect whether run from commandline or not
        if inspect.getouterframes(inspect.currentframe())[1][1] == main.__file__:
            #initialize connection to MySQL database
            try:
                con = m.connect(user=args[1], password=args[2],
                                        database=self._database, port=args[3])
                cur = con.cursor()
                return con, cur
            except IndexError as e:
                sys.stderr.write('Incorrect commandline usage.\nUsage: python <filename> <user> <password> <port>')              
        else:
            #get credentials from file or user input
            try:
                with open("credentials.txt", 'r') as c:
                    #if len(c) >= 2:
                    user = next(c)
                    password = next(c)
            except IOError or StopIteration as e:
                #stdin.readline used here for compatibility with Python 2.7 and 3.x
                print('User:')
                user = sys.stdin.readline()
                print('Password:')
                password = sys.stdin.readline()
        
            if not self.silent: print('Credentials loaded successfully.')
        
            con = m.connect(
                    user=user,
                    password=password,
                    database=self._database)
        
            cur = con.cursor()
        if not self.silent: print('Connection to database successful.\n')
        return con,cur
                
if __name__ == "__main__":
    print(is_run_from_commandline())
    
    with DatabaseConnection(sys.argv) as (con, cur):
        pass