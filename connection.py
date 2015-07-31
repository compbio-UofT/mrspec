import mysql.connector as m
import csv, os, sys, shutil, inspect
import __main__ as main

def is_run_from_commandline():
    '''Return true if the script calling this method was run from command line, false otherwise (i.e. in an IDE).'''
    if inspect.getouterframes(inspect.currentframe())[1][1] == main.__file__:
        return True
    return False

class DatabaseConnection(object):
    
    def __init__(self, sysargs, silent=False):
        self.silent = silent            
        self.con, self.cur = self._establish_connection(sysargs)
    
    def __enter__(self):
        return self.con, self.cur
        
    def _establish_connection(self, args):

        #inspect whether run from commandline or not
        if inspect.getouterframes(inspect.currentframe())[1][1] == main.__file__:
            #Initialize connection to MySQL database
            #usage: python query.py <user> <password> <port>
            try:
                con = m.connect(user=args[1], password=args[2],
                                        database='mrspec', port=args[3])
                cur = con.cursor()
                return con, cur
            except IndexError as e:
                if not self.silent: sys.stderr.write('Incorrect commandline usage.\nUsage: python <filename> <user> <password> <port>')              
        else:
            #get credentials from file or user input
            try:
                with open("credentials.txt", 'r') as c:
                    user = c.next()
                    password = c.next()
            except IOError as e:
                #stdin.readline used here for compatibility with Python 2.7 and 3.x
                print('User:')
                user = sys.stdin.readline()
                print('Password:')
                password = sys.stdin.readline()
        
            if not self.silent: print('Credentials loaded successfully.')
        
            con = m.connect(
                    user=user,
                    password=password,
                    database='mrspec')
        
            cur = con.cursor()
        if not self.silent: print('Connection to database successful.\n')
        return con,cur
    
    def __exit__(self, type, value, traceback):
        self.con.close()
        if not self.silent: print('\nConnection to database closed.')
                
if __name__ == "__main__":
    print(is_run_from_commandline())
    
    with DatabaseConnection(sys.argv) as (con, cur):
        print('test')