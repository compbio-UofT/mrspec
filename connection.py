import mysql.connector as m
import csv, os, sys, shutil, inspect
import __main__ as main

def is_run_from_commandline():
    '''Return true if the script calling this method was run from command line, false otherwise (i.e. in an IDE).'''
    if inspect.getouterframes(inspect.currentframe())[1][1] == main.__file__:
        return True
    return False

def establish_connection(args, silent=False):
        
    if is_run_from_commandline():
        #Initialize connection to MySQL database
        #usage: python query.py <user> <password> <port>
        try:
            con = m.connect(user=args[1], password=args[2],
                                    database='mrspec', port=args[3])
            cur = con.cursor()
            return con, cur
        except IndexError as e:
            if not silent: sys.stderr.write('Incorrect commandline usage.\nUsage: python <filename> <user> <password> <port>')              
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
    
        if not silent: print('Credentials loaded successfully.')
    
        con = m.connect(
                user=user,
                password=password,
                database='mrspec')
    
        cur = con.cursor()
    if not silent: print('Connection to database successful.\n')
    return con,cur
        
if __name__ == "__main__":
    con,cur = establish_connection('', False)
    print(is_run_from_commandline())
    con.close()
    
    #print "MAIN",__file__