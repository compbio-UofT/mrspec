import mysql.connector as m
import csv, os, sys, shutil

def create_table(name, query):
    if table_exists(name):
        print("Table '{}' already in database. No changes made.".format(name))
    else:
        #selection = ",".join(["CAST({0} AS {1}) AS {2}".format(c[0], c[1], c[0] if len(c) < 3 else c[2]) for c in table_schema])

        #cur.execute("CREATE TABLE {} SELECT {} FROM {}".format(name, selection, source))

        cur.execute(query)
        con.commit()

        print("Table '{}' created successfully.".format(name))

def create_standardized_table(name, source, table_schema):
    if table_exists(name):
        print("Table '{}' already in database. No changes made.".format(name))
    else:
        selection = ",".join(["CAST({0} AS {1}) AS {2}".format(c[0], c[1], c[0] if len(c) < 3 else c[2]) for c in table_schema]) #c[2] is used to rename the column

        cur.execute("CREATE TABLE {} SELECT {} FROM {}".format(name, selection, source))

        print("Table '{}' created successfully.".format(name))

def table_exists(tablename):
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{}'".format(tablename.replace('\'', '\'\'')))
    if cur.fetchone()[0] == 1:
        return True
    return False

def column_exists(tablename, column):
    cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}' AND column_name = '{}'".format(tablename.replace('\'', '\'\''), column.replace('\'', '\'\'')))
    if cur.fetchone()[0] == 1:
        return True
    return False

def import_csv(f, name, form):

    if table_exists(name):
        print("Table '{}' already in database. No changes made.".format(name))
    else:
        with open(f, 'r') as csvfile:
            r = csv.reader(csvfile, delimiter = ',', quotechar = '|')

            header_raw = next(r)

            header = ', '.join('`{0}` {1}'.format(w, form) for w in header_raw)
            #print(len(header_raw))

        cur.execute("create table if not exists {} ({}) CHARACTER SET utf8 COLLATE utf8_general_ci".format(name, header))
        con.commit()

        #FILE MUST BE IN MYSQL FOLDER
        cur.execute("load data infile '{}' into table {} fields terminated by ',' optionally enclosed by '\"' lines terminated by '\r\n' ignore 1 lines".format(f,name))
        con.commit()

        print("Table {} in {} successfully imported from {}.".format(name,'mrspec',f))


if __name__ == "__main__":

    #load table_schema from file, or load default
    table_schema = None
    try:
        with open("schema.csv", 'r') as csvfile:
            r = csv.reader(csvfile, delimiter = ',', quotechar = '|')
            table_schema = [line.split(',') for line in r]
        print("Table schema loaded from file.")
    except IOError as e:
        print("Error parsing schema file: "+str(e)+". Table schema loaded from defaults.")

        #datatypes
        u = 'UNSIGNED'
        s = 'SIGNED'
        d = 'DECIMAL(11,6)'
        t = 'CHAR'

        metabolites = ['CrCH2', 'AcAc', 'Acn', 'Ala', 'Asp', 'Cho', 'Cr', 'GABA', 'GPC', 'Glc',
                       'Gln', 'Glu', 'Gua', 'Ins', 'Lac', 'Lip09', 'Lip13a', 'Lip13b', 'Lip20', 'MM09',
                       'MM12', 'MM14', 'MM17', 'MM20', 'NAA', 'NAAG', 'PCh', 'PCr', 'Scyllo', 'Tau',
                       'tCr', 'tNAA', 'tCho', 'Glx']
        table_schema = [
            ['tabPatient_ID', t, 'ID'],
            ['AgeAtScan', s],
            ['Gender', t],
            ['ScanBZero', t],
            ['LocationName', t],
            ['Location_ID', s],
            ['ScanTEParameters', u]
        ]

        for metabolite in metabolites:
            table_schema += [[metabolite, d],['`' + metabolite + "_%SD`", s]]

        table_schema += [['`Indication (as written on MRI requisition)`', t, 'Indication'],
                         ['`Diagnosis (from chart)`', t, 'Diagnosis']
                         ]

    if sys.argv[0][-8:] == 'query.py':
        #Initialize connection to MySQL database
        #usage: python query.py <user> <password>
        con = m.connect(user=sys.argv[1], password=sys.argv[2],
                        database='mrspec')
        cur = con.cursor()
        #initialize the Flask application

    else:
        ##get credentials from file or user input
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

        print('Credentials loaded successfully.')

        con = m.connect(
                user=user,
                password=password,
                database='mrspec')

        cur = con.cursor()
        print('Connection to database successful.\n')

    #import requisite tables in local folder and mysql folder
    import_csv("outcomes2.csv", "outcomes", "varchar(500)")
    import_csv("mrspec.csv", "mrspec", "text")
    import_csv("tabPatients.csv", "tab_MRN", "varchar(50)")

    #re-personalize mrspec
    create_table('mrspec_MRN', "CREATE TABLE IF NOT EXISTS mrspec_MRN SELECT m.*, t.HSC_Number as MRN FROM mrspec AS m JOIN tab_MRN as t ON m.tabPatient_ID=t.tabPatient_ID")

    #merge tables into table 'merged'
    cur.execute("ALTER TABLE outcomes DROP COLUMN tabPatient_ID") if column_exists("outcomes", "tabPatient_ID") else None
    cur.execute("CREATE TEMPORARY TABLE OUTCOMES_GROUPED SELECT * FROM outcomes GROUP BY `MRN (column to be removed once study is in analysis phase)`,str_to_date(outcomes.Date, '%Y-%m-%d') ORDER BY str_to_date(outcomes.Date, '%Y-%m-%d')") if not table_exists('merged') else None

    create_table('merged', "CREATE TABLE IF NOT EXISTS merged SELECT * FROM OUTCOMES_GROUPED RIGHT JOIN mrspec_MRN ON OUTCOMES_GROUPED.`MRN (column to be removed once study is in analysis phase)` = mrspec_MRN.MRN AND str_to_date(OUTCOMES_GROUPED.Date, '%Y-%m-%d') = str_to_date(mrspec_MRN.procedureDate, '%y-%m-%d')")

    #create standardized table
    create_standardized_table('standard', 'merged', table_schema)

    print('\nAll operations completed successfully.')

    #close the connection to the database
    con.close()
