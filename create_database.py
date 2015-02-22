import mysql.connector as m
import csv, os, sys, shutil, inspect, os, sys
from connection import *
from query import *

d = DatabaseConnection(sys.argv)
con= d.con
cur = d.cur

met_to_calculate = {'tCr':['PCr','Cr'], 'tNAA':['NAA','NAAG'], 'tCho':['Cho','GPC','PCh'], 'Glx':['Gln','Glu']}
high_low_mets = {'tCr':['PCr','Cr'], 'tNAA':['NAA','NAAG'], 'tCho':['Cho','GPC','PCh']}

#class TableModifier(DatabaseConnection):

def insert_aggregate_metabolites_optimal(name, met_to_calculate):
    if table_exists(name):
        for met in met_to_calculate:
            can_calculate = True            
            for m in met_to_calculate[met]:
                if not column_exists(name, m):
                    print('Unable to calculate {}, metabolite {} value required.'.format(met,m))
                    can_calculate = False

            if can_calculate:                   
                if column_exists(name, met+'_opt'):
                    cur.execute('UPDATE {} SET {} = NULL'.format(name, met+"_opt"))
                else:
                    cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, met+'_opt', 'DECIMAL(11,6)'))
                if column_exists(name, met+'_opt_%SD'):
                    cur.execute('UPDATE {} SET {} = NULL'.format(name, '`' + met+'_opt_%SD`'))
                else:
                    cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, '`' + met+'_opt_%SD`', 'BIGINT(21)'))
                    
                    
                added = ' + '.join(["sel."+ mm +"_opt" for mm in met_to_calculate[met]])
                greatest = ','.join(["sel.`"+ mm + "_opt_%SD`" for mm in met_to_calculate[met]])
                
                not_zero = ''.join(['AND '," > 0 AND ".join([ 'sel.' + mm + '_opt' for mm in met_to_calculate[met]]),' > 0'])

                #met_echo_high[mm]
                subquery1 = "SELECT Scan_ID," + ",".join(["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}`>0 AND {3}.ScanTEParameters {2} THEN {0} ELSE NULL END),',',1) as `{0}_opt`".format(mm, '998', met_echo_high[mm],name) for mm in met_to_calculate[met]]) + ' FROM {} GROUP BY {}, AgeAtScan'.format(name,unique_desc)
                
                print('--------------FIX AGGMET OPT VALUES-----------------')
                q = "UPDATE {0} T, ({4}) sel SET T.{1} = ({2}) WHERE T.Scan_ID = sel.Scan_ID".format(name, met +"_opt", added, '', subquery1)
                print(q)
                cur.execute(q)
                
                subquery2 = "SELECT Scan_ID," + ",".join(["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}_%SD`<={1} AND `{0}_%SD`>0 AND {3}.ScanTEParameters {2} THEN `{0}_%SD` ELSE NULL END),',',1) as `{0}_opt_%SD`".format(mm, '998', met_echo_high[mm], name) for mm in met_to_calculate[met]]) + ' FROM {} GROUP BY {}, AgeAtScan'.format(name,unique_desc)
                
                print('--------------FIX AGGMET OPT SD-----------------')
                q2 = "UPDATE {0} T, ({4}) sel SET T.{1} = GREATEST({2}) WHERE T.Scan_ID = sel.Scan_ID".format(name, '`' + met +"_opt_%SD`", greatest, '', subquery2)
                print(q2)
                cur.execute(q2)
                con.commit()
                
def insert_additional_metabolites(name, met_to_calculate):
    if table_exists(name):
        for met in met_to_calculate:
            can_calculate = True            
            for m in met_to_calculate[met]:
                if not column_exists(name, m):
                    print('Unable to calculate {}, metabolite {} value required.'.format(met,m))
                    can_calculate = False
                    
            if can_calculate:                   
                if column_exists(name, met):                    
                    cur.execute('UPDATE {} SET {} = NULL'.format(name, met))
                else:
                    cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, met, 'DECIMAL(11,6)'))
                if column_exists(name, met+'_%SD'):
                    cur.execute('UPDATE {} SET {} = NULL'.format(name, '`'+met+"_%SD`"))
                else:
                    cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, '`' + met+'_%SD`', 'BIGINT(21)'))
                con.commit()

                added = " + ".join(met_to_calculate[met])
                greatest = ','.join(["`"+ mm + "_%SD`" for mm in met_to_calculate[met]])

                not_zero = ''.join(['AND '," > 0 AND ".join([ mm for mm in met_to_calculate[met]]),' > 0'])
                
                print('--------------FIX AGGMET VALUES-----------------')
                q = "UPDATE {0} as T SET {1} = ({2}) WHERE T.Scan_ID = Scan_ID {3}".format(name, met, added, not_zero)
                print(q)
                cur.execute(q)
                
                print('--------------FIX AGGMET SD-----------------')
                q2="UPDATE {0} as T SET {1} = LEAST({2}) WHERE T.Scan_ID = Scan_ID {3}".format(name, '`'+met+"_%SD`", greatest, not_zero)
                print q2
                cur.execute(q2)
                con.commit()

def check_for_table_before_executing(name, query):
    '''Str, Str -> None
    Executes the specified query if the table (name) does not exist in the database.'''
    if table_exists(name):
        print("Table '{}' already in database. No changes made.".format(name))
    else:
        cur.execute(query)
        con.commit()

        print("Table '{}' created successfully.".format(name))

def create_standardized_table(name, source, table_schema, fulltexts, unique):
    if table_exists(name):
        print("Table '{}' already in database. No changes made.".format(name))
    else:
        selection = ",".join(["CAST({0} AS {1}) AS {2}".format(c[0], c[1], c[0] if len(c) < 3 else c[2]) for c in table_schema]) #c[2] is used to rename the column
        
        group_by = ''
        if unique:
            group_by = "GROUP BY {}".format(unique)
            
        cur.execute("CREATE TABLE {} SELECT {} FROM {} {}".format(name, selection, source, group_by)) ##GROUP BY ID,AgeAtScan
        
        if fulltexts:
            cur.execute("ALTER TABLE {} ADD FULLTEXT({})".format(name,fulltexts))
            
        print("Table '{}' created successfully.".format(name))
        
def create_sd_table(name, source, imports, nulls):
    if table_exists(name):
        print("Table '{}' already in database. No changes made.".format(name))
    else:
        
        #import certain columns with values from another table
        selection = ",".join(imports)
        cur.execute("CREATE TABLE {} SELECT {} FROM {}".format(name, selection, source))
        
        #add remaining columns with NULL values
        for col_spec in nulls:
            cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, col_spec[0], col_spec[1]))
            
        con.commit()
            
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
        print("Error parsing schema file: " +str(e)+". Table schema loaded from defaults.")

        #datatypes
        u = 'UNSIGNED'
        s = 'SIGNED'
        d = 'DECIMAL(11,6)'
        t = 'CHAR'

        metabolites = ['CrCH2', 'AcAc', 'Acn', 'Ala', 'Asp', 'Cho', 'Cr', 'GABA', 'GPC', 'Glc',
                       'Gln', 'Glu', 'Gua', 'Ins', 'Lac', 'Lip09', 'Lip13a', 'Lip13b', 'Lip20', 'MM09',
                       'MM12', 'MM14', 'MM17', 'MM20', 'NAA', 'NAAG', 'PCh', 'PCr', 'Scyllo', 'Tau']        
                
        table_schema = [
            ['MRN', t, 'ID'],
            ['AgeAtScan', s],
            ['Gender', t],
            ['ScanBZero', t],
            ['LocationName', t],
            ['Scan_ID', u],
            ['ScanTEParameters', u]
        ]
        
        table_schema += [['`Indication (as written on MRI requisition)`', t, 'Indication'],
                         ['`Diagnosis (from chart)`', t, 'Diagnosis']
                         ] 
        update_table_schema= [['HSC_Number', t, 'ID']] + table_schema[1:]
        

        for metabolite in metabolites:
            table_schema += [[metabolite, d],['`' + metabolite + "_%SD`", s]]

        for metabolite in metabolites:
            update_table_schema += [[metabolite, d],['`' + metabolite + "_SD`", s]]
        

        
        #Specifies which information to be imported into sd tables
        sd_table_imports = ['ID', 'AgeAtScan', 'Gender', 'ScanBZero', 'LocationName', 'ScanTEParameters']
        #Specify which columns should be added with null values to be calculated later
        sd_table_nulls = []
        for metabolite in metabolites + ['tCr', 'tNAA', 'tCho', 'Glx']:
            sd_table_nulls.append([metabolite + '_SD', d])

    #Establish connection with database
    with DatabaseConnection(sys.argv) as (con,cur):
    
        #import requisite tables in local folder and mysql folder
        import_csv("outcomes2.csv", "outcomes", "varchar(500)")
        import_csv("mrspec.csv", "mrspec", "text")
        import_csv("tabPatients.csv", "tab_MRN", "varchar(50)")
    
        #re-personalize mrspec with MRN
        check_for_table_before_executing('mrspec_MRN', "CREATE TABLE IF NOT EXISTS mrspec_MRN SELECT m.*, t.HSC_Number as MRN FROM mrspec AS m JOIN tab_MRN as t ON m.tabPatient_ID=t.tabPatient_ID")
    
        #merge tables into table 'merged'
        cur.execute("ALTER TABLE outcomes DROP COLUMN tabPatient_ID") if column_exists("outcomes", "tabPatient_ID") else None
        check_for_table_before_executing("merged","CREATE TABLE OUTCOMES_GROUPED SELECT * FROM outcomes GROUP BY `MRN (column to be removed once study is in analysis phase)`,str_to_date(outcomes.Date, '%Y-%m-%d') ORDER BY str_to_date(outcomes.Date, '%Y-%m-%d')")
        con.commit()
    
        check_for_table_before_executing('merged', "CREATE TABLE IF NOT EXISTS merged SELECT * FROM OUTCOMES_GROUPED RIGHT JOIN mrspec_MRN ON OUTCOMES_GROUPED.`MRN (column to be removed once study is in analysis phase)` = mrspec_MRN.MRN AND str_to_date(OUTCOMES_GROUPED.Date, '%Y-%m-%d') = str_to_date(mrspec_MRN.procedureDate, '%y-%m-%d')")   
        
        ##
        #create standardized table
        create_standardized_table('standard', 'merged', table_schema, None, 'Scan_ID')#'Indication,Diagnosis')
        ##update code##
        import_csv("updates.csv", 'updates', "text")
        check_for_table_before_executing('updates_merged', "CREATE TABLE IF NOT EXISTS updates_merged SELECT * FROM updates LEFT JOIN OUTCOMES_GROUPED ON OUTCOMES_GROUPED.`MRN (column to be removed once study is in analysis phase)` = updates.HSC_Number AND str_to_date(OUTCOMES_GROUPED.Date, '%Y-%m-%d') = str_to_date(updates.ProcedureDate, '%d/%m/%Y')")
        con.commit()
        cur.execute('alter table updates_merged add column AgeAtScan bigint(21)') if not column_exists('updates_merged','AgeAtScan') else None
        cur.execute("UPDATE updates_merged as T SET T.AgeAtScan = (TO_DAYS(STR_TO_DATE(T.ProcedureDate,'%d/%m/%Y')) - TO_DAYS(STR_TO_DATE(T.PatientBirthDay,'%d/%m/%Y'))) where T.Scan_ID = Scan_ID")
        con.commit()
        create_standardized_table("standard_update", 'updates_merged', update_table_schema, None, 'Scan_ID')# fulltexts)
        
        ##COMMENT THIS LINE OUT AFTER SCRIPT HAS RUN ONCE, otherwise you will get an error
        #cur.execute('INSERT INTO standard SELECT * FROM standard_update')
        
        con.commit()
        
        ##calculate additional metabolites (tCr, tCho, Glx, tNAA)
        insert_additional_metabolites('standard', met_to_calculate)
        insert_aggregate_metabolites_optimal('standard', high_low_mets)
        
        #create tables for standard deviations
        create_sd_table("sd_both_both_alllocations", "standard", sd_table_imports, sd_table_nulls)
        #create_standardized_table("sd_F_both_alllocations", "standard", sd_table_schema, '')
        #create_standardized_table("sd_M_both_alllocations", "standard", sd_table_schema, '')    
        #create_standardized_table("sd_both_both_alllocations", "standard", sd_table_schema, '')     
    
        print('\nAll operations completed successfully.')