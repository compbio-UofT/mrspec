import mysql.connector as m
import csv, os, sys, shutil, inspect, time
from query_database import *

class MrspecDatabaseEditor(MrspecDatabaseQueryer):
    '''Class for populating an empty Mrspec database or modifying an existing one.
    
    Import tables from .csv files, convert tables to standardized schemas for use with MrspecDatabaseQueryer, calculate compound metabolites, .
    
    Attributes:
    - self.silent (bool): indicate whether progress messages will be printed to sys.stout. Exceptions will always be printed.
    - self._database (str): name of database to connect to. Changing this value after connection established will have no effect.
    - self.con (object): database connection object
    - self.cur (object): database cursor object
    - self._base_metabolites (List): list of all metabolite names (str) read from LCModel
    - self.met_to_calculate (Dict): dictionary containing metabolites that are calculated by adding multiple metabolites from _base_metabolites. Each key points to a list of metabolites.
    - self.queryable_metabolites (List): list of all queryable metabolites in the database
    - self.table (String): name of table on which to run all queries
    - self.unique_desc (Str): name of unique identifier for each scan
    - self.metadata (List): list of queryable patient and scan metadata such as tabPatientID and scan parameters
    - self.met_threshold (Dict): dictionary of quality threshold values (int) for all queryable metabolites. Used to filter noisy results, optional. Defaults are loaded from 'metabolite_thresholds.txt' stored in '../config'.
    - self.met_echo (Dict): dictionary of echotimes for all queryable metabolites. Defaults are loaded from 'metabolite_echotimes.txt' stored in '../config'.
    - self.low (Str): expression used to define low echotimes in queries (i.e. <50)
    - self.high (Str): expression used to define high echotimes in queries (i.e. =144)
    - self._u (str): MySQL datatype designator for unsigned integers
    - self._s (str): MySQL datatype designator for signed integers
    - self._d (str): MySQL datatype designator for numbers with decimal points
    - self._t (str): MySQL datatype designator for text
    - self.outcomes_schema (List): database schema for the file containing patient outcomes
    - self.table_schema (List): database schema for the file containing patient scans from before 2012
    - self.update_table_schema (List): database schema for the file containing patient scans from after 2012
    '''
    def __init__(self, silent=False, database='mrspec',finalized=True):
        #call __init__ of parent class: MrspecDatabaseQueryer
        super(MrspecDatabaseEditor, self).__init__(silent,database,finalized)
                
        #datatypes
        self._u = 'UNSIGNED'
        self._s = 'SIGNED'
        self._d = 'DECIMAL(11,6)'
        self._t = 'CHAR'        
        
        self.outcomes_schema = [['`Indication (as written on MRI requisition)`', self._t, 'Indication'],
            ['`Diagnosis (from chart)`', self._t, 'Diagnosis'],
            ['`Classification Code`', self._t],
            ['`if yes: type`', self._t, "Anesthetic"],
            ['`Treatment (from chart)`', self._t, "Treatment"]]
        
        self.table_schema = [
            #['MRN', self._t, 'ID'],
            ['AgeAtScan', self._s],
            ['Gender', self._t],
            ['ScanBZero', self._t],
            ['LocationName', self._t],
            ['Scan_ID', self._u],
            ['ScanTEParameters', self._u],
            ['tabPatient_ID', self._t]
            ] + self.outcomes_schema                        
    
        self.update_table_schema= self.table_schema[:] #[['HSC_Number', self._t, 'ID']] + self.table_schema[1:] #
    
        for metabolite in self._base_metabolites:
            self.table_schema += [[metabolite, self._d],['`' + metabolite + "_%SD`", self._s]]
            self.update_table_schema += [[metabolite, self._d],['`' + metabolite + "_SD`", self._s, '`' + metabolite + "_%SD`"]]               

    def populate_SD_table(self, gender, field, location, return_single_scan_per_procedure, filter_by_sd):
        '''Populates a table in which the '''        
        limit = 50

        cols,all_scans = self.execute_and_return_query(self.parse_query(ID='', Scan_ID='', age='', gender='', field='', location='', metabolites=self.met_threshold, limit='', 
                                                                        uxlimit='', lxlimit='', mets_span_each=False, return_single_scan_per_procedure=False, 
                                                                        filter_by_sd=True, windowed_SD_threshold='',classification_code='',extended=False))

        all_queries=[]
        i=0
        for row in all_scans:
            age = row[0]
            Scan_ID = row[-len(self.metadata)] #sixth last row

            j=0
            #iterate through all columns of the query (doesn't matter at what columns metabolites begin)
            for column in cols:
                #get metabolite name by truncating title if filtered
                name = column[:-9] if filter_by_sd else column

                if name in self.met_threshold and all_scans[i][j] is not None:
                    subquery = ''.join(['UPDATE {2} as S, (SELECT ({1} - (( T.avgXY - T.avgX * T.avgY ) / ( T.avgXsq - power(T.avgX, 2) )*({3} - T.avgx) + T.avgy))/T.sd as q FROM (SELECT STDDEV_SAMP({0}) as sd,avg(AgeAtScan) AS avgX, avg({0})  AS avgY, avg({0}*AgeAtScan)  AS avgXY, avg(power(AgeAtScan, 2)) AS avgXsq,U.{0} FROM ('.format(column, all_scans[i][j], self.table, age), self.parse_query(age=age, gender=gender, field=field, location=location, metabolites=[name], limit=limit, mets_span_each=True, return_single_scan_per_procedure=return_single_scan_per_procedure, filter_by_sd=filter_by_sd,extended=False),') AS U) AS T) AS Q set S.{}_SD=CAST(Q.q AS {}) WHERE S.{}={}'.format(name,self._d,'Scan_ID',Scan_ID)])
                    all_queries.append(subquery)
                j+=1
            i+=1
        if not self.silent: print('Sending SD score updates to database...')
        #["LOCK TABLES {} WRITE".format(self.table)]+["UNLOCK TABLES"]
        t = self.cur.execute('; '.join(all_queries), multi=True)
        for u in t:
            pass
        self.con.commit()
        
    def calculate_standard_deviation(self, ages, metabolites, values, gender=None, field=None, location=None, return_single_scan_per_procedure=False, filter_by_sd=True):
        limit=50
        
        for age in ages:
            for value,metabolite in values,metabolites:
                
                c,rows = execute_and_return_query(''.join(['SELECT ({1}- (( T.avgXY - T.avgX * T.avgY ) / ( T.avgXsq - power(T.avgX, 2) )*({3} - T.avgx) + T.avgy))/T.sd as q FROM (SELECT STDDEV_SAMP({0}) as sd,avg(AgeAtScan) AS avgX, avg({0})  AS avgY, avg({0}*AgeAtScan)  AS avgXY, avg(power(AgeAtScan, 2)) AS avgXsq,U.{0} FROM ('.format(metabolite+'_Filtered' if not filter_by_sd else metabolite, value, self.table, age), self.parse_query(age=age, gender=gender, field=field, location=location, metabolites=metabolite, limit=limit, mets_span_each=True, return_single_scan_per_procedure=return_single_scan_per_procedure, filter_by_sd=filter_by_sd,extended=False),') AS U) AS T']))

    def populate_SD_table_without_multi(self, gender, field, location, return_single_scan_per_procedure, filter_by_sd):
        '''Populates a table in which the '''        
        limit = 50
    
        cols,all_scans = self.execute_and_return_query(self.parse_query(metabolites=self.queryable_metabolites, mets_span_each=False, return_single_scan_per_procedure=False, filter_by_sd=True))
    
        i=0
        l = len(all_scans)
        if not self.silent:
            print("Progress:\n ")
        for row in all_scans:
            age = row[0]
            Scan_ID = row[-len(self.metadata)] #sixth last row
    
            j=0
            #iterate through all columns of the query (doesn't matter at what columns metabolites begin)
            for column in cols:
                #get metabolite name by truncating title if filtered
                name = column[:-9] if filter_by_sd else column   
    
                if name in self.met_threshold and all_scans[i][j] is not None:
                    subquery = ''.join(['UPDATE {2} as S, (SELECT ({1}- (( T.avgXY - T.avgX * T.avgY ) / ( T.avgXsq - power(T.avgX, 2) )*({3} - T.avgx) + T.avgy))/T.sd as q FROM (SELECT STDDEV_SAMP({0}) as sd,avg(AgeAtScan) AS avgX, avg({0})  AS avgY, avg({0}*AgeAtScan)  AS avgXY, avg(power(AgeAtScan, 2)) AS avgXsq,U.{0} FROM ('.format(column, all_scans[i][j], self.table, age), self.parse_query(age=age, gender=gender, field=field, location=location, metabolites=[name], limit=limit, mets_span_each=False, return_single_scan_per_procedure=return_single_scan_per_procedure, filter_by_sd=filter_by_sd,extended=False),') AS U) AS T) AS Q set S.{}_SD=CAST(Q.q AS {}) WHERE S.{}={}'.format(name,self._d,'Scan_ID',Scan_ID)])
                    ##old which just takes mean
                    #subquery = ''.join(['UPDATE {2} as S, (SELECT (CASE WHEN COUNT(T.{0})<2 THEN 0 ELSE ({1}-AVG(T.{0}))/STDDEV_SAMP(T.{0}) END) AS q FROM ('.format(column, all_scans[i][j], self.table), self.parse_query('','',age,gender,field,location,[name],limit,'','',True,return_single_scan_per_procedure,filter_by_sd,[],[],'',''),') AS T) AS Q set S.{}_SD=CAST(Q.q AS DECIMAL(11,6))where S.{}={}'.format(name,'Scan_ID',Scan_ID)])
                    #print(subquery)
                    self.cur.execute(subquery)
                    self.con.commit()
                j+=1
            i+=1
            if not self.silent and (i % 50) == 0:
                print(str(i*100/l) + '%')            
            
    def create_null_sd_columns(self, table):
        for m in self.queryable_metabolites:
            if self.column_exists(table, m+'_SD'):
                self.cur.execute('UPDATE {} SET {} = NULL'.format(table, m+"_SD"))
            else:
                self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, m+'_SD', self._d))
        self.con.commit()    

    def insert_aggregate_metabolites_optimal(self, table, met_to_calculate):
        if self.table_exists(table):
            for met in met_to_calculate:
                can_calculate = True            
                for m in met_to_calculate[met]:
                    if not self.column_exists(table, m):
                        if not self.silent: print('Unable to calculate {}, metabolite {} value required.'.format(met,m))
                        can_calculate = False
    
                if can_calculate:                   
                    if self.column_exists(table, met+'_opt'):
                        self.cur.execute('UPDATE {} SET {} = NULL'.format(table, met+"_opt"))
                    else:
                        self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, met+'_opt', self._d))
                    if self.column_exists(table, met+'_opt_%SD'):
                        self.cur.execute('UPDATE {} SET {} = NULL'.format(table, '`' + met+'_opt_%SD`'))
                    else:
                        self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, '`' + met+'_opt_%SD`', 'BIGINT(21)'))
                        
                    added = ' + '.join(["sel."+ mm +"_opt" for mm in met_to_calculate[met]])
                    greatest = ','.join(["sel.`"+ mm + "_opt_%SD`" for mm in met_to_calculate[met]])
                    
                    not_zero = ''.join(['AND '," > 0 AND ".join([ 'sel.' + mm + '_opt' for mm in met_to_calculate[met]]),' > 0'])
    
                    subquery1 = "SELECT AgeAtScan,Scan_ID,{},".format(self.unique_desc) + ",".join(["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}`>0 AND {3}.ScanTEParameters {2} THEN {0} ELSE NULL END),',',1) as `{0}_opt`".format(mm, '100', self.met_echo[mm],table) for mm in met_to_calculate[met]]) + ' FROM {} GROUP BY AgeAtScan,{}'.format(table,self.unique_desc)
                    
                    if not self.silent: print('--------------FIX AGGMET OPT VALUES-----------------')
                    q = "UPDATE {0} T, ({4}) sel SET T.{1} = ({2}) WHERE T.AgeAtScan = sel.AgeAtScan AND T.{3}=sel.{3}".format(table, met +"_opt", added, self.unique_desc, subquery1)
                    if not self.silent: print(q)
                    self.cur.execute(q)
                    
                    subquery2 = "SELECT AgeAtScan,Scan_ID,{},".format(self.unique_desc) + ",".join(["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}_%SD`<={1} AND `{0}_%SD`>0 AND {3}.ScanTEParameters {2} THEN `{0}_%SD` ELSE NULL END),',',1) as `{0}_opt_%SD`".format(mm, '998', self.met_echo[mm], table) for mm in met_to_calculate[met]]) + ' FROM {} GROUP BY AgeAtScan,{}'.format(table,self.unique_desc)
                    
                    if not self.silent: print('--------------FIX AGGMET OPT SD-----------------')
                    q2 = "UPDATE {0} T, ({4}) sel SET T.{1} = GREATEST({2}) WHERE T.AgeAtScan = sel.AgeAtScan AND T.{3}=sel.{3}".format(table, '`' + met +"_opt_%SD`", greatest, self.unique_desc, subquery2)
                    if not self.silent: print(q2)
                    self.cur.execute(q2)
                    self.con.commit()
                    
    def insert_additional_metabolites(self, table, met_to_calculate):
        if self.table_exists(table):
            for met in met_to_calculate:
                can_calculate = True            
                for m in met_to_calculate[met]:
                    if not self.column_exists(table, m):
                        if not self.silent: print('Unable to calculate {}, metabolite {} value required.'.format(met,m))
                        can_calculate = False
                        
                if can_calculate:                   
                    if self.column_exists(table, met):                    
                        self.cur.execute('UPDATE {} SET {} = NULL'.format(table, met))
                    else:
                        self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, met, self._d))
                    if self.column_exists(table, met+'_%SD'):
                        self.cur.execute('UPDATE {} SET {} = NULL'.format(table, '`'+met+"_%SD`"))
                    else:
                        self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, '`' + met+'_%SD`', 'BIGINT(21)'))
                    self.con.commit()
    
                    added = " + ".join(met_to_calculate[met])
                    least = ','.join(["`"+ mm + "_%SD`" for mm in met_to_calculate[met]])
    
                    not_zero = ''.join(['AND '," >= 0 AND ".join([ mm for mm in met_to_calculate[met]]), '>=0'])
                    
                    if not self.silent: print('--------------FIX AGGMET VALUES-----------------')
                    q = "UPDATE {0} as T SET {1} = ({2}) WHERE T.Scan_ID = Scan_ID {3}".format(table, met, added, not_zero)
                    if not self.silent: print(q)
                    self.cur.execute(q)
                    
                    if not self.silent: print('--------------FIX AGGMET SD-----------------')
                    q2="UPDATE {0} as T SET {1} = LEAST({2}) WHERE T.Scan_ID = Scan_ID {3}".format(table, '`'+met+"_%SD`", least, not_zero)
                    if not self.silent: print(q2)
                    self.cur.execute(q2)
                    self.con.commit()
    
    def check_for_table_before_executing(self, name, query, commit=True):
        '''Str, Str -> None
        Executes the specified query if the table (name) does not exist in the database.'''
        if self.table_exists(name):
            if not self.silent: print("Table '{}' already in database. No changes made.".format(name))
        else:
            self.cur.execute(query)
            self.con.commit()
    
            if not self.silent: print("Table '{}' created successfully.".format(name))
    
    def create_standardized_table(self, name, source, table_schema, fulltexts, unique):
        if self.table_exists(name):
            if not self.silent: print("Table '{}' already in database. No changes made.".format(name))
        else:
            selection = ",".join(["CAST({0} AS {1}) AS {2}".format(c[0], c[1], c[0] if len(c) < 3 else c[2]) for c in table_schema]) #c[2] is used to rename the column
            
            group_by = ''
            if unique:
                group_by = "GROUP BY {}".format(unique)
                
            self.cur.execute("CREATE TABLE {} SELECT {} FROM {} {}".format(name, selection, source, group_by))
            
            if fulltexts:
                cur.execute("ALTER TABLE {} ADD FULLTEXT({})".format(name,fulltexts))
                
            if not self.silent: print("Table '{}' created successfully.".format(name))
    
    #depracated function
    def create_sd_table(self, name, source, imports, nulls):
        if self.table_exists(name):
            if not self.silent: print("Table '{}' already in database. No changes made.".format(name))
        else:
            
            #import certain columns with values from another table
            selection = ",".join(imports)
            self.cur.execute("CREATE TABLE {} SELECT {} FROM {}".format(name, selection, source))
            
            #add remaining columns with NULL values
            for col_spec in nulls:
                self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, col_spec[0], col_spec[1]))
                
            self.con.commit()
                
            if not self.silent: print("Table '{}' created successfully.".format(name))     
    
    def import_csv(self, f, name, form):
        if self.table_exists(name):
            if not self.silent: print("Table '{}' already in database. No changes made.".format(name))
        else:
            with open(f, 'r') as csvfile:
                r = csv.reader(csvfile, delimiter = ',', quotechar = '|')
    
                header_raw = next(r)
    
                header = ', '.join('`{0}` {1}'.format(w, form) for w in header_raw)
                #if not self.silent: print(len(header_raw))
    
            self.cur.execute("create table if not exists {} ({}) CHARACTER SET utf8 COLLATE utf8_general_ci".format(name, header))
            self.con.commit()
    
            #FILE MUST BE IN MYSQL FOLDER (i.e. "C:\ProgramData\MySQL\MySQL Server x.x\data\mrspec")
            self.cur.execute("load data infile '{}' into table {} fields terminated by ',' optionally enclosed by '\"' lines terminated by '\r\n' ignore 1 lines".format(f,name))
            self.con.commit()
    
            if not self.silent: print("Table '{}' in '{}' successfully imported from '{}'.".format(name,self._database,f))
    
    def duplicate_table(old_table,new_table):
        if not self.table_exists(old_table):
            x = self.cur.execute("CREATE TABLE {1} LIKE {0}; INSERT {1} SELECT * FROM {0}".format(old_table,new_table),multi=True)
            for e in x:
                pass
            if not self.silent: print("Table '{}' successfully copied from '{}'.".format(new_table,old_table))
            
    def rename_lower_field_metabolites(self,table):
        if self.execute_and_return_query("select count(Cr) from {} where ScanBZero=1.5".format(table))[1][0][0]:
            self.cur.execute("UPDATE {0} SET tCr=Cr,tCho=Cho,`tCr_%SD`=`Cr_%SD`,`tCho_%SD`=`Cho_%SD` WHERE ScanBZero=1.5 AND {0}.Scan_ID=Scan_ID".format(table))
            self.cur.execute("UPDATE {0} SET tCr_opt=Cr WHERE (`tCr_%SD` BETWEEN 0.000001 AND {1}) AND ScanBZero=1.5 AND {0}.Scan_ID=Scan_ID".format(table,self.met_threshold['tCr_opt']))
            self.cur.execute("UPDATE {0} SET tCho_opt=Cho WHERE (`tCho_%SD` BETWEEN 0.000001 AND {1}) AND ScanBZero=1.5 AND {0}.Scan_ID=Scan_ID".format(table,self.met_threshold['tCho_opt']))
            self.con.commit()
            self.cur.execute("UPDATE {0} SET Cr=NULL,Cho=NULL,`Cr_%SD`=NULL,`Cho_%SD`=NULL WHERE ScanBZero=1.5".format(table))
            self.con.commit()
            if not self.silent: print("\nRenamed Cr and Cho from 1.5T to tCr and tCho (and tCr_opt,tCho_opt) in '{}'.".format(table))   
            
    def drop_table_if_exists(self, name):
        if self.table_exists(name):
            self.cur.execute("DROP TABLE {}".format(name))
            if not self.silent: print("Table '{}' dropped from '{}'.".format(name, self._database))
            
if __name__ == "__main__":

    #Establish connection with database
    with MrspecDatabaseEditor(finalized=False) as (c,con,cur):
        
        if c.table_exists(c.table):
            print("'{}' already exists in database, no changes made.".format(c.table))
        else:
    
            #import requisite tables in local folder and mysql folder
            c.import_csv("outcomes2.csv", "outcomes", "varchar(500)")
            c.import_csv("mrspec.csv", "mrspec", "text")
            c.import_csv("tabPatients.csv", "tab_MRN", "varchar(50)")
        
            #re-personalize mrspec with MRN
            c.check_for_table_before_executing('mrspec_MRN', "CREATE TABLE IF NOT EXISTS mrspec_MRN SELECT m.*, t.HSC_Number as MRN FROM mrspec AS m JOIN tab_MRN as t ON m.tabPatient_ID=t.tabPatient_ID")
        
            #merge tables into table 'merged', group patient outcomes (since data is all the same for same patient on same day)
            cur.execute("ALTER TABLE outcomes DROP COLUMN tabPatient_ID") if c.column_exists("outcomes", "tabPatient_ID") else None
            c.check_for_table_before_executing("merged","CREATE TABLE IF NOT EXISTS OUTCOMES_GROUPED SELECT * FROM outcomes GROUP BY `MRN (column to be removed once study is in analysis phase)`,str_to_date(outcomes.Date, '%d/%m/%Y') ORDER BY str_to_date(outcomes.Date, '%d/%m/%Y')")
            con.commit()
        
            c.check_for_table_before_executing('merged', "CREATE TABLE IF NOT EXISTS merged SELECT * FROM OUTCOMES_GROUPED RIGHT JOIN mrspec_MRN ON OUTCOMES_GROUPED.`MRN (column to be removed once study is in analysis phase)` = mrspec_MRN.MRN AND str_to_date(OUTCOMES_GROUPED.Date, '%d/%m/%Y') = str_to_date(mrspec_MRN.procedureDate, '%d/%m/%Y')")   
            
            
            #create standardized table
            c.create_standardized_table(c.table, 'merged', c.table_schema, None, 'Scan_ID')
            
            ##post-processing
            c.insert_additional_metabolites(c.table, c.met_to_calculate)
            c.insert_aggregate_metabolites_optimal(c.table, c.met_to_calculate)
            #rename Cr and Cho for 1.5T scans to tCr and tCho
            c.rename_lower_field_metabolites(c.table)        
            c.create_null_sd_columns(c.table)
            
            if prompt_yes_no("\nDo you wish to calculate the windowed SD columns? WARNING: This will take a long time and is not recommended until all update files have been imported."): c.populate_SD_table_without_multi('', '', '', False, True)

            print('\nAll operations completed successfully.')