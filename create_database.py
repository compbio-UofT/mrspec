import mysql.connector as m
import csv, os, sys, shutil, inspect, time
from connection import *

class InvalidConfigFileError(Exception):
    pass

class MrspecDatabaseEditor(object):
    
    def __init__(self, silent=False):
        self.silent = silent
        d = DatabaseConnection(sys.argv, silent)
        self.con = d.con
        self.cur = d.cur
        #metabolites that are aggregates of others
        self.met_to_calculate = {'tCr':['PCr','Cr'], 'tNAA':['NAA','NAAG'], 'tCho':['GPC','PCh'], 'Glx':['Gln','Glu']}
        
        #metabolites stored as dictionaries for performance reasons
        self.met_threshold = {'CrCH2':40, 'AcAc':40, 'Acn':40, 'Ala':40, 'Asp':40, 'Cho':20, 'Cr':30,
                         'GABA':40, 'GPC':40, 'Glc':40, 'Gln':40, 'Glu':30, 'Gua':40, 'Ins':30, 'Lac':40, 'Lip09':40,
                         'Lip13a':40, 'Lip13b':40, 'Lip20':40, 'MM09':40, 'MM12':40, 'MM14':40, 'MM17':40, 'MM20':40,
                         'NAA':20, 'NAAG':40, 'PCh':30, 'PCr':40, 'Scyllo':40, 'Tau':40, 'tCr':30, 'tNAA':20, 'tCho':20, 'Glx':30,'tCr_opt':30, 'tNAA_opt':20, 'tCho_opt':20, 'Glx_opt':30}
        
        self.high = '= 144'
        self.low = '< 50'
        self.both = 'IS NOT NULL'
        self.met_echo = {'CrCH2':self.high, 'AcAc':self.high, 'Acn':self.high, 'Ala':self.low, 'Asp':self.low, 'Cho':self.high, 'Cr':self.high,
                    'GABA':self.low, 'GPC':self.low, 'Glc':self.low, 'Gln':self.low, 'Glu':self.low, 'Gua':self.high, 'Ins':self.low, 'Lac':self.high, 'Lip09':self.low,
                    'Lip13a':self.low, 'Lip13b':self.low, 'Lip20':self.low, 'MM09':self.low, 'MM12':self.low, 'MM14':self.low, 'MM17':self.low, 'MM20':self.low,
                    'NAA':self.high, 'NAAG':self.low, 'PCh':self.low, 'PCr':self.low, 'Scyllo':self.low, 'Tau':self.low, 'tCr':self.high, 'tNAA':self.high, 'tCho':self.high, 'Glx':self.low, 'tCr_opt':self.high, 'tNAA_opt':self.high, 'tCho_opt':self.high, 'Glx_opt':self.low}
        
        self.load_default_thresholds()
        
        self.table = "standard"
        
        self.unique_desc = "DatabaseID"
        
        self.metadata = ['Scan_ID',
                    self.unique_desc,
                    "Indication",
                    "Diagnosis",
                    "ScanBZero",
                    "LocationName"
        ]        
    
    def __enter__(self):
        return self, self.con, self.cur
    
    def __exit__(self, type, value, traceback):
        self.con.close()
        if not self.silent: print('\nConnection to database closed.')

    def load_default_thresholds(self):
        m={}
        try:
            with open("config/metabolite_thresholds.txt", 'r') as met_file:
                for line in met_file:
                    (key, val) = line.split()
                    m[key] = int(val)
            if set(m)==set(self.met_echo):
                self.met_threshold=m
                print("Metabolite thresholds loaded from file.")
            else:
                raise InvalidConfigFileError(met_file.name+" is not a valid configuration file.")
        except Exception as e:
            print("Metabolite thresholds loaded from defaults. Reason: " +str(e)+".")
        
    def populate_SD_table(self, gender, field, location, return_single_scan_per_procedure, filter_by_sd, overlay):
        '''Populates a table in which the '''
        limit = 50
    
        cols,all_scans = self.execute_and_return_query(self.parse_query(ID='', Scan_ID='', age='', gender='', field='', location='', metabolites=self.met_threshold, limit='', 
                                                                    uxlimit='', lxlimit='', mets_span_each=False, return_single_scan_per_procedure=False, 
                                                                    filter_by_sd=True, keywords='', key_exclude='', 
                                                                    windowed_SD_threshold='',classification_code=''))
        
    
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
                    subquery = ''.join(['UPDATE {2} as S, (SELECT (CASE WHEN COUNT(T.{0})<2 THEN 0 ELSE ({1}-AVG(T.{0}))/STDDEV_SAMP(T.{0}) END) AS q FROM ('.format(column, all_scans[i][j], self.table), parse_query('','',age,gender,field,location,[name],limit,'','',True,return_single_scan_per_procedure,filter_by_sd,[],[],'',''),') AS T) AS Q set S.{}_SD=CAST(Q.q AS DECIMAL(11,6))where S.{}={}'.format(name,'Scan_ID',Scan_ID)])
    
                    self.cur.execute(subquery)
                    self.con.commit()
                j+=1
            i+=1
    
    #bug exists where could access scan information from later date because of use of coalesce if unique is True                     !!!                                        !!!!!
    def parse_query(self, ID, Scan_ID, age, gender, field, location, metabolites, limit, uxlimit, lxlimit, mets_span_each, return_single_scan_per_procedure, filter_by_sd, keywords, key_exclude, windowed_SD_threshold,classification_code):
        print "HEY"+str(self.met_threshold)
    
        ###compile columns to select in database###
        graph_data = [ ''.join([self.table,".AgeAtScan"]) ]
    
        location_names = ''
        if location:
            location_names = "AND {}.LocationName IN({})".format(self.table,location)
    
    
        if filter_by_sd:            
            graph_data.extend(["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}_%SD`<={1} AND `{0}_%SD`>0 AND {3}.ScanTEParameters {2} {4} {5} THEN {0} ELSE NULL END),',',1) as `{0}_Filtered`".format(metabolite, self.met_threshold[metabolite], self.met_echo[metabolite],self.table,location_names, 'AND (`{0}_SD` >= {1} OR `{0}_SD` <= -{1})'.format(metabolite,windowed_SD_threshold) if windowed_SD_threshold else '') for metabolite in metabolites])
        else:
            ##add feature to do windowed_sd_threshold?
            graph_data.extend(metabolites)
    
        graph_data.extend([met+'_SD' for met in self.met_threshold])    
        graph_data.extend([self.table + ".{}".format(m) for m in self.metadata])
    
        select = ','.join(graph_data)
    
        ###compile options for 'where': gender, field, location, met null and or###
    
        parsed_where = ''
        parsed_options = []
    
        ID = None if not ID else "'"+("','").join(ID.split(','))+"'"
        Scan_ID = None if not Scan_ID else "'"+("','").join(Scan_ID.split(','))+"'"
    
        constraints = {'Gender':gender, 'ScanBZero':field, self.unique_desc:ID, 'Scan_ID':Scan_ID}
        for constraint in constraints:
            if constraints[constraint]:
                parsed_options.append("{}.{} IN({})".format(self.table,constraint,
                                                            constraints[constraint]))
    
        ###keywords:search indication and diagnosis###
        if keywords:
            cond = []
            for keyword in keywords:
                cond += ["{3}.{1} LIKE '{0}' OR {3}.{2} LIKE '{0}' ".format(keyword, self.metadata[3],self.metadata[2], self.table)]
            parsed_keys = " AND ".join(cond)
            parsed_keys = ''.join(['(', parsed_keys, ')'])
            parsed_options.append(parsed_keys)
    
        ##keywords to exclude
        if key_exclude:
            cond = []
            for keyword in key_exclude:
                cond += ["{3}.{1} NOT LIKE '{0}' AND {3}.{2} NOT LIKE '{0}' ".format(keyword, self.metadata[3],self.metadata[2], self.table)]
            parsed_keys = " AND ".join(cond)
            parsed_keys = ''.join(['(', parsed_keys, ')'])
            parsed_options.append(parsed_keys)
    
        ##classification code
        if classification_code:
            cond = []
            for code in classification_code:
                cond += ["{2}.{1} LIKE '%{0}%'".format(code, '`Classification Code`', self.table)]
            parsed_keys = " AND ".join(cond)
            parsed_keys = ''.join(['(', parsed_keys, ')'])
            parsed_options.append(parsed_keys)    
    
        if uxlimit:
            parsed_options.append('{}.AgeAtScan < {}'.format(self.table, uxlimit))
        if lxlimit:
            parsed_options.append('{}.AgeAtScan > {}'.format(self.table, lxlimit))
    
        if parsed_options:
            parsed_where = 'WHERE ' + ' AND '.join(parsed_options)
    
        ###group by statement: unique. remove hashtag to make active again###
        if return_single_scan_per_procedure:
            group_by = 'GROUP BY {},AgeAtScan'.format(self.unique_desc)
        else:
            group_by = 'GROUP BY Scan_ID'
    
        ###finally, compile query###
        query = ''
    
        join = ""#LEFT JOIN `{1}` ON `{1}`.{2} = {3}.{2} AND `{1}`.AgeAtScan = {3}.AgeAtScan ".format(','.join(met_threshold.keys()), sd_table, unique_desc, table)
    
        #print "select: ", select
        #print "table: ", table
        #print "join: ", join
        #print "parsed_where: ", parsed_where
        #print "group_by: ", group_by
    
        ##limit_parser = {True: _parse_limit, False: _parse_no_limit}
        if not limit:
            query = "SELECT {} FROM {} {} {} {} ORDER BY {}".format(select, self.table, join, parsed_where, group_by,self.unique_desc)
        else:
            ###limit: limit###
            limit = 'LIMIT {}'.format(limit)
            if parsed_where:
                linker = 'AND'
            else: linker = "WHERE"
            where_less = parsed_where + ' {} {}.AgeAtScan < {}'.format(linker,self.table,age)
            where_geq = parsed_where + ' {} {}.AgeAtScan >= {}'.format(linker,self.table,age)
    
            query = "(SELECT {0} FROM {1} {6} {2} {3} ORDER BY {1}.AgeAtScan DESC {4}) UNION ALL (SELECT {0} FROM {1} {6} {5} {3} ORDER BY {1}.AgeAtScan {4})".format(select, self.table, where_less, group_by, limit, where_geq, join)
    
            #print "limit: ", limit
            #print "where_less: ", where_less
            #print "where_geq: ", where_geq
    
        #print "query: ", query
    
        return query    
        
    def prompt_yes_no(self, question, default="no"):
        """Ask a yes/no question via sys.stdin.readline() and return their answer.
    
        "question" is a string that is presented to the user.
        "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).
    
        The "answer" return value is True for "yes" or False for "no".
        """
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

    def insert_aggregate_metabolites_optimal(self, name, met_to_calculate):
        if self.table_exists(name):
            for met in met_to_calculate:
                can_calculate = True            
                for m in met_to_calculate[met]:
                    if not self.column_exists(name, m):
                        if not self.silent: print('Unable to calculate {}, metabolite {} value required.'.format(met,m))
                        can_calculate = False
    
                if can_calculate:                   
                    if self.column_exists(name, met+'_opt'):
                        self.cur.execute('UPDATE {} SET {} = NULL'.format(name, met+"_opt"))
                    else:
                        self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, met+'_opt', 'DECIMAL(11,6)'))
                    if self.column_exists(name, met+'_opt_%SD'):
                        self.cur.execute('UPDATE {} SET {} = NULL'.format(name, '`' + met+'_opt_%SD`'))
                    else:
                        self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, '`' + met+'_opt_%SD`', 'BIGINT(21)'))
                        
                        
                    added = ' + '.join(["sel."+ mm +"_opt" for mm in met_to_calculate[met]])
                    greatest = ','.join(["sel.`"+ mm + "_opt_%SD`" for mm in met_to_calculate[met]])
                    
                    not_zero = ''.join(['AND '," > 0.000001 AND ".join([ 'sel.' + mm + '_opt' for mm in met_to_calculate[met]]),' > 0.000001'])
    
                    subquery1 = "SELECT AgeAtScan,Scan_ID,ID," + ",".join(["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}`>0 AND {3}.ScanTEParameters {2} THEN {0} ELSE NULL END),',',1) as `{0}_opt`".format(mm, '100', self.met_echo[mm],name) for mm in met_to_calculate[met]]) + ' FROM {} GROUP BY AgeAtScan,{}'.format(name,self.unique_desc)
                    
                    if not self.silent: print('--------------FIX AGGMET OPT VALUES-----------------')
                    q = "UPDATE {0} T, ({4}) sel SET T.{1} = ({2}) WHERE T.AgeAtScan = sel.AgeAtScan AND T.ID=sel.ID".format(name, met +"_opt", added, '', subquery1)
                    if not self.silent: print(q)
                    self.cur.execute(q)
                    
                    subquery2 = "SELECT AgeAtScan,Scan_ID,ID," + ",".join(["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}_%SD`<={1} AND `{0}_%SD`>0 AND {3}.ScanTEParameters {2} THEN `{0}_%SD` ELSE NULL END),',',1) as `{0}_opt_%SD`".format(mm, '998', self.met_echo[mm], name) for mm in met_to_calculate[met]]) + ' FROM {} GROUP BY AgeAtScan,{}'.format(name,self.unique_desc)
                    
                    if not self.silent: print('--------------FIX AGGMET OPT SD-----------------')
                    q2 = "UPDATE {0} T, ({4}) sel SET T.{1} = GREATEST({2}) WHERE T.AgeAtScan = sel.AgeAtScan AND T.ID=sel.ID".format(name, '`' + met +"_opt_%SD`", greatest, '', subquery2)
                    if not self.silent: print(q2)
                    self.cur.execute(q2)
                    self.con.commit()
                    
    def insert_additional_metabolites(self, name, met_to_calculate):
        if self.table_exists(name):
            for met in met_to_calculate:
                can_calculate = True            
                for m in met_to_calculate[met]:
                    if not self.column_exists(name, m):
                        if not self.silent: print('Unable to calculate {}, metabolite {} value required.'.format(met,m))
                        can_calculate = False
                        
                if can_calculate:                   
                    if self.column_exists(name, met):                    
                        self.cur.execute('UPDATE {} SET {} = NULL'.format(name, met))
                    else:
                        self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, met, 'DECIMAL(11,6)'))
                    if self.column_exists(name, met+'_%SD'):
                        self.cur.execute('UPDATE {} SET {} = NULL'.format(name, '`'+met+"_%SD`"))
                    else:
                        self.cur.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(name, '`' + met+'_%SD`', 'BIGINT(21)'))
                    self.con.commit()
    
                    added = " + ".join(met_to_calculate[met])
                    least = ','.join(["`"+ mm + "_%SD`" for mm in met_to_calculate[met]])
    
                    not_zero = ''.join(['AND '," >= 0 AND ".join([ mm for mm in met_to_calculate[met]]), '>=0'])
                    
                    if not self.silent: print('--------------FIX AGGMET VALUES-----------------')
                    q = "UPDATE {0} as T SET {1} = ({2}) WHERE T.Scan_ID = Scan_ID {3}".format(name, met, added, not_zero)
                    if not self.silent: print(q)
                    self.cur.execute(q)
                    
                    if not self.silent: print('--------------FIX AGGMET SD-----------------')
                    q2="UPDATE {0} as T SET {1} = LEAST({2}) WHERE T.Scan_ID = Scan_ID {3}".format(name, '`'+met+"_%SD`", least, not_zero)
                    if not self.silent: print q2
                    self.cur.execute(q2)
                    self.con.commit()
    
    def execute_and_return_query(self, query, commit=False):
        '''Str, Bool -> [], [][]
        Executes the specified MySQL query, returning the result, with the option to commit changes to the database.'''
        self.cur.execute(query)
        if commit: self.con.commit()
        columns = [i[0] for i in self.cur.description]
        rows = self.cur.fetchall()
        return columns, rows    
    
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
                
            self.cur.execute("CREATE TABLE {} SELECT {} FROM {} {}".format(name, selection, source, group_by)) ##GROUP BY ID,AgeAtScan
            
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
        
    
    def table_exists(self, tablename):
        self.cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{}'".format(tablename.replace('\'', '\'\'')))
        if self.cur.fetchone()[0] == 1:
            return True
        return False
    
    def column_exists(self, tablename, column):
        self.cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}' AND column_name = '{}'".format(tablename.replace('\'', '\'\''), column.replace('\'', '\'\'')))
        if self.cur.fetchone()[0] == 1:
            return True
        return False
    
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
    
            if not self.silent: print("Table '{}' in '{}' successfully imported from '{}'.".format(name,'mrspec',f))


if __name__ == "__main__":

    #load table_schema from file, or load default
    table_schema = None
    try:
        with open("schema.csv", 'r') as csvfile:
            r = csv.reader(csvfile, delimiter = ',', quotechar = '|')
            table_schema = [line.split(',') for line in r]
        print("Table schema loaded from file.")
    except IOError as e:
        print("Table schema loaded from defaults. Reason: " +str(e)+".")

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
            ['ScanTEParameters', u],
            ['`Indication (as written on MRI requisition)`', t, 'Indication'],
            ['`Diagnosis (from chart)`', t, 'Diagnosis'],
            ['`Classification Code`', t]
                         ]
        
        update_table_schema= [['HSC_Number', t, 'ID']] + table_schema[1:]
        

        for metabolite in metabolites:
            table_schema += [[metabolite, d],['`' + metabolite + "_%SD`", s]]
            update_table_schema += [[metabolite, d],['`' + metabolite + "_SD`", s, '`' + metabolite + "_%SD`"]]
        

        
        #Specifies which information to be imported into sd tables
        sd_table_imports = ['ID', 'AgeAtScan', 'Gender', 'ScanBZero', 'LocationName', 'ScanTEParameters']
        #Specify which columns should be added with null values to be calculated later
        sd_table_nulls = []
        for metabolite in metabolites + ['tCr', 'tNAA', 'tCho', 'Glx']:
            sd_table_nulls.append([metabolite + '_SD', d])

    #Establish connection with database
    with MrspecDatabaseEditor() as (c,con,cur):
        
        if c.table_exists(c.table):
            print("'{}' already exists in database, no changes made.".format(c.table))
        else:
    
            #import requisite tables in local folder and mysql folder
            c.import_csv("outcomes2.csv", "outcomes", "varchar(500)")
            c.import_csv("mrspec.csv", "mrspec", "text")
            c.import_csv("tabPatients.csv", "tab_MRN", "varchar(50)")
        
            #re-personalize mrspec with MRN
            c.check_for_table_before_executing('mrspec_MRN', "CREATE TABLE IF NOT EXISTS mrspec_MRN SELECT m.*, t.HSC_Number as MRN FROM mrspec AS m JOIN tab_MRN as t ON m.tabPatient_ID=t.tabPatient_ID")
        
            #merge tables into table 'merged'
            cur.execute("ALTER TABLE outcomes DROP COLUMN tabPatient_ID") if c.column_exists("outcomes", "tabPatient_ID") else None
            c.check_for_table_before_executing("merged","CREATE TABLE OUTCOMES_GROUPED SELECT * FROM outcomes GROUP BY `MRN (column to be removed once study is in analysis phase)`,str_to_date(outcomes.Date, '%d/%m/%Y') ORDER BY str_to_date(outcomes.Date, '%d/%m/%Y')")
            con.commit()
        
            c.check_for_table_before_executing('merged', "CREATE TABLE IF NOT EXISTS merged SELECT * FROM OUTCOMES_GROUPED RIGHT JOIN mrspec_MRN ON OUTCOMES_GROUPED.`MRN (column to be removed once study is in analysis phase)` = mrspec_MRN.MRN AND str_to_date(OUTCOMES_GROUPED.Date, '%d/%m/%Y') = str_to_date(mrspec_MRN.procedureDate, '%y-%m-%d')")   
            
            ##
            #create standardized table
            c.create_standardized_table('standard', 'merged', table_schema, None, 'Scan_ID')
            
            ##update code##
            c.import_csv("updates.csv", 'updates', "text")
            c.check_for_table_before_executing('updates_merged', "CREATE TABLE IF NOT EXISTS updates_merged SELECT * FROM updates LEFT JOIN OUTCOMES_GROUPED ON OUTCOMES_GROUPED.`MRN (column to be removed once study is in analysis phase)` = updates.HSC_Number AND str_to_date(OUTCOMES_GROUPED.Date, '%d/%m/%Y') = str_to_date(updates.ProcedureDate, '%d/%m/%Y')")
            con.commit()
            cur.execute('alter table updates_merged add column AgeAtScan bigint(21)') if not c.column_exists('updates_merged','AgeAtScan') else None
            cur.execute("UPDATE updates_merged as T SET T.AgeAtScan = (TO_DAYS(STR_TO_DATE(T.ProcedureDate,'%d/%m/%Y')) - TO_DAYS(STR_TO_DATE(T.PatientBirthDay,'%d/%m/%Y'))) where T.Scan_ID = Scan_ID")
            con.commit()
            c.create_standardized_table("standard_update", 'updates_merged', update_table_schema, None, 'Scan_ID')# fulltexts)
            
            ##COMMENT THIS LINE OUT AFTER SCRIPT HAS RUN ONCE, otherwise you will get an error
            cur.execute('INSERT INTO standard SELECT * FROM standard_update')
            
            con.commit()
             

        print('\nAll operations completed successfully.')