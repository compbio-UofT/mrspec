from connection import *
import sys

class MrspecDatabaseQueryer(object):

    def __init__(self, silent=False,database='mrspec'):
        #whether or not to print status messages
        self.silent = silent
        #establish database connection
        c = DatabaseConnection(sys.argv, silent, database)
        self.con = c.con
        self.cur = c.cur
        
        #metabolites read directly from mrspec data
        self._base_metabolites = ['CrCH2', 'AcAc', 'Acn', 'Ala', 'Asp', 'Cho', 'Cr', 'GABA', 'GPC', 'Glc',
                            'Gln', 'Glu', 'Gua', 'Ins', 'Lac', 'Lip09', 'Lip13a', 'Lip13b', 'Lip20', 'MM09',
                            'MM12', 'MM14', 'MM17', 'MM20', 'NAA', 'NAAG', 'PCh', 'PCr', 'Scyllo', 'Tau']
        
        #metabolites that are calculated by adding others together
        self.met_to_calculate = {'tCr':['PCr','Cr'], 'tNAA':['NAA','NAAG'], 'tCho':['GPC','PCh'], 'Glx':['Gln','Glu']}        
        
        #all metabolites that will be present in the database
        self.queryable_metabolites = self._base_metabolites + \
            self.met_to_calculate.keys() + \
            [m+'_opt' for m in self.met_to_calculate.keys()]
        
        #set metabolite standard deviation quality thresholds
        self.met_threshold = {'CrCH2':40, 'AcAc':40, 'Acn':40, 'Ala':40, 'Asp':40, 'Cho':20, 'Cr':30,
                         'GABA':40, 'GPC':40, 'Glc':40, 'Gln':40, 'Glu':30, 'Gua':40, 'Ins':30, 'Lac':40, 'Lip09':40,
                         'Lip13a':40, 'Lip13b':40, 'Lip20':40, 'MM09':40, 'MM12':40, 'MM14':40, 'MM17':40, 'MM20':40,
                         'NAA':20, 'NAAG':40, 'PCh':30, 'PCr':40, 'Scyllo':40, 'Tau':40, 'tCr':30, 'tNAA':20, 'tCho':20, 'Glx':30,'tCr_opt':30, 'tNAA_opt':20, 'tCho_opt':20, 'Glx_opt':30}
        self.load_default_thresholds()
        
        #set metabolite preferred echo times
        self.high = '=144'
        self.low = '<50'
        self.both = 'IS NOT NULL'
        self.met_echo = {'CrCH2':self.high, 'AcAc':self.high, 'Acn':self.high, 'Ala':self.low, 'Asp':self.low, 'Cho':self.high, 'Cr':self.high,
                    'GABA':self.low, 'GPC':self.low, 'Glc':self.low, 'Gln':self.low, 'Glu':self.low, 'Gua':self.high, 'Ins':self.low, 'Lac':self.high, 'Lip09':self.low,
                    'Lip13a':self.low, 'Lip13b':self.low, 'Lip20':self.low, 'MM09':self.low, 'MM12':self.low, 'MM14':self.low, 'MM17':self.low, 'MM20':self.low,
                    'NAA':self.high, 'NAAG':self.low, 'PCh':self.low, 'PCr':self.low, 'Scyllo':self.low, 'Tau':self.low, 'tCr':self.high, 'tNAA':self.high, 'tCho':self.high, 'Glx':self.low, 'tCr_opt':self.high, 'tNAA_opt':self.high, 'tCho_opt':self.high, 'Glx_opt':self.low}
        self.load_default_echotimes()
              
        self.table = "standard"
        
        self.unique_desc = "tabPatient_ID"
        
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
            if set(m)==set(self.queryable_metabolites):
                self.met_threshold = m
                print("Metabolite thresholds loaded from file.")
            else:
                raise InvalidConfigFileError(met_file.name + " is not a valid configuration file.")
        except Exception as e:
            print("Metabolite thresholds loaded from defaults. Reason: " +str(e)+".")
        
    def load_default_echotimes(self):
        m={}
        try:
            with open("config/metabolite_echotimes.txt", 'r') as met_file:
                for line in met_file:
                    (key, val) = line.split()
                    m[key] = val
            if set(m)==set(self.queryable_metabolites):
                self.met_echo = m
                print("Metabolite echotimes loaded from file.")
            else:
                raise InvalidConfigFileError(met_file.name + " is not a valid configuration file.")
        except Exception as e:
            print("Metabolite echotimes loaded from defaults. Reason: " +str(e)+".")
            
    def table_exists(self, tablename):
        self.cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{}' AND table_schema='{}'".format(tablename.replace('\'', '\'\''),self._database))
        if self.cur.fetchone()[0] == 1:
            return True
        return False

    def column_exists(self, tablename, column):
        self.cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}' AND column_name = '{}' AND table_schema='{}'".format(tablename.replace('\'', '\'\''), column.replace('\'', '\'\''),self._database))
        if self.cur.fetchone()[0] == 1:
            return True
        return False

    def execute_and_return_query(self, query):
        '''Str, Bool -> [], [][]
        Executes the specified MySQL query, returning the result, with the option to commit changes to the database.'''
        self.cur.execute(query)
        columns = [i[0] for i in self.cur.description]
        rows = self.cur.fetchall()
        return columns, rows
    
    #bug exists where could access scan information from later date because of use of coalesce if unique is True                     !!!                                        !!!!!
    def parse_query(self, ID, Scan_ID, age, gender, field, location, metabolites, limit, uxlimit, lxlimit, mets_span_each, return_single_scan_per_procedure, filter_by_sd, keywords, key_exclude, windowed_SD_threshold,classification_code,extended=True):

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

        if extended:
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

            query = "(SELECT {0} FROM {1} {6} {2} {3} ORDER BY {1}.AgeAtScan DESC {4}) UNION ALL (SELECT {0} FROM {1} {6} {5} {3} ORDER BY {1}.AgeAtScan {4}) ".format(select, self.table, where_less, group_by, limit, where_geq, join)

        return query