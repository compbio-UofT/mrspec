import mysql.connector as m
import sys, random, time, os
from os import path
from flask import Flask, render_template, request, jsonify, json as j, send_file
import __main__ as main
from connection import *

#metabolites stored as dictionaries for performance reasons
met_threshold = {'CrCH2':40, 'AcAc':40, 'Acn':40, 'Ala':40, 'Asp':40, 'Cho':20, 'Cr':30,
                 'GABA':40, 'GPC':40, 'Glc':40, 'Gln':40, 'Glu':30, 'Gua':40, 'Ins':30, 'Lac':40, 'Lip09':40,
                 'Lip13a':40, 'Lip13b':40, 'Lip20':40, 'MM09':40, 'MM12':40, 'MM14':40, 'MM17':40, 'MM20':40,
                 'NAA':20, 'NAAG':40, 'PCh':30, 'PCr':40, 'Scyllo':40, 'Tau':40, 'tCr':30, 'tNAA':20, 'tCho':20, 'Glx':30,'tCr_opt':30, 'tNAA_opt':20, 'tCho_opt':20, 'Glx_opt':30}

high = '= 144'
low = '< 50'
both = 'IS NOT NULL'
met_echo_high = {'CrCH2':high, 'AcAc':high, 'Acn':high, 'Ala':low, 'Asp':low, 'Cho':high, 'Cr':high,
                 'GABA':low, 'GPC':low, 'Glc':low, 'Gln':low, 'Glu':low, 'Gua':high, 'Ins':low, 'Lac':high, 'Lip09':low,
                 'Lip13a':low, 'Lip13b':low, 'Lip20':low, 'MM09':low, 'MM12':low, 'MM14':low, 'MM17':low, 'MM20':low,
                 'NAA':high, 'NAAG':low, 'PCh':low, 'PCr':low, 'Scyllo':low, 'Tau':low, 'tCr':both, 'tNAA':both, 'tCho':both, 'Glx':low, 'tCr_opt':both, 'tNAA_opt':both, 'tCho_opt':both, 'Glx_opt':low}

table = "standard"

unique_desc = "DatabaseID"

metadata = ['Scan_ID',
    unique_desc,
    "Indication",
    "Diagnosis",
    "ScanBZero","LocationName"
]

# Initialize the Flask application
app = Flask(__name__)

#bug exists where could access scan information from later date
def default_query(ID, age, gender, field, location, metabolites, limit, mets_span_each, unique, filter_by_sd, keywords, key_exclude, perform_as_subquery_with):

    graph_data = [
        table + ".AgeAtScan"] + (metabolites if not filter_by_sd else ["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}_%SD`<={1} AND `{0}_%SD`>0 AND ScanTEParameters {2} THEN {0} ELSE NULL END),',',1) as `{0}_Filtered`".format(metabolite, met_threshold[metabolite], '= 144' if metabolite in met_echo_high else '<=50') for metabolite in metabolites])
    
    select = ','.join(graph_data + metadata)

    ###compile options for where: gender, field, location, met null and or###
    where = ''
    constraints = {'Gender':gender, 'ScanBZero':field, 'LocationName':location, unique_desc:ID}
    for constraint in constraints:
        if constraints[constraint]:
            where += " {} {} = '{}'".format(
                'AND' if where else 'WHERE',
                constraint,
                constraints[constraint])
    ##add location to where: location
    #where += " {} {} IN({})".format(
                            #'AND' if where else 'WHERE',
                            #'LocationName',
                            #location) if location else ''

    #where += " {} {} IN({})".format(
                            #'AND' if where else 'WHERE',
                            #unique_desc,
                            #ID) if ID else ''

    ##if mets_span_each, filter by standard deviation for each metabolite
    where += ' AND (' if where else 'WHERE ('
    where += " IS NOT NULL {} ".format('OR' if not mets_span_each else 'AND').join(
                metabolites)
    where += ' IS NOT NULL)'

    ##keywords
    if keywords:
        where += (' AND (' if where else 'WHERE (')
        cond = []
        for keyword in keywords:
            cond += ["{1} LIKE '{0}' OR {2} LIKE '{0}' ".format(keyword, metadata[1],metadata[2])]
        where += " OR ".join(cond)
        where += ") "

    ##keywords to exclude
    if key_exclude:
        where += (' AND (' if where else 'WHERE (')
        cond = []
        for key in key_exclude:
            cond += ["{1} NOT LIKE '{0}' OR {2} NOT LIKE '{0}' ".format(keyword, metadata[1],metadata[2])]
        where += " AND ".join(cond)
        where += ") "

    ###group by statement: unique###
    group_by = 'GROUP BY ' + unique_desc + (', AgeAtScan' if not unique else '')

    ###finally, compile query###
    query = ''
    if limit == '':
        query = "SELECT {} FROM {} {} {} ORDER BY AgeAtScan".format(select, table, where, group_by)
    else:
        ###limit: limit###
        limit = 'LIMIT {}'.format(limit)
        where_less = where + ' AND AgeAtScan < {}'.format(age)
        where_geq = where + ' AND AgeAtScan >= {}'.format(age)

        query = "(SELECT {0} FROM {1} {2} {3} ORDER BY AgeAtScan DESC {4}) UNION ALL (SELECT {0} FROM {1} {5} {3} ORDER BY AgeAtScan {4})".format(select, table, where_less, group_by, limit, where_geq)

    ##if performed as subquery, add parameters
    if perform_as_subquery_with:
        query = ''.join([perform_as_subquery_with[0],'(',query,') AS T ',perform_as_subquery_with[1]])
    #execute query
    ##print(query)
    cur.execute(query)

    rows = cur.fetchall()

    #columns = [i[0] for i in cur.description]
    return rows


def create_SD_table(gender, field, location, unique, filter_by_sd, overlay):
    '''Populates a table in which the '''
    #all_sd = []
    limit = 50

    a,all_patient_IDs = execute_query("SELECT AgeAtScan,{0} from {1} group by {0},AgeAtScan".format(unique_desc, table))

    i=0
    for row in all_patient_IDs:
        age = row[0]
        patient_ID = row[1]

        #subject_metabolites_query = default_query(patient_ID, age, "", "", location, met_threshold, "", False, False, filter_by_sd, [],[], []) 
        #obtain a list of all patients
        q = parse_query(patient_ID, age, '', '', location, met_threshold, '', 
                   '', '', True, 
                   True, filter_by_sd, '', 
                   '','')
        
        
        cols,subject_metabolites_query = execute_query(q)
        print cols,subject_metabolites_query

        j=0
        #iterate through all columns of the query (doesn't matter at what columns metabolites begin)
        for column in cols:
            #get metabolite name by truncating title if filtered
            name = column[:-9] if filter_by_sd else column

            if name in met_threshold and subject_metabolites_query[0][j] is not None:
                subquery = ''.join(['UPDATE {2} as S, (SELECT (CASE WHEN COUNT(T.{0})<2 THEN 0 ELSE ({1}-AVG(T.{0}))/STDDEV_SAMP(T.{0}) END) AS q FROM ('.format(column, subject_metabolites_query[0][j], table), parse_query('', age, gender, field, 
                                                              location, 
                                                              [name], 
                                                              limit, 
                                                              '', 
                                                              '', 
                                                              True, 
                                                              unique, 
                                                              filter_by_sd, 
                                                              [], 
                                                              [],''),') AS T) AS Q set S.{}_SD=CAST(Q.q AS DECIMAL(11,6))where S.{}={} AND S.AgeAtScan={}'.format(name,unique_desc,patient_ID,age)])
 

                cur.execute(subquery)
                con.commit()
                ##c,result=execute_query(subquery)

                ##patient_sd = 0 if result[0][2] <= 1 else (float(subject_metabolites_query[0][j]) - float(result[0][0]))/float(result[0][1]) #N = (X-mu)/sigma

                ##qq = "UPDATE {} SET {}=CAST({} AS DECIMAL(11,6)) WHERE {} = {} AND AgeAtScan = {}".format(sd_table, name + '_SD', patient_sd, unique_desc, patient_ID, age)
                #print qq + ';'
                ##cur.execute(qq)
                ###con.commit() ##necessary to reflect changes
                
                #sd.append({metabolite:int(patient_sd)})
            j+=1
        #all_sd.append(sd)
        i+=1
        
def windowed_SD(cols, query, gender, field, location, unique, filter_by_sd, overlay):
    '''Formats the standard deviation values for metadata to be passed to the front end.'''
    all_sd = []

    i=0
    for row in query:
        age = row[0]
        patient_ID = row[-len(metadata)]

        #subject_metabolites_query = default_query(patient_ID, age, "", "", location, met_threshold.keys(), "", False, False, filter_by_sd, [],[], [])

        sd = []

        j=2
        for column in cols[2:-len(metadata)]:
            #print column
            metabolite = column[:-3]
            if metabolite in met_echo_high and row[j] is not None:
                #subquery = ["SELECT AVG({0}), STDDEV_SAMP({0}),COUNT({0}) FROM ".format(column),""]

                #result = default_query('',age, gender, field, location, [metabolite], limit, True, unique,filter_by_sd, [],[],subquery)

                #print result
                #print subject_metabolites_query[0][i], result[0][0],result[0][1],result[0][2]
                #patient_sd = 0 if result[0][2] <= 1 else (float(subject_metabolites_query[0][j]) - float(result[0][0]))/float(result[0][1]) #N = (X-mu)/sigma

                ##patient_sd = random.randint(-4,4)

                sd.append({metabolite: float(row[j])})
            j+=1
        all_sd.append(sd)
        i+=1

    if overlay == 0:
        all_sd += [[]]

    return all_sd

##refactored to work with DataTables join
def windowed_SD2(cols, query, gender, field, location, unique, filter_by_sd, overlay):
    '''Formats the standard deviation values for metadata to be passed to the front end.'''
    all_sd = {}

    i=0
    for row in query:
        age = row[0]
        patient_ID = row[-len(metadata)+1]

        #subject_metabolites_query = default_query(patient_ID, age, "", "", location, met_threshold.keys(), "", False, False, filter_by_sd, [],[], [])

        sd = []

        j=2
        for column in cols[2:-len(metadata)]:
            #print column
            metabolite = column[:-3]
            if metabolite in met_echo_high and row[j] is not None:
                #subquery = ["SELECT AVG({0}), STDDEV_SAMP({0}),COUNT({0}) FROM ".format(column),""]

                #result = default_query('',age, gender, field, location, [metabolite], limit, True, unique,filter_by_sd, [],[],subquery)

                #print result
                #print subject_metabolites_query[0][i], result[0][0],result[0][1],result[0][2]
                #patient_sd = 0 if result[0][2] <= 1 else (float(subject_metabolites_query[0][j]) - float(result[0][0]))/float(result[0][1]) #N = (X-mu)/sigma

                ##patient_sd = random.randint(-4,4)

                sd.append({metabolite: float(row[j])})
            j+=1
        all_sd[str(row[-len(metadata)])] = sd
        i+=1

    if overlay == 0:
        all_sd[''] = []

    return all_sd


def windowed_SD_dynamic(cols, query, gender, field, location, unique, filter_by_sd, overlay):
    '''Dynamically calculates standard deviation depending on the partition of the database being selected.'''
    all_sd = []
    limit = 50

    i=0
    for row in query:
        age = row[0]
        patient_ID = row[-len(metadata)]

        subject_metabolites_query = default_query(patient_ID, age, "", "", location, met_threshold.keys(), "", False, False, filter_by_sd, [],[], [])

        sd = []

        j=0
        for column in cols:
            metabolite = column[:-9]
            if metabolite in met_echo_high and subject_metabolites_query[0][j] is not None:
                subquery = ["SELECT AVG({0}), STDDEV_SAMP({0}),COUNT({0}) FROM ".format(column),""]

                result = default_query('',age, gender, field, location, [metabolite], limit, True, unique,filter_by_sd, [],[],subquery)

                #print result
                #print subject_metabolites_query[0][i], result[0][0],result[0][1],result[0][2]
                patient_sd = 0 if result[0][2] <= 1 else (float(subject_metabolites_query[0][j]) - float(result[0][0]))/float(result[0][1]) #N = (X-mu)/sigma

                ##patient_sd = random.randint(-4,4)

                sd.append({metabolite:int(patient_sd)})
            j+=1
        all_sd.append(sd)
        i+=1

    if overlay == 0:
        all_sd += [[]]

    return all_sd

def windowed_SD_notworking(cols, query, gender, field, location, unique, filter_by_sd, overlay):
    '''NOT FULLY IMPLEMENTED
    Does the same thing as windowed_SD_dynamic except it should be more efficient as it queries the database only once.'''
    #complete query
    all_sd = []
    limit = 50
    parsed_query = ''
    
    
    all_queries = []    
    met_threshold = []

    for row in query:
        age = row[0]
        patient_ID = row[-len(metadata)]
        
        mets_to_compare = []
        sd_queries = []        

        sd = []

        for i in range(1,len(cols)-len(metadata)):
            #print i, row[i], cols[i]
            
            if row[i] is not None:
                met = cols[i][:-9]
                mets_to_compare.append({met: row[i]})
                
                subquery = parse_query('', age, gender, field, location, 
                                      [met], 
                                      limit, 
                                      True, 
                                      unique, 
                                      filter_by_sd, 
                                      [], 
                                      [])
                query = "SELECT AVG({0}), STDDEV_SAMP({0}),COUNT({0}) FROM ({1}) AS T".format(cols[i],subquery)
                sd_queries.append(query)                
                
        if sd_queries:
            pt_queries = '(' + ')\n\t UNION ALL \n\t('.join(sd_queries) + ')'            
            all_queries.append(pt_queries)
            met_threshold.append(mets_to_compare)
            
            
        #print pt_queries
        #print all_queries
        #print met_threshold
            #union()
        #assert len(met_threshold) == len(all_queries)
        
    all_queries =  '\nUNION ALL \n'.join(all_queries)
    
    #print all_queries
            
    c,rows = execute_query(all_queries)
        
            #j=0
            #for m in mets_to_compare:
                #print rows[j],rows[j][0],rows[j][1],rows[j][2],mets_to_compare[m]

                #patient_sd = 0 if rows[j][2] <= 1 else (float(mets_to_compare[m]) - float(rows[j][0]))/float(rows[j][1]) #N = (X-mu)/sigma


                #sd.append({m:int(patient_sd)})
                #j+=1
                
        #all_sd.append(sd)

    #if overlay == 0:
        #all_sd += [[]]

    return all_sd

'''SELECT
    Scores.Date, Scores.Keyword, Scores.Score,
    (N * Sum_XY - Sum_X * Sum_Y)/(N * Sum_X2 - Sum_X * Sum_X) AS Slope
FROM Scores
INNER JOIN (
    SELECT
        Keyword,
        COUNT(*) AS N,
        SUM(CAST(Date as float)) AS Sum_X,
        SUM(CAST(Date as float) * CAST(Date as float)) AS Sum_X2,
        SUM(Score) AS Sum_Y,
        SUM(Score*Score) AS Sum_Y2,
        SUM(CAST(Date as float) * Score) AS Sum_XY
    FROM Scores
    GROUP BY Keyword
) G ON G.Keyword = Scores.Keyword;'''

def execute_query(query, commit=False):
    '''Str, Bool -> [], [][]
    Executes the specified MySQL query, returning the result, with the option to commit changes to the database.'''
    cur.execute(query)
    if commit: con.commit()
    columns = [i[0] for i in cur.description]
    rows = cur.fetchall()
    return columns, rows

#bug exists where could access scan information from later date because of use of coalesce if unique is True
def parse_query(ID, age, gender, field, location, metabolites, limit, uxlimit, lxlimit, mets_span_each, unique, filter_by_sd, keywords, key_exclude, windowed_SD_threshold):
        
    graph_data = [table + ".AgeAtScan"]
    
    location_names = ''
    if location:
        location_names = "AND {}.LocationName IN({})".format(table,location)
    
    #faster than list concatenation
    graph_data.extend(metabolites if not filter_by_sd else ["SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN `{0}_%SD`<={1} AND `{0}_%SD`>0 AND {3}.ScanTEParameters {2} {4} THEN {0} ELSE NULL END),',',1) as `{0}_Filtered`".format(metabolite, met_threshold[metabolite], met_echo_high[metabolite],table,location_names) for metabolite in metabolites])

    ##
    graph_data.extend([met+'_SD' for met in met_threshold])    
    
    graph_data.extend([table + ".{}".format(m) for m in metadata])
        
    select = ','.join(graph_data)

    ###compile options for where: gender, field, location, met null and or###
        
    parsed_where = ''
    parsed_options = []
    
    ID = None if not ID else "'"+("','").join(ID.split(','))+"'"
        
    constraints = {'Gender':gender, 'ScanBZero':field, unique_desc:ID}
    for constraint in constraints:
        if constraints[constraint]:
            parsed_options.append("{}.{} IN({})".format(table,constraint,
                constraints[constraint]))

    ##if mets_span_each, filter by standard deviation for each metabolite
    #where += ' AND (' if where else 'WHERE ('
    #where += " IS NOT NULL {} ".format('OR' if not mets_span_each else 'AND').join(
                #metabolites)
    #where += ' IS NOT NULL)'

    ##keywords
    if keywords:
        cond = []
        for keyword in keywords:
            cond += ["{3}.{1} LIKE '{0}' OR {3}.{2} LIKE '{0}' ".format(keyword, metadata[3],metadata[2], table)]
        parsed_keys = " AND ".join(cond)
        parsed_keys = ''.join(['(', parsed_keys, ')'])
        parsed_options.append(parsed_keys)

    ##keywords to exclude
    if key_exclude:
        cond = []
        for keyword in keywords:
            cond += ["{3}.{1} NOT LIKE '{0}' AND {3}.{2} NOT LIKE '{0}' ".format(keyword, metadata[3],metadata[2], table)]
        parsed_keys = " AND ".join(cond)
        parsed_keys = ''.join(['(', parsed_keys, ')'])
        parsed_options.append(parsed_keys)

    if uxlimit:
        parsed_options.append('{}.AgeAtScan < {}'.format(table, uxlimit))
    if lxlimit:
        parsed_options.append('{}.AgeAtScan > {}'.format(table, lxlimit))
    
    #select data that lies outside of a SD threshold
    if windowed_SD_threshold:
        thresholds = ' OR '.join(['`{0}_SD` >= {1} OR `{0}_SD` <= -{1}'.format(mm,windowed_SD_threshold) for mm in metabolites])
        parsed_options.append(thresholds)
        
    if parsed_options:
        parsed_where = 'WHERE '
        parsed_where += ' AND '.join(parsed_options)
            
    ###group by statement: unique###
    group_by = ''.join(['GROUP BY ', unique_desc, ', AgeAtScan' if not unique else ''])

    ###finally, compile query###
    query = ''
    
    join = ""#LEFT JOIN `{1}` ON `{1}`.{2} = {3}.{2} AND `{1}`.AgeAtScan = {3}.AgeAtScan ".format(','.join(met_threshold.keys()), sd_table, unique_desc, table)

    #print "select: ", select
    #print "table: ", table
    #print "join: ", join
    #print "parsed_where: ", parsed_where
    #print "group_by: ", group_by
    
    ##limit_parser = {True: _parse_limit, False: _parse_no_limit}
    if limit == '':
        query = "SELECT {} FROM {} {} {} {} ORDER BY {}".format(select, table, join, parsed_where, group_by,unique_desc)
    else:
        ###limit: limit###
        limit = 'LIMIT {}'.format(limit)
        if parsed_where:
            linker = 'AND'
        else: linker = "WHERE"
        where_less = parsed_where + ' {} {}.AgeAtScan < {}'.format(linker,table,age)
        where_geq = parsed_where + ' {} {}.AgeAtScan >= {}'.format(linker,table,age)

        query = "(SELECT {0} FROM {1} {6} {2} {3} ORDER BY {1}.AgeAtScan DESC {4}) UNION ALL (SELECT {0} FROM {1} {6} {5} {3} ORDER BY {1}.AgeAtScan {4})".format(select, table, where_less, group_by, limit, where_geq, join)

        #print "limit: ", limit
        #print "where_less: ", where_less
        #print "where_geq: ", where_geq

    #print "query: ", query
        
    return query

#adds patient as separate dataseries
##updated for use with DataTable joining
def format_query_with_pseries_and_names(query, columns, values, legend, overlay):
    #values = values.split(",")
    ##print rows, columns, values

    #print values
    qq = {}

    for i,column in enumerate(columns[1:]):
        ##print(i, column)
        q = {}
        cols = []
        rows = []
        
        #removed from cols
        ##[{'id': "", 'label': "", 'type': 'number'} for aa in range(0, overlay)]

        cols += [{'id': "Age", 'label': "Age", 'type': 'number'}] + \
                [{'id': "DatabaseID", 'label':'DatabaseID', "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } }] + \
                [{'id': column, 'label': column + '_' + '_'.join([str(legend_val) for legend_val in legend]), 'type': 'number'}] + \
                [{'id': "Scan_ID",'label':'Scan_ID', "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } }]
                

        for row in query:
            ##print(i,row[i+1])
            vals = [{'v': str(row[0])},{'v':str(row[-len(metadata)+1])},{'v': float(row[i+1]) if row[i+1] is not None else None},{'v': str(row[-len(metadata)])}]
            rows.append({'c':vals})

        #add patient data as its own data series
        if overlay == 0:
            rows.append({'c':[{'v': values[0]},{'v': None},{'v':float(values[i+1])} ]})
            cols.append({'id': "Patient Data", 'label': "Patient Data", 'type': 'number'})


        q['rows'] = rows
        q['cols'] = cols

        qq[column] = q


    return qq
#{column: q for column in columns[1:-1]}

#adds patient as separate dataseries
##
def format_query_with_pseries(query, columns, values, legend):
    #values = values.split(",")
    ##print rows, columns, values

    #print values

    q = {}
    cols = []
    rows = []

    columns.append("Patient Data")

    for column in columns:
        cols.append({'id': column, 'label': column + '_' + '_'.join([str(legend_val) for legend_val in legend]), 'type': 'number'})

    for row in query:
        vals = [{'v': str(value)} for value in row[:len(columns)-1]]
        rows.append({'c':vals})

    for val in values[1:]:
        rows.append({'c':[{'v': values[0]}] + [{'v': None} for value in row[1:len(columns)-1]]+[{'v':str(val)}]})

    q['rows'] = rows
    q['cols'] = cols

    return q

def format_metadata(query, overlay):
    array = []
    for row in query:
        array.append([{metadata[i]:r} for i, r in enumerate(row[-len(metadata):])])

    if overlay == 0:
        array += [[{'Query Patient':''}]]

    return array

def format_metadata2(query, overlay):
    array = {}
    for row in query:
        #array.append([{metadata[i]:r} for i, r in enumerate(row[-len(metadata):])])
        array[str(row[-len(metadata)])] = [{metadata[i]:r} for i, r in enumerate(row[-len(metadata):])]

    if overlay == 0:
        array[''] = [{'Query Patient':'test'}]

    return array

#adds patient as separate dataseries and prepares separate graphs for each with tooltips
def format_query_with_pseries_names_tooltips(query, columns, values):
    #values = values.split(",")
    ##print rows, columns, values

    #print values
    qq = {}

    #print columns
    for i,column in enumerate(columns[1:]):
        q = {}
        cols = []
        rows = []

        cols.append({'id': "Age", 'label': "Age", 'type': 'number'})
        cols.append({'id': column, 'label': column, '+': 'number'})
        cols.append({"id": None, "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } } )
        cols.append({'id': "Patient Data", 'label': "Patient Data", 'type': 'number'})
        cols.append({"id": None, "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } } )


        for row in query:
            vals = [{'v': float(row[1])},{'v': float(row[i+1])},{'v':row[0] if row[0] != '' else "No indication available."},{'v': None}, {'v': None}]
            rows.append({'c':vals})
            #add patient data as its own data series
            rows.append({'c':[{'v': values[0]},{'v': None},{'v': None},{'v':float(values[i+1])},{'v':None}]})

        q['rows'] = rows
        q['cols'] = cols

        qq[column] = q

    return qq

#adds patient as separate dataseries, for merged graphs
def format_query_with_pseries_tooltips(query, columns, values):
    #values = values.split(",")
    #print columns, values

    #print values

    q = {}
    cols = [{'id': columns[0], 'label': columns[0], 'type': 'number'}] #rendered as domain
    rows = []

    columns.append("Patient Data")
    for column in columns[1:]:
        cols.append({'id': column, 'label': column, 'type': 'number'})
        cols.append({"id": None, "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } } )

    #print cols

    for row in query:
        vals = []
        for value in row[1:]:
            vals += [{'v': float(value)},{'v': row[0] if row[0] != '' else "No indication available."}]
        vals += [{'v': None},{'v':None}] #column reserved for query patient dataseries + tooltip
        rows.append({'c':vals})

    for val in values[1:]:
        rows.append({'c':[{'v': values[0]}] + [{'v': None} for value in row[2:]]+[{'v':float(val)},{'v':None}]})

    q['rows'] = rows
    q['cols'] = cols

    return q

#adds patient as an extra data point
def format_query_with_point(query, columns, values):
    #values = values.split(",")
    ##print rows, columns, values

    #print(values)

    q = {}
    cols = []
    rows = []

    for column in columns:
        cols.append({'id': column, 'label': column, 'type': 'number'})

    for row in query:
        vals = [{'v': float(value)} for value in row]
        rows.append({'c':vals})


    rows.append({'c':[{'v': float(val)} for val in values]})

    q['rows'] = rows
    q['cols'] = cols

    return q

#does not include patient data
def format_query(query, columns, values):
    #values = values.split(",")
    ##print rows, columns, values

    q = {}
    cols = []
    rows = []

    for column in columns:
        cols.append({'id': column, 'label': column, 'type': 'number'})

    for row in query:
        values = [{'v': float(value)} for value in row]
        rows.append({'c':values})

    q['rows'] = rows
    q['cols'] = cols

    return q

@app.route('/img/<name>.<ext>')
def return_image(name, ext):
    return send_file('img/'+name+'.'+ext, mimetype='image/gif')

# This route will show a form to perform an AJAX request
# jQuery is loaded to execute the request and update the
# value of the operation
@app.route('/')
def index():
    return render_template('index.html')

# Route that will process the AJAX request, sum up two
# integer numbers (defaulted to zero) and return the
# result as a proper JSON response (Content-Type, etc.)
@app.route('/_add_numbers')
def add_numbers():
    ID = request.args.get('ID', 0, type=str)
    #ID = '' if not ID else ''.join(ID.split(','))
    
    metabolites = request.args.get('metabolites', 0, type=str) #metabolites
    values = request.args.get('values', 0, type=str) #values
    merge = request.args.get('merge', 0, type=str)
    age = request.args.get('age', 0, type=int)
    gender = request.args.get('gender', 0, type=str)
    field = request.args.get('field', 0, type=str)
    filter_by_sd=True
    unique=False
    location = request.args.get('location', '', type=str)
    overlay = request.args.get('overlay', 0, type=int)
    calc_sd = True
    #print(ID)
    k_inc = request.args.get('keywords', 0, type=str)
    k_exc = request.args.get('key_exclude', 0, type=str)
    keywords = k_inc if not k_inc else k_inc.split(',')
    key_exclude = k_exc if not k_exc else k_exc.split(',')
    windowed_SD_threshold = request.args.get('windowed_SD_threshold',0,type=str)
    
    #temporary workaround while figuring out what to do with patient data
    overlay = 1
    
    query = parse_query(ID=ID,
        age=age,
        gender=gender,
        field=field,
        metabolites=metabolites.split(','),
        limit=request.args.get('limit', 0, type=str),
        uxlimit=request.args.get('uxlimit', 0, type=str),
        lxlimit=request.args.get('lxlimit', 0, type=str),
        location = location,
        mets_span_each=False,
        unique=unique,
        filter_by_sd=filter_by_sd,
        keywords=keywords,
        key_exclude = key_exclude, windowed_SD_threshold=windowed_SD_threshold)
    
    print query
    
    cols,q = execute_query(query)
        
    sd_array = windowed_SD2(cols, q, gender, field, location, unique, filter_by_sd, overlay)

    # Set default values for legend
    if not gender:
        gender = 'Both'
    if not field:
        field = 'Both'
    if not location:
        location = 'Any'

    if merge == 'true':
        d = {metabolites:format_query_with_pseries(q,
                                                   ("Age," + metabolites).split(','),
                                                   (str(age) + "," + values).split(","),
                                                   [gender, field, location])}
    else:
        d = format_query_with_pseries_and_names(q,
                                                ("Age," + metabolites).split(','),
                                                (str(age) + "," + values).split(","),
                                                [gender, field, location],
                                                overlay)

    return jsonify(result=d,
                   names = [metabolites] if merge == "true" else metabolites.split(','),
                   metadata_array=format_metadata2(q,overlay),
                   sd_array = sd_array)

if __name__ == '__main__':
    # Reload Flask app when template file changes
    # http://stackoverflow.com/questions/9508667/reload-flask-app-when-template-file-changes
    extra_dirs = ['templates',]
    extra_files = extra_dirs[:]
    for extra_dir in extra_dirs:
        for dirname, dirs, files in os.walk(extra_dir):
            for filename in files:
                filename = path.join(dirname, filename)
                if path.isfile(filename):
                    extra_files.append(filename)
    
    #establish connection to database
    with DatabaseConnection(sys.argv) as (con,cur):
        #Launch app if script was called from commandline
        if is_run_from_commandline():
            app.run(
                host="0.0.0.0",
                port=int(8081),
                debug=True,
                extra_files=extra_files
            )
            
        #Otherwise, execute custom code for debugging
        else:
            ###Sandbox for testing###            
            
            start = time.clock()
            


            #create_SD_table('', '', '', True, True, None)

    

            end = time.clock()
            
            print end-start
