import mysql.connector as m
import sys, random, time
from flask import Flask, render_template, request, jsonify, json as j, send_file

'''all_mets = ['CrCH2', 'AcAc', 'Acn', 'Ala', 'Asp', 'Cho', 'Cr', 'GABA', 'GPC', 'Glc',
        'Gln', 'Glu', 'Gua', 'Ins', 'Lac', 'Lip09', 'Lip13a', 'Lip13b', 'Lip20', 'MM09',
        'MM12', 'MM14', 'MM17', 'MM20', 'NAA', 'NAAG', 'PCh', 'PCr', 'Scyllo', 'Tau',
        'tCr', 'tNAA', 'tCho', 'Glx'] '''

#metabolites stored as dictionaries for performance reasons
met_threshold = {'CrCH2':40, 'AcAc':40, 'Acn':40, 'Ala':40, 'Asp':40, 'Cho':20, 'Cr':30,
                 'GABA':40, 'GPC':40, 'Glc':40, 'Gln':40, 'Glu':30, 'Gua':40, 'Ins':30, 'Lac':40, 'Lip09':40,
                 'Lip13a':40, 'Lip13b':40, 'Lip20':40, 'MM09':40, 'MM12':40, 'MM14':40, 'MM17':40, 'MM20':40,
                 'NAA':20, 'NAAG':40, 'PCh':30, 'PCr':40, 'Scyllo':40, 'Tau':40, 'tCr':30, 'tNAA':20, 'tCho':20, 'Glx':30}

##met_echo_high = ["CrCH2", "AcAc", "Acn", "Cho", "Cr", "Gua", "Lac", "NAA", "tCr", "tNAA", "tCho"] #Cr = tCr for 1.5T
#met_echo_high = {'CrCH2':40, 'AcAc':40, 'Acn':40, 'Cho':20, 'Cr':30, 'Gua':40, 'Lac':40, 'NAA':20, 'tCr':30, 'tNAA':20, 'tCho':20}
#met_echo_high = {'CrCH2':high, 'AcAc':high, 'Acn':high, 'Cho':high, 'Cr':high, 'Gua':high, 'Lac':high, 'NAA':high, 'tCr':high, 'tNAA':high, 'tCho':high}
high = '=144'
low = '<50'
met_echo_high = {'CrCH2':high, 'AcAc':high, 'Acn':high, 'Ala':low, 'Asp':low, 'Cho':high, 'Cr':high,
                 'GABA':low, 'GPC':low, 'Glc':low, 'Gln':low, 'Glu':low, 'Gua':high, 'Ins':low, 'Lac':high, 'Lip09':low,
                 'Lip13a':low, 'Lip13b':low, 'Lip20':low, 'MM09':low, 'MM12':low, 'MM14':low, 'MM17':low, 'MM20':low,
                 'NAA':high, 'NAAG':low, 'PCh':low, 'PCr':low, 'Scyllo':low, 'Tau':low, 'tCr':high, 'tNAA':high, 'tCho':high, 'Glx':low}

#met_echo_low = [met for met in all_mets if met not in met_echo_high]

table = "standard"
unique_desc = "ID"

metadata = [
    unique_desc,
    "Indication",
    "Diagnosis",
    "ScanBZero"
]

# Initialize the Flask application
app = Flask(__name__)

def windowed_SD(cols, query, gender, field, location, unique, filter_by_sd, overlay):
    #complete query
    all_sd = []
    limit = 50

    for row in query:
        age = row[0]
        patient_ID = row[-len(metadata)]
        
        mets_to_compare = {}
        sd_queries = []        

        sd = []

        for i in range(1,len(cols)-len(metadata)):
            #print i, row[i], cols[i]
            
            if row[i] is not None:
                met = cols[i][:-9]
                mets_to_compare[met] = row[i]
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
                
        all_queries = '(' + ') UNION ALL ('.join(sd_queries) + ')'
        print patient_ID, all_queries
            
        if all_queries != '()':
            c,rows = execute_query(all_queries)
        
            j=0
            for m in mets_to_compare:
                print rows[j],rows[j][0],rows[j][1],rows[j][2],mets_to_compare[m]

                patient_sd = 0 if rows[j][2] <= 1 else (float(mets_to_compare[m]) - float(rows[j][0]))/float(rows[j][1]) #N = (X-mu)/sigma


                sd.append({m:int(patient_sd)})
                j+=1
                
        all_sd.append(sd)

    if overlay == 0:
        all_sd += [[]]

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

def execute_query(query):
    cur.execute(query)
    columns = [i[0] for i in cur.description]
    rows = cur.fetchall()
    return columns, rows

#bug exists where could access scan information from later date
def parse_query(ID, age, gender, field, location, metabolites, limit, mets_span_each, unique, filter_by_sd, keywords, key_exclude):
        
    graph_data = ["AgeAtScan"]
    #faster than list concatenation
    graph_data.extend(metabolites if not filter_by_sd else ["COALESCE(CASE WHEN `{0}_%SD`<={1} AND `{0}_%SD`>=0 AND ScanTEParameters {2} THEN {0} ELSE NULL END) as `{0}_Filtered`".format(metabolite, met_threshold[metabolite], met_echo_high[metabolite]) for metabolite in metabolites])
    graph_data.extend(metadata)
    
    select = ','.join(graph_data)

    ###compile options for where: gender, field, location, met null and or###
        
    parsed_where = ''
    parsed_options = []
        
    constraints = {'Gender':gender, 'ScanBZero':field, 'LocationName':location}
    for constraint in constraints:
        if constraints[constraint]:
            parsed_options.append("{} = '{}'".format(constraint,
                constraints[constraint]))
            
    ##add ID to where
    if ID:
        parsed_options.append("{} IN({})".format(unique_desc,ID))

    ##if mets_span_each, filter by standard deviation for each metabolite
    #where += ' AND (' if where else 'WHERE ('
    #where += " IS NOT NULL {} ".format('OR' if not mets_span_each else 'AND').join(
                #metabolites)
    #where += ' IS NOT NULL)'

    ##keywords
    if keywords:
        cond = []
        for keyword in keywords:
            cond += ["{1} LIKE '{0}' OR {2} LIKE '{0}' ".format(keyword, metadata[1],metadata[2])]
        parsed_keys = " AND ".join(cond)
        parsed_keys = ''.join(['(', parsed_keys, ')'])
        parsed_options.append(parsed_keys)

    ##keywords to exclude
    if key_exclude:
        cond = []
        for key in key_exclude:
            cond += ["{1} NOT LIKE '{0}' OR {2} NOT LIKE '{0}' ".format(keyword, metadata[1],metadata[2])]
        parsed_keys = " AND ".join(cond)
        parsed_keys = ''.join(['(', parsed_keys, ')'])
        parsed_options.append(parsed_keys)
        
    if parsed_options:
        parsed_where = 'WHERE '
        parsed_where += ' AND '.join(parsed_options)
            
    ###group by statement: unique###
    group_by = ''.join(['GROUP BY ', unique_desc, ', AgeAtScan' if not unique else ''])

    ###finally, compile query###
    query = ''
    ##limit_parser = {True: _parse_limit, False: _parse_no_limit}
    if limit == '':
        query = "SELECT {} FROM {} {} {} ORDER BY AgeAtScan".format(select, table, parsed_where, group_by)
    else:
        ###limit: limit###
        limit = 'LIMIT {}'.format(limit)
        if parsed_where:
            linker = 'AND'
        else: linker = "WHERE"
        where_less = parsed_where + ' {} AgeAtScan < {}'.format(linker,age)
        where_geq = parsed_where + ' {} AgeAtScan >= {}'.format(linker,age)

        query = "(SELECT {0} FROM {1} {2} {3} ORDER BY AgeAtScan DESC {4}) UNION ALL (SELECT {0} FROM {1} {5} {3} ORDER BY AgeAtScan {4})".format(select, table, where_less, group_by, limit, where_geq)
    ###print query    
    
    return query

#SELECT `lap_time`, `uid` FROM `table` t1 WHERE `lap_time` =< 120 AND NOT EXISTS (SELECT 1 FROM `table` WHERE `uid` = t1.`uid` AND `lap_time` > t1.`lap_time` AND `lap_time` < 120) ORDER BY `lap_time` DESC LIMIT 5

#adds patient as separate dataseries
##
def format_query_with_pseries_and_names(query, columns, values, overlay):
    #values = values.split(",")
    ##print rows, columns, values

    #print values
    qq = {}

    for i,column in enumerate(columns[1:]):
        ##print(i, column)
        q = {}
        cols = []
        rows = []

        cols += [{'id': "Age", 'label': "Age", 'type': 'number'}] + [{'id': "", 'label': "", 'type': 'number'} for aa in range(0, overlay)] + [{'id': column, 'label': column, 'type': 'number'}]

        for row in query:
            ##print(i,row[i+1])
            vals = [{'v': str(row[0])}]+[{'v':None} for nn in range(0,overlay)]+[{'v': str(row[i+1])}]
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
def format_query_with_pseries(query, columns, values):
    #values = values.split(",")
    ##print rows, columns, values

    #print values

    q = {}
    cols = []
    rows = []

    columns.append("Patient Data")

    for column in columns:
        cols.append({'id': column, 'label': column, 'type': 'number'})

    for row in query:
        vals = [{'v': str(value)} for value in row[:len(columns)-1]]
        rows.append({'c':vals})

    for val in values[1:]:
        rows.append({'c':[{'v': values[0]}] + [{'v': None} for value in row[1:len(columns)-1]]+[{'v':float(val)}]})

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
        cols.append({'id': column, 'label': column, 'type': 'number'})
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
    b = request.args.get('b', 0, type=str) #metabolites
    c = request.args.get('c', 0, type=str) #values
    merge = request.args.get('merge', 0, type=str)
    age = request.args.get('age', 0, type=int)
    gender = request.args.get('gender', 0, type=str)
    field = request.args.get('field', 0, type=str)
    filter_by_sd=True
    unique=True
    location = request.args.get('location', '', type=str)
    overlay = request.args.get('overlay', 0, type=int)
    calc_sd = True

    k_inc = request.args.get('keywords', 0, type=str)
    k_exc = request.args.get('key_exclude', 0, type=str)
    keywords = k_inc if not k_inc else k_inc.split(',')
    key_exclude = k_exc if not k_exc else k_exc.split(',')
    
    if calc_sd:
    
        asdf = parse_query(ID='',
        age=age,
        gender=gender,
        field=field,
        metabolites=met_threshold.keys(),
        limit=request.args.get('limit', 0, type=str),
        location = location,
        mets_span_each=False,
        unique=unique,
        filter_by_sd=filter_by_sd,
        keywords=keywords,
        key_exclude = key_exclude)
    
        cols,q = execute_query(asdf)
        
        sd_array = windowed_SD(cols, q, gender, field, location, unique, filter_by_sd, overlay)        
    
    else:
        asdf = parse_query(ID='',
        age=age,
        gender=gender,
        field=field,
        metabolites=b.split(','),
        limit=request.args.get('limit', 0, type=str),
        location = location,
        mets_span_each=False,
        unique=unique,
        filter_by_sd=filter_by_sd,
        keywords=keywords,
        key_exclude = key_exclude)
    
        cols,q = execute_query(asdf)
        
        sd_array = None            
        

    if merge == 'true':
        d = {b:format_query_with_pseries(q, ("Age," + b).split(','), (str(age) + "," + c).split(","))}
    else:
        d = format_query_with_pseries_and_names(q, ("Age," + b).split(','), (str(age) + "," + c).split(","), overlay)

    return jsonify(result=d, names = [b] if merge == "true" else b.split(','), metadata_array=format_metadata(q,overlay), sd_array = sd_array)

if __name__ == '__main__':

    if sys.argv[0][-8:] == 'query.py':
        #Initialize connection to MySQL database
        #usage: python query.py <user> <password>
        con = m.connect(user=sys.argv[1], password=sys.argv[2],
                        database='mrspec')
        cur = con.cursor()
        #initialize the Flask application
        app.run(
            host="0.0.0.0",
            port=int("8080"),
            debug=True
        )
    else:
        #Initialize connection to MySQL database
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

        #example query
        
        start = time.clock()
        
        for i in range(0,100):
            asdf = parse_query(ID='',age=500, gender="", field="", metabolites=['Cr','Tau','GPC'], limit='50', location="", mets_span_each=True, unique=True, filter_by_sd=True, keywords=[], key_exclude = [])
            col,q = execute_query(asdf)
        
        

        


        ##print j.dumps(format_query_with_pseries(q, 'Age,Acn'.split(","), "500,0.5".split(",")))
        ##print j.dumps(format_query_with_pseries_and_names(q, 'Age,Acn,Cr'.split(","), "500,0.5,6".split(",")), indent=1)

        ##print q

        w = windowed_SD(col,q, gender="F", field="", location="", unique=True, filter_by_sd=True, overlay=0)
        end = time.clock()
        
        print end-start        

       # #print j.dumps(w, indent=1)
        ##print bool(len(w) == len(format_metadata(q,0)))

        #close connection to database
        con.close()