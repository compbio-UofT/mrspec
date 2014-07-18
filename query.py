import mysql.connector as m
import sys
from flask import Flask, render_template, request, jsonify, json as j

all_mets = ['CrCH2', 'AcAc', 'Acn', 'Ala', 'Asp', 'Cho', 'Cr', 'GABA', 'GPC', 'Glc',
        'Gln', 'Glu', 'Gua', 'Ins', 'Lac', 'Lip09', 'Lip13a', 'Lip13b', 'Lip20', 'MM09',
        'MM12', 'MM14', 'MM17', 'MM20', 'NAA', 'NAAG', 'PCh', 'PCr', 'Scyllo', 'Tau',
        'tCr', 'tNAA', 'tCho', 'Glx']

met_threshold = {'CrCH2':40, 'AcAc':40, 'Acn':40, 'Ala':40, 'Asp':40, 'Cho':20, 'Cr':30,
                 'GABA':40, 'GPC':40, 'Glc':40, 'Gln':40, 'Glu':30, 'Gua':40, 'Ins':30, 'Lac':40, 'Lip09':40,
                 'Lip13a':40, 'Lip13b':40, 'Lip20':40, 'MM09':40, 'MM12':40, 'MM14':40, 'MM17':40, 'MM20':40,
                 'NAA':20, 'NAAG':40, 'PCh':30, 'PCr':40, 'Scyllo':40, 'Tau':40, 'tCr':30, 'tNAA':20, 'tCho':20, 'Glx':30}

met_echo_high = ["CrCH2", "AcAc", "Acn", "Cho", "Cr", "Gua", "Lac", "NAA", "tCr", "tNAA", "tCho"] #Cr = tCr for 1.5T

#met_echo_low = [met for met in all_mets if met not in met_echo_high]

table = "standard"
unique_desc = "ID"

metadata = [
    unique_desc,
    "Indication",
    "Diagnosis"
]

# Initialize the Flask application
app = Flask(__name__)

def windowed_SD(query, gender, field, location, unique, filter_by_sd):
    all_sd = []
    limit = 50

    i=0
    for row in query:
        age = row[0]
        patient_ID = row[-len(metadata)]

        subject_metabolites_query = default_query(patient_ID, age, "", "", location, all_mets, "", False, False, filter_by_sd, [],[], [])

        sd = []

        j=0
        for column in [k[0] for k in cur.description]:
            metabolite = column[:-9]
            if metabolite in all_mets and subject_metabolites_query[0][j] is not None:
                subquery = ["SELECT AVG({0}), STDDEV_SAMP({0}),COUNT({0}) FROM ".format(column),""]

                result = default_query('',age, gender, field, location, [metabolite], limit, True, unique,
                             filter_by_sd, [],[],
                             subquery)
                #print result
                #print subject_metabolites_query[0][i], result[0][0],result[0][1],result[0][2]
                patient_sd = 0 if result[0][2] <= 1 else (float(subject_metabolites_query[0][j]) - float(result[0][0]))/float(result[0][1]) #N = (X-mu)/sigma

                sd.append({metabolite:patient_sd})
            j+=1
        all_sd.append(sd)
        i+=1

    return all_sd



#bug exists where could access scan information from later date
def default_query(ID, age, gender, field, location, metabolites, limit, mets_span_each, unique, filter_by_sd, keywords, key_exclude, perform_as_subquery_with):

    graph_data = [
        "AgeAtScan"] + (metabolites if not filter_by_sd else ["COALESCE(CASE WHEN `{0}_%SD`<={1} AND `{0}_%SD`>0 AND ScanTEParameters {2} THEN {0} ELSE NULL END) as `{0}_Filtered`".format(metabolite, met_threshold[metabolite], '= 144' if metabolite in met_echo_high else '<=50') for metabolite in metabolites])

    #print(age, gender, field, location, metabolites, limit, mets_span_each, unique, filter_by_sd, keywords, key_exclude, perform_as_subquery_with)

    select = ','.join(graph_data + metadata)

    ###compile options for where: gender, field, location, met null and or###
    where = ''
    constraints = {'Gender':gender, 'ScanBZero':field}
    for constraint in constraints:
        if constraints[constraint]:
            where += " {} {} = '{}'".format(
                'AND' if where else 'WHERE',
                constraint,
                constraints[constraint])
    ##add location to where: location
    where += " {} {} IN({})".format(
                            'AND' if where else 'WHERE',
                            'LocationName',
                            location) if location else ''

    where += " {} {} IN({})".format(
                            'AND' if where else 'WHERE',
                            unique_desc,
                            ID) if ID else ''

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

    ##unique people
    #select AgeAtScan, GROUP_CONCAT(CASE WHEN 'Acn_%SD'<40 AND MRN = standard.MRN AND AgeAtScan = standard.AgeAtScan then Acn else null end ) as Acn_Filtered, LocationName from standard where LocationName IN('BG', 'OCC_WM') GROUP BY MRN ORDER BY AgeAtScan limit 5;
    ##not unique
    #select AgeAtScan, GROUP_CONCAT(CASE WHEN 'Acn_%SD'<40 AND MRN = standard.MRN then Acn else NULL end) as Acn_Filtered, LocationName from standard where LocationName IN('BG', 'OCC_WM') GROUP BY MRN, AgeAtScan ORDER BY AgeAtScan limit 5;

    #select AgeAtScan, GROUP_CONCAT(ScanTEParameters) as Echo, GROUP_CONCAT(LocationName) as Loc, GROUP_CONCAT(ScanBZero) as T, GROUP_CONCAT(Acn) as Acn, GROUP_CONCAT(`Acn_%SD`) as `Acn_%SD`, GROUP_CONCAT(CASE WHEN 'Acn_%SD'<=40 AND 'Acn_%SD'>0 AND MRN = standard.MRN AND AgeAtScan = standard.AgeAtScan THEN Acn ELSE NULL END) as Acn_Filtered from standard where LocationName IN('BG', 'OCC_WM') GROUP BY MRN ORDER BY AgeAtScan limit 5;

    #SELECT * FROM (select GROUP_CONCAT(AgeAtScan) as Age, GROUP_CONCAT(ScanTEParameters) as Echo, GROUP_CONCAT(LocationName) as Loc, GROUP_CONCAT(ScanBZero) as T, GROUP_CONCAT(Cr) as Cr, GROUP_CONCAT(`Cr_%SD`) as `Cr_%SD`, COALESCE(CASE WHEN `Cr_%SD`<=20 AND `Cr_%SD`>0 AND ScanTEParameters = 144 AND MRN = standard.MRN THEN Cr ELSE NULL END) as `Cr_Filtered` from standard where LocationName IN('BG', 'OCC_WM') GROUP BY MRN ORDER BY AgeAtScan) as Q where `Cr_Filtered` IS NOT NULL;

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
    print(query)
    cur.execute(query)

    rows = cur.fetchall()

    #columns = [i[0] for i in cur.description]
    return rows

#SELECT `lap_time`, `uid` FROM `table` t1 WHERE `lap_time` =< 120 AND NOT EXISTS (SELECT 1 FROM `table` WHERE `uid` = t1.`uid` AND `lap_time` > t1.`lap_time` AND `lap_time` < 120) ORDER BY `lap_time` DESC LIMIT 5

#adds patient as separate dataseries
##
def format_query_with_pseries_and_names(query, columns, values):
    #values = values.split(",")
    #print rows, columns, values

    print values
    qq = {}

    for i,column in enumerate(columns[1:]):
        #print(i, column)
        q = {}
        cols = []
        rows = []

        cols.append({'id': "Age", 'label': "Age", 'type': 'number'})
        cols.append({'id': column, 'label': column, 'type': 'number'})
        cols.append({'id': "Patient Data", 'label': "Patient Data", 'type': 'number'})

        for row in query:
            #print(i,row[i+1])
            vals = [{'v': str(row[0])},{'v': str(row[i+1])}] + [{'v': None}]
            rows.append({'c':vals})

        #add patient data as its own data series

        rows.append({'c':[{'v': values[0]},{'v': None},{'v':float(values[i+1])} ]})

        q['rows'] = rows
        q['cols'] = cols

        qq[column] = q


    return qq
#{column: q for column in columns[1:-1]}

#adds patient as separate dataseries
##
def format_query_with_pseries(query, columns, values):
    #values = values.split(",")
    #print rows, columns, values

    print values

    q = {}
    cols = []
    rows = []

    columns.append("Patient Data")

    for column in columns:
        cols.append({'id': column, 'label': column, 'type': 'number'})

    for row in query:
        vals = [{'v': str(value)} for value in row[:len(columns)-1]] + [{'v': None}]
        rows.append({'c':vals})

    for val in values[1:]:
        rows.append({'c':[{'v': values[0]}] + [{'v': None} for value in row[1:len(columns)-1]]+[{'v':float(val)}]})

    q['rows'] = rows
    q['cols'] = cols

    return q

def format_metadata(query):
    array = []
    for row in query:
        array.append([{metadata[i]:r} for i, r in enumerate(row[-len(metadata):])])

    return array

#adds patient as separate dataseries and prepares separate graphs for each with tooltips
def format_query_with_pseries_names_tooltips(query, columns, values):
    #values = values.split(",")
    #print rows, columns, values

    print values
    qq = {}

    print columns
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
    print columns, values

    print values

    q = {}
    cols = [{'id': columns[0], 'label': columns[0], 'type': 'number'}] #rendered as domain
    rows = []

    columns.append("Patient Data")
    for column in columns[1:]:
        cols.append({'id': column, 'label': column, 'type': 'number'})
        cols.append({"id": None, "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } } )

    print cols

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
    #print rows, columns, values

    print(values)

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
    #print rows, columns, values

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

    k_inc = request.args.get('keywords', 0, type=str)
    k_exc = request.args.get('key_exclude', 0, type=str)
    keywords = k_inc if not k_inc else k_inc.split(',')
    key_exclude = k_exc if not k_exc else k_exc.split(',')
    #keywords = request.args.get('keywords', 0, type=str) if request.args.get('keywords', 0, type=str).split(",") else request.args.get('keywords', 0, type=str).split(",").split(",")
    #key_exclude = request.args.get('key_exclude', 0, type=str) if request.args.get('key_exclude', 0, type=str).split(",") else request.args.get('key_exclude', 0, type=str).split(",").split(",")

    print(keywords, key_exclude)


    q = default_query(ID='',
        age=age,
        gender=request.args.get('gender', 0, type=str),
        field=request.args.get('field', 0, type=str),
        metabolites=b.split(','),
        limit=request.args.get('limit', 0, type=str),
        location = '',
        mets_span_each=True,
        unique=False,
        filter_by_sd=True,
        keywords=keywords,
        key_exclude =key_exclude, perform_as_subquery_with=[])

    if merge == 'true':
        d = {b:format_query_with_pseries(q, ("Age," + b).split(','), (str(age) + "," + c).split(","))}
    else:
        d = format_query_with_pseries_and_names(q, ("Age," + b).split(','), (str(age) + "," + c).split(","))

    return jsonify(result=d, names = [b] if merge == "true" else b.split(','), metadata_array=format_metadata(q) )

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
        q = default_query(ID='',age=500, gender="F", field="", metabolites=['Cr'], limit='50', location="", mets_span_each=True, unique=True, filter_by_sd=True, keywords=[], key_exclude = [], perform_as_subquery_with=[])


        #print j.dumps(format_query_with_pseries_names_tooltips(q, 'Age,Acn,Cr'.split(","), "500,0.5,6".split(",")), indent=1)
        #print j.dumps(format_query_with_pseries_and_names(q, 'Age,Acn,Cr'.split(","), "500,0.5,6".split(",")), indent=1)

        #print j.dumps(format_metadata(q), indent=1)
        print q

        w = windowed_SD(q, gender="F", field="", location="", unique=True, filter_by_sd=True)

        print j.dumps(w)
        print bool(len(w) == len(format_metadata(q)))

        #close connection to database
        con.close()