import mysql.connector as m
import sys, random, time, os
from os import path
from flask import Flask, render_template, request, jsonify, json as j, send_file
import __main__ as main
from connection import is_run_from_commandline, prompt_yes_no
from query_database import *

# Initialize the Flask application
app = Flask(__name__)
c = None

@app.before_first_request
def establish_database_connection():
    global c
    c=MrspecDatabaseQueryer()
    
def format_legend(column,legend):
    parsed_labels = []
    for label in legend:
        if label != '':
            parsed_labels.append(label)
    if parsed_labels:
        return column + ": " + ", ".join(parsed_labels)
    else:
        return column

##updated for use with DataTable joining
def format_query_with_names(query, columns, values, legend):
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
                [{'id': column, 'label': format_legend(column,legend), 'type': 'number'}] + \
                [{'id': "Scan_ID",'label':'Scan_ID', "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } }]
                
        for row in query:
            ##print(i,row[i+1])
            vals = [{'v': str(row[0])},{'v':str(row[-len(c.metadata)+1])},{'v': float(row[i+1]) if row[i+1] is not None else None},{'v': str(row[-len(c.metadata)])}]
            rows.append({'c':vals})

        ##add patient data as its own data series
        #if overlay == 0:
            #rows.append({'c':[{'v': values[0]},{'v': None},{'v':float(values[i+1])} ]})
            #cols.append({'id': "Patient Data", 'label': "Patient Data", 'type': 'number'})

        q['rows'] = rows
        q['cols'] = cols

        qq[column] = q

    return qq

##
def format_query(query, columns, values, legend):

    cols = [{'id': "Age", 'label': "Age", 'type': 'number'}] + \
        [{'id': "DatabaseID", 'label':'DatabaseID', "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } }]
    rows = []
    q={}

    for i,column in enumerate(columns[1:]):
        ##print(i, column)

        #removed from cols
        ##[{'id': "", 'label': "", 'type': 'number'} for aa in range(0, overlay)]

        cols += [{'id': column, 'label': format_legend(column,legend), 'type': 'number'}] + \
            [{'id': "Scan_ID",'label':'Scan_ID', "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } }]

    for row in query:
        ##print(i,row[i+1])
        vals = [{'v': str(row[0])},{'v':str(row[-len(c.metadata)+1])}] 
        for i in range(1,len(columns)):
            vals+=[{'v': float(row[i]) if row[i] is not None else None},{'v': str(row[-len(c.metadata)])}]
        rows.append({'c':vals})

    ##add patient data as its own data series
    #if overlay == 0:
        #rows.append({'c':[{'v': values[0]},{'v': None},{'v':float(values[i+1])} ]})
        #cols.append({'id': "Patient Data", 'label': "Patient Data", 'type': 'number'})

    q['rows'] = rows
    q['cols'] = cols

    #qq[column] = q

    return q

def format_metadata(query):
    array = {}
    for row in query:
        #array.append([{metadata[i]:r} for i, r in enumerate(row[-len(metadata):])])
        array[str(row[-len(c.metadata)])] = [{c.metadata[i]:r} for i, r in enumerate(row[-len(c.metadata):])]

    return array

##refactored to work with DataTables join
def windowed_SD(cols, query, gender, field, location, unique, filter_by_sd):#, overlay):
    '''Formats the standard deviation values for metadata to be passed to the front end.'''
    all_sd = {}

    i=0
    for row in query:
        age = row[0]
        Scan_ID = str(row[-len(c.metadata)])

        sd = []

        j=2
        for column in cols[2:-len(c.metadata)]:
            #print column, row[j]
            metabolite = column[:-3]
            if metabolite in c.met_echo and row[j] is not None:
                #subquery = ["SELECT AVG({0}), STDDEV_SAMP({0}),COUNT({0}) FROM ".format(column),""]

                #result = default_query('',age, gender, field, location, [metabolite], limit, True, unique,filter_by_sd, [],[],subquery)

                #print result
                #print subject_metabolites_query[0][i], result[0][0],result[0][1],result[0][2]
                #patient_sd = 0 if result[0][2] <= 1 else (float(subject_metabolites_query[0][j]) - float(result[0][0]))/float(result[0][1]) #N = (X-mu)/sigma

                ##patient_sd = random.randint(-4,4)

                sd.append({metabolite: float(row[j])})
            j+=1
        all_sd[Scan_ID] = sd
        i+=1

    #if overlay == 0:
        #all_sd[''] = []

    return all_sd

@app.route('/img/<name>.<ext>')
def return_image(name, ext):
    return send_file('img/'+name+'.'+ext, mimetype='image/gif')

@app.route('/js/<name>.<ext>')
def return_js(name, ext):
    return send_file('js/'+name+'.'+ext, mimetype='text/javascript')

@app.route('/config/<name>.<ext>')
def return_txt(name, ext):
    return send_file('config/'+name+'.'+ext, mimetype='text')

@app.route('/css/<name>.<ext>')
def return_css(name, ext):
    return send_file('css/'+name+'.'+ext, mimetype='text/css')

@app.route('/')
def index():
    return render_template('index.html')

#change default metabolite thresholds    
@app.route('/_alter_thresholds')
def alter_thresholds():
    r = j.loads(request.args.get('thresholds', '', type=str))
    c.met_threshold = r
    print  r
    print c.met_threshold
    return jsonify(data=None)

#change default metabolite echotimes    
@app.route('/_alter_echotimes')
def alter_echotimes():
    r = j.loads(request.args.get('echotimes', '', type=str))
    c.met_echo = r
    print r
    print c.met_echo
    return jsonify(data=None)

@app.route('/_get_query')
def get_query():
    '''Process the query from the frontend.
    '''
    metabolites = request.values.getlist('metabolites') #metabolites
    values = request.args.get('values', 0, type=str) #values
    merge = request.args.get('merge', 0, type=str)
    age = request.args.get('age', 0, type=int)
    gender = request.args.get('gender', 0, type=str)
    field = request.args.get('field', 0, type=str)
    location = request.args.get('location', '', type=str)
    
    filter_by_sd=True
    return_single_scan_per_procedure=False
    calc_sd = True
    
    diagnosis = request.args.get('diagnosis', 0, type=str)
    diagnosis = diagnosis if not diagnosis else diagnosis.split(',')
    
    diagnosis_exclude = request.args.get('diagnosis_exclude', 0, type=str)
    diagnosis_exclude = diagnosis_exclude if not diagnosis_exclude else diagnosis_exclude.split(',')
    
    indication = request.args.get('indication', 0, type=str)
    indication = indication if not indication else indication.split(',')
    
    indication_exclude = request.args.get('indication_exclude', 0, type=str)
    indication_exclude = indication_exclude if not indication_exclude else indication_exclude.split(',')
    
    treatment = request.args.get('treatment', 0, type=str)
    treatment = treatment if not treatment else treatment.split(',')

    anesthesia = request.args.get('anesthesia', 0, type=str)
    anesthesia = anesthesia if not anesthesia else anesthesia.split(',')
    
    windowed_SD_threshold = request.args.get('windowed_SD_threshold',0,type=str)
    
    detailed_legend = request.args.get('legend', 0, type=str)
    
    query = c.parse_query(
        ID=request.args.get('ID', 0, type=str),
        Scan_ID=request.args.get('Scan_ID', 0, type=str),
        ID_exclude=request.args.get('ID_exclude', 0, type=str),
        Scan_ID_exclude=request.args.get('Scan_ID_exclude', 0, type=str),
        treatment=treatment,
        age=age,
        gender=gender,
        field=field,
        metabolites=metabolites,
        limit=request.args.get('limit', 0, type=str),
        uxlimit=request.args.get('uxlimit', 0, type=str),
        lxlimit=request.args.get('lxlimit', 0, type=str),
        location = location,
        mets_span_each=False,
        return_single_scan_per_procedure=return_single_scan_per_procedure,
        filter_by_sd=filter_by_sd,
        diagnosis=diagnosis,
        diagnosis_exclude = diagnosis_exclude, 
        windowed_SD_threshold=windowed_SD_threshold, 
        classification_code=request.args.getlist('classification_code'), 
        indication=indication,
        indication_exclude=indication_exclude,
        anesthesia=anesthesia)
    
    cols,q = c.execute_and_return_query(query)
        
    sd_array = windowed_SD(cols, q, gender, field, location, return_single_scan_per_procedure, filter_by_sd)

    legend = [field, location]
    if detailed_legend == 'true':
        legend.append(gender)
        if windowed_SD_threshold:
            legend.append(''.join([u"\u00B1",windowed_SD_threshold," SD"]))

    if merge == 'true':
        d = {','.join(metabolites):format_query(q,
                                                ['Age']+metabolites,
                                                (str(age) + "," + str(values)).split(","),
                                                legend)}
    else:
        d = format_query_with_names(q,
                                    ['Age']+metabolites,
                                    (str(age) + "," + str(values)).split(","),
                                    legend)

    return jsonify(result=d,
                   names = [','.join(metabolites)] if merge == "true" else metabolites,
                   metadata_array=format_metadata(q),
                   sd_array = sd_array)

if __name__ == '__main__':
    # Reload Flask app when template file changes. App must be run with use_reloader=True to work
    # http://stackoverflow.com/questions/9508667/reload-flask-app-when-template-file-changes
    extra_dirs = ['templates','js','config',]
    extra_files = extra_dirs[:]
    for extra_dir in extra_dirs:
        for dirname, dirs, files in os.walk(extra_dir):
            for filename in files:
                filename = path.join(dirname, filename)
                if path.isfile(filename):
                    extra_files.append(filename)
    
    #Launch app if script was called from commandline
    if is_run_from_commandline():
        app.run(
            host="0.0.0.0",
            port=int(8081),
            debug=True,
            extra_files=extra_files,
            use_reloader=True
        )
    #Otherwise, execute custom code for debugging
    else:
        with MrspecDatabaseQueryer() as (c,con,cur):
            pass
            ###Sandbox for testing###            

