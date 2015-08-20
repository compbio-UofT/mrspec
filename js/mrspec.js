function sendThresholds(){

    var thresholds={}

    $('.met').each(function(){
        thresholds[this.name]=parseInt( this.value )
    })

    $.getJSON('/_alter_thresholds', {
        thresholds:JSON.stringify(thresholds)
    }, function(data) {
    });
}


function loadThresholds(){

    $.get("config/metabolite_thresholds.txt", function(response) {
     var lines = response.split('\n');
     for(line in lines){
        var line_split = lines[line].split(' ');
        $("input[name='"+line_split[0]+"']").val(line_split[1] )
    }
});
}

function sendEchotimes(){

    var echotimes={}

    $('.e_met').each(function(){
        echotimes[this.name]= this.value
    })

    $.getJSON('/_alter_echotimes', {
        echotimes:JSON.stringify(echotimes)
    }, function(data) {
    });
}

function loadEchotimes(){

    $.get("config/metabolite_echotimes.txt", function(response) {
     var lines = response.split('\n');
     for(line in lines){
        var line_split = lines[line].split(' ');
        $("input[name='e_"+line_split[0]+"']").val(line_split[1] )
    }
});
}

function calculateAge(){
    var years = $('#years').val()
    var months = $('#months').val()
    var weeks = $('#weeks').val()
    var days = $('#days').val()

    var total = parseInt(days) + parseInt(weeks)*7 + parseInt(months)*30 + parseInt(years)*365

    $("input[name='age']").val(total)

}

function inputValid(){
    var num_list = /^[0-9.]+(,[0-9.]+)*$/i;
    var opt_num_list = /^([0-9]+(,[0-9]+)*)*$/i

    /*if ($('input[name="values"]').val().search(num_list) < 0){
        alert('You entered\n\n' +$('input[name="values"]').val()+ '\n\nPlease enter metabolite values separated by dashes.')
        return false
    }*/

    if ($('input[name="DatabaseID"]').val().search(opt_num_list) < 0){
        alert('You entered\n\n' +$('input[name="DatabaseID"]').val()+ '\n\nPlease enter ID values separated by dashes.')
        return false
    }

    if ($('input[name="limit"]').val().search(/^[0-9]*$/) < 0){
        alert('Limit must be a number.')
        return false
    }

    if ($('input[name="age"]').val().search(/^[0-9]*$/) < 0){
        alert('Age must be a number.')
        return false
    }

    return true
}

function exportToCsv(){
    for (result in window.my_config.results){
        data = window.my_config.results[result]

        csv = google.visualization.dataTableToCsv(data)

        csv_cols = []
        // Iterate columns
        for (var i=0; i<data.getNumberOfColumns(); i++) {
            // Replace any commas in column labels
            csv_cols.push(data.getColumnLabel(i).replace(/,/g,""));
        }

        // Create column row of CSV
        csv = csv_cols.join(",")+"\r\n" + csv;

        downloadCsv(csv, result)
    }
}

function exportChartsAsPng(){
    for (chart in window.my_config.charts){
        downloadPNG(window.my_config.charts[chart].getImageURI(), chart)
    }
}

function downloadPNG (png_out, name) {

    var data = atob( png_out.substring( "data:image/png;base64,".length ) ),
    asArray = new Uint8Array(data.length);

    for( var i = 0, len = data.length; i < len; ++i ) {
        asArray[i] = data.charCodeAt(i);    
    }

    var blob = new Blob( [ asArray.buffer ], {type: "image/png"} );

    var url  = window.URL || window.webkitURL;
    var link = document.createElementNS("http://www.w3.org/1999/xhtml", "a");
    link.href = url.createObjectURL(blob);
    link.download = name + ".png"; 

    var event = document.createEvent("MouseEvents");
    event.initEvent("click", true, false);
    link.dispatchEvent(event); 
}

function clearCanvas() {

    remove_sidebar()
    $(".myCharts").remove()

    document.getElementById('merge').disabled = false

    window.my_config = null

    return false;
}

$(function() {
    $('a#query').bind('click', function() {

        $.getJSON('/_get_query', {
            keywords: $('input[name="keyword"]').val(),
            key_exclude: $('input[name="key_exclude"]').val(),
            age: $('input[name="age"]').val(),
            limit: $('input[name="limit"]').val(),
            uxlimit: $('input[name="uxlimit"]').val(),
            lxlimit: $('input[name="lxlimit"]').val(),
            location: $('#location').val(),
            merge: document.getElementById('merge').checked,
            legend: document.getElementById('legend').checked,
            metabolites: $('#metabolites').val(),
            classification_code: $('#code').val(),

            values: $('input[name="values"]').val(),
            gender: $("#gender option:selected").val(),
            field: $("#field option:selected").val(),
            ID: $('input[name="DatabaseID"]').val(),
            Scan_ID: $('input[name="ScanID"]').val(),

            windowed_SD_threshold: $('input[name="sdThreshold"]').val(),
            overlay: (document.getElementById('overlay').checked == false || (document.getElementById('overlay').checked == true && window.my_config == null))? 0:(window.my_config.results[window.my_config.names[0]].getNumberOfColumns())
        }, function(data) {

            if (document.getElementById('overlay').checked == false || (document.getElementById('overlay').checked == true && window.my_config == null)){
                window.my_config = {metadata_array: data.metadata_array,
                    sd_array: data.sd_array,
                    results: data.result,
                    names: data.names
                }

                render_html(data.names)

                for (result in data.result) {
                    window.my_config.results[result] = new google.visualization.DataTable(data.result[result])
                }
                drawChart(window.my_config.results);
            } else {

                if (window.my_config.sd_array != null){
                    window.my_config['sd_array'] = $.extend({},window.my_config['sd_array'],data.sd_array)
                    window.my_config['metadata_array'] = $.extend({}, window.my_config['metadata_array'], data.metadata_array)
                }
                redraw_charts(data.result)
            }
            remove_sidebar()
        });
return false;
});
});

function remove_sidebar(){
    $('#sidebar').remove()
    $('#sidebar_top').remove()
}

function downloadCsv(csv_out, name) {
    var blob = new Blob([csv_out], {type: 'text/csv;charset=utf-8'});
    var url  = window.URL || window.webkitURL;
    var link = document.createElementNS("http://www.w3.org/1999/xhtml", "a");
    link.href = url.createObjectURL(blob);
    link.download = name + ".csv"; 

    var event = document.createEvent("MouseEvents");
    event.initEvent("click", true, false);
    link.dispatchEvent(event); 
}

function setSize(){
    var width = ($(window).width() / 2 )
    var height = $(window).height()
    $('#left').css({
        'width':width+20,

    })
    $('#right').css({
        'width':width-20,
    })
}

function scrollControl(){

    var scroll = $(this).scrollTop()? $(this).scrollTop():0
    var height = $('#sidebar').height()
    var max_height = $(window).height()
    var width = $(window).width()/2
    var top_height = document.getElementById('top_header').offsetHeight
    var top_height2 = 50


    if (scroll < $('#charts').offset().top) {

        $('#sidebar_top').css({
            'position': 'static',
            'top': '0',
            'max-height': max_height - (top_height-scroll),
        })

        $('#sidebar').css({
            'position': 'static',
            'top': '0',
            'max-height': max_height - (top_height+top_height2-scroll),
        });

    } else {

        $('#sidebar_top').css({'position':'fixed','left': width + 25})


        $('#sidebar').css({
            'position': 'fixed',
            'top': '50px',
            'left': width + 25,

        })
    }
}

function clearSelection(){
    remove_sidebar()
    if (window.my_config != undefined){
        for (c in window.my_config.charts){
            window.my_config.charts[c].setSelection()
        }
    }
}

function render_html(names) {
    if (document.getElementById('overlay').checked != true){
        $(".myCharts").remove()

    } 

    for (name in names) {
        $("#charts").after("<div id = " + names[name] + " style='max-width: 700px; width: 100%; height: 400px; float: right;' class = 'myCharts'>")
    } 

}

function drawChart(allChartData) {

    charts = {}
    window.my_config.columns = {}
    window.my_config.series = {}

    for (c in allChartData){
        var columns = allChartData[c].getNumberOfColumns()
        window.my_config.columns[c] = []
        window.my_config.series[c] = {}

        for (var i = 0; i < columns; i++) {
            window.my_config.columns[c].push(i);
            if (i < (columns-2)/2) {
                window.my_config.series[c][i] = {}
            }
        }

        window.my_config['options'] = {
            title: 'Age vs. ' + c,
            hAxis: {title: 'Age',            
            logScale: document.getElementById('scale').checked? true:false},
            vAxis: {title: "mM per Kg wet wgt."},
            legend: { position: 'top', maxLines : 5},
            aggregationTarget: 'series',
            selectionMode: 'multiple',
            pointSize: 4,
            explorer: {},
            trendlines: document.getElementById('trendline').checked? { 0: {pointSize: 0, type: 'linear'} }: null,
            chartArea: columns > 2? {width: '70%', height: '70%'}:{width: '80%', height: '70%'},
            tooltip: {isHtml: true, trigger: 'none'},
            series: window.my_config.series[c]
        }

        charts[c] = new google.visualization.ScatterChart(document.getElementById(c));
        charts[c].draw(allChartData[c], window.my_config['options']);
    }

    window.my_config['charts'] = charts;

    for (c in charts) {
        google.visualization.events.addListener(charts[c], 'select', function() {
            showHideSeries()
            update_sidebar()
        } )

//google.visualization.events.addListener(charts[c], 'onmouseover', sidebar_mouseover);
//google.visualization.events.addListener(charts[c], 'onmouseout', sidebar_mouseout)
}
};

function sidebar_mouseover(e) {
    for(chart in window.my_config.charts) {
        var columns = window.my_config.columns[chart]

        var sel = window.my_config.charts[chart].getSelection();
        // if selection length is 0, we deselected an element
        if (sel.length > 0) {
            // if row is undefined, we clicked on the legend
            if (sel[0].row != null) {
                var col = sel[0].column;
                if (columns[col] == col) {
                    // hide the data series
                    columns[col] = {
                        label: window.my_config.results[chart].getColumnLabel(col),
                        type: window.my_config.results[chart].getColumnType(col),
                        calc: function () {
                            return null;
                        }
                    };
                    
                    // grey out the legend entry
                    window.my_config.series[chart][(col - 2)/2].color = '#CCCCCC';
                }
                else {
                    // show the data series
                    columns[col] = col;
                    window.my_config.series[chart][(col - 2)/2].color = null;
                }
                var view = new google.visualization.DataView(window.my_config.results[chart]);
                view.setColumns(columns);
                window.my_config.options.title = "Age vs. " + chart
                window.my_config.charts[chart].draw(view, window.my_config.options);
            }
        }
    }
}

function redraw_charts(results){
    var merged_results = null
    var results_new = window.my_config.results
    for (d in results_new){
        var label = ''
        if (d in results){
            var n = new google.visualization.DataTable(results[d])
            var o = results_new[d]

            var old_cols = []

            for (i = 2; i < results_new[d].getNumberOfColumns(); i++) {
                old_cols.push(i)
            }
            merged_results = google.visualization.data.join(o, n, 'full',[[0,0],[1,1]],old_cols,[2,3]);

            results_new[d] = merged_results
            window.my_config.results[d] = merged_results

            //label = results[d].cols[results[d].cols.length -1].label
        }
        //merged_results[d]['cols'] = merged_results[d]['cols'].concat({"id": "", "label": label, "type": "number"})
    }
    drawChart(results_new)
}

function compare_unique_dict(a,b) {
    var a_key = 0
    var b_key = 0
    for (c in a)
        a_key = Math.round(a[c] * 10)/10
    for (d in b)
        b_key = Math.round(b[d] * 10)/10
    if (a_key < b_key)
        return -1;
    if (a_key > b_key)
        return 1;
    return 0;
}

function add_SD(r){
    var complete_list = ""

    var positive = []
    var negative = []
    var zero = []

    var categories = [positive,negative,zero]
    var filtered = $('input[name="sdThreshold"]').val()? parseFloat($('input[name="sdThreshold"]').val()):0

    //sort and order
    var ordered_sd = window.my_config.sd_array[r].sort(compare_unique_dict)

    for (m in ordered_sd){
        if (compare_unique_dict(ordered_sd[m],{'a': -filtered}) == -1){
            negative.push(ordered_sd[m])
        }
        else if (compare_unique_dict(ordered_sd[m],{'a':filtered}) == 1){
            positive.unshift(ordered_sd[m])
        }
        else {            
            zero.push(ordered_sd[m])
        }						
    }
    var ent_list = "<div id='sdDisplay' style='overflow:hidden'>"

    for (c in categories){
        var arrow = ""
        var div = ""
        var elem = ''
        if (categories[c].length > 0){
            if (c == 0){
                div += "<div id='raised' style='float:left;width:30%'>"
                arrow += "<img src='/img/up.png' alt='Raised' style='width:15px;height:15px'>"
            }       
            else if (c == 1) {
                div += "<div id='lowered' style='float:left;width:30%'>"
                arrow += "<img src='/img/down.png' alt='Lowered' style='width:15px;height:15px'>"
            } else {
                div = "<div id='normal' style='float:left;width:30%'>"
            }

            for (i = 0; i < categories[c].length; i++){
                for (name in categories[c][i])
                    elem += name +": " + arrow + Math.round(categories[c][i][name] * 10)/10 + " SD <br>"
            }
            ent_list += div + elem + "</div>"

        }
    }
    return ent_list + "</div>"
}

function showHideSeries () {

    for(chart in window.my_config.charts) {
        var columns = window.my_config.columns[chart]

        var sel = window.my_config.charts[chart].getSelection();
        // if selection length is 0, we deselected an element
        if (sel.length > 0) {
            // if row is undefined, we clicked on the legend
            if (sel[0].row === null) {
                var col = sel[0].column;
                if (columns[col] == col) {
                    // hide the data series
                    columns[col] = {
                        label: window.my_config.results[chart].getColumnLabel(col),
                        type: window.my_config.results[chart].getColumnType(col),
                        calc: function () {
                            return null;
                        }
                    };
                    
                    // grey out the legend entry
                    window.my_config.series[chart][(col - 2)/2].color = '#CCCCCC';
                }
                else {
                    // show the data series
                    columns[col] = col;
                    window.my_config.series[chart][(col - 2)/2].color = null;
                }
                var view = new google.visualization.DataView(window.my_config.results[chart]);
                view.setColumns(columns);
                window.my_config.options.title = "Age vs. " + chart
                window.my_config.charts[chart].draw(view, window.my_config.options);
            }
        }
    }
}

function update_right_sidebar(){

    $("#sidebar_top").append("<img src='../img/left.png' style='width:50px;height:50px;float:none' class='arrow' onclick='document.getElementById(\"sidebar\").scrollLeft = 0'></img><img src='../img/right.png' style='width:50px;height:50px;float:none' class='arrow' onclick='document.getElementById(\"sidebar\").scrollLeft = document.getElementById(\"sidebar\").scrollWidth'></img>")

    patients = window.my_config.current_selection
    sd_array = window.my_config.sd_array

    shared_mets = {}

    for (r in patients){
        for (i in sd_array[r]){
            for (met in sd_array[r][i]){

                shared = true

                for (s in patients){

                    this_one = false

                    for (j in sd_array[s]){
                        for (mm in sd_array[s][j]){
                            if (met == mm) 
                                this_one = true
                        }
                    }

                    if(this_one==false){
                        shared = false
                    }
                }
                if (shared == true && !(met in shared_mets)){
                    shared_mets[met]=0
                }
            }
        }
    }

    //get standard deviation values
    for (r in patients){
        for (i in sd_array[r]){
            for (met in sd_array[r][i]){
                if (met in shared_mets){
                    shared_mets[met] += sd_array[r][i][met] / Object.keys(patients).length
                }
            }
        }
    }

    var sd_array = []
    for (met in shared_mets){
        var qq = {}

        qq[met]=shared_mets[met]
        sd_array.push(qq)
    }

    window.my_config.sd_array['group']=sd_array

    /*
    for (met in shared_mets){
        console.log(met)
        sel = window.my_config.charts[met].getSelection()
        tot = 0
        for (val in sel){
            tot += window.my_config.charts[met].getValue(sel[val].row,sel[val].column)
        }
    }*/

    var i = /*"<div class='title'>Mean of Selected Patients:</div><div class='content'>tCr: 15.3</div><div class='content'>Cr: 9.8</div>*/"<div class='title'>Pooled Standard Deviation:</div><div class='content'>"+add_SD('group')+"</div>"

    $("#sidebar_right").append("<div id='"+r+"' class='group'>"+i+"</div>")

    if (window.my_config.sd_array != null)
        setSize()
}

function getScanID(c,e){
    for (q = 3; q < c.getNumberOfColumns(); q+= 2){ if (c.getValue(e.row, q) != null)
        return c.getValue(e.row, q)
    }
}

function update_sidebar(){

    remove_sidebar()

    $('#right').append("<div id='sidebar_top' style='height:50px;width:100%''></div><div id='sidebar'><div id ='sidebar_left' style='float:left;width:528px;'></div><div id ='sidebar_right' style='float:none;margin-left:560px;width:528px'></div></div>")

    var patients = {}

    for (c in window.my_config.charts){
        selection = window.my_config.charts[c].getSelection()
        data = window.my_config.results[c]


        for (i = 0; i < selection.length; i++){
            patient = getScanID(data,selection[i])
            if (patient in patients || patients == null){} else{
                patients[patient] = null
            }
        }
    }

    window.my_config['current_selection'] = patients

    for (r in patients){

        var i = ""

        for (m in window.my_config.metadata_array[r]) {
            for (n in window.my_config.metadata_array[r][m]) {
                if (window.my_config.metadata_array[r][m][n]) {
                    i+= "<div class='title'>"
                    +n+": </div><div class='content'>" 
                    + window.my_config.metadata_array[r][m][n] + "</div>"
                }


            }
        }
        if (window.my_config.sd_array[r] != null){
            i+= "<div class='title'>SD score for available metabolites:</div><div class='content'>" + add_SD(r) +"</div>"
        }

        $("#sidebar_left").append("<div id='"+r+"' class='patient'>"+i+"</div>")

    }

    if (window.my_config.sd_array != null)
        setSize()

    if (Object.keys(patients).length > 1){
        update_right_sidebar()
    }

}