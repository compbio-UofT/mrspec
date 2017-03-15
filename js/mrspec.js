var cursorX;
var cursorY;

document.onmousemove = function(e){
	cursorX = e.pageX;
	cursorY = e.pageY;
}

function toggleMetabolites(){
        $('#all')[0].checked ? toggleMetabolitesOn() : toggleMetabolitesOff()
	// $('.q_mets').prop('selected', $('#metabolites').chosen().val() == undefined );
	// $('#metabolites').trigger('chosen:updated');
}

function toggleMetabolitesOn(){
	$('.q_mets').prop('selected', true);
	$('#metabolites').trigger('chosen:updated');
}

function toggleMetabolitesOff(){
	$('.q_mets').prop('selected', false);
	$('#metabolites').trigger('chosen:updated');
}

function loadThresholds(){

	$.getJSON('/_get_thresholds', {
	}, function(data) {
		for(met in data.thresholds){
			$("input[name='"+met+"']").val( data.thresholds[met] )
		}
	});
}

function loadEchotimes(){

	$.getJSON('/_get_echotimes', {
	}, function(data) {
		for(met in data.echotimes){
			$("input[name='e_"+met+"']").val( data.echotimes[met] )
		}
	});
}

function sendThresholds(){

	var thresholds={}

	$('.met').each(function(){
		thresholds[this.name]=parseInt( this.value )
	})

	$.getJSON('/_alter_thresholds', {
		thresholds:JSON.stringify(thresholds)
	}, function(data) {} )
}

function sendEchotimes(){

	var echotimes={}

	$('.e_met').each(function(){
		echotimes[this.name.slice(2)]= this.value
	})

	$.getJSON('/_alter_echotimes', {
		echotimes:JSON.stringify(echotimes)
	}, function(data) {} )
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

function onEach(obj,func,arg){
	Object.keys(obj).forEach(function(val){
		args = [obj[val],val].concat(arg)
		func.apply(obj,args)})
};

function downloadCSV(table,name){
	csv = google.visualization.dataTableToCsv(table)

	csv_cols = []
    // Iterate columns
    for (var i=0; i<table.getNumberOfColumns(); i++) {
        // Replace any commas in column labels
        csv_cols.push(table.getColumnLabel(i).replace(/,/g,""));
    }
    // Create column row of CSV
    csv = csv_cols.join(",")+"\r\n" + csv;

    //prepare download object
    var blob = new Blob([csv], {type: 'text/csv;charset=utf-8'});
    var url  = window.URL || window.webkitURL;
    var link = document.createElementNS("http://www.w3.org/1999/xhtml", "a");
    link.href = url.createObjectURL(blob);
    link.download = name + ".csv"; 

    var event = document.createEvent("MouseEvents");
    event.initEvent("click", true, false);
    link.dispatchEvent(event); 
}

function downloadPNG(chart,name) {
	png_out = chart.getImageURI()

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
	removeSidebar()

	$('#right').css({'display':'none'})

	$(".myCharts").remove()

	document.getElementById('merge').disabled = false

	window.my_config = null

	return false;
}

$(function() {
	$('a#query').bind('click', function() {

		$.getJSON('/_get_query', {
			diagnosis: $('input[name="diagnosis"]').val(),
			diagnosis_exclude: $('input[name="diagnosis_exclude"]').val(),
			indication: $('input[name="indication"]').val(),
			indication_exclude: $('input[name="indication_exclude"]').val(),
			anesthesia: $('input[name="anesthesia"]').val(),
			treatment: $('input[name="treatment"]').val(),

			age: $('input[name="age"]').val(),
			limit: $('input[name="limit"]').val(),
			uxlimit: $('input[name="uxlimit"]').val(),
			lxlimit: $('input[name="lxlimit"]').val(),
			location: $('#location').val(),
			merge: document.getElementById('merge').checked,
			legend: document.getElementById('legend').checked,
			metabolites: $('#metabolites').getSelectionOrder(),
			classification_code: $('#code').val(),

			values: $('input[name="values"]').val(),
			gender: $("#gender option:selected").val(),
			field: $("#field option:selected").val(),
			ID: $('input[name="DatabaseID"]').val(),
			Scan_ID: $('input[name="ScanID"]').val(),

			ID_exclude: $('input[name="DatabaseID_exclude"]').val(),
			Scan_ID_exclude: $('input[name="ScanID_exclude"]').val(),

			windowed_SD_threshold: $('input[name="sdThreshold"]').val(),
			//overlay: (document.getElementById('overlay').checked == false || (document.getElementById('overlay').checked == true && window.my_config == null))? 0:(window.my_config.results[window.my_config.names[0]].getNumberOfColumns())
		}, function(data) {

			removeSidebar()

			for (result in data.result) {
				data.result[result] = new google.visualization.DataTable(data.result[result])
			}

			//add custom patient data
			data.result = addCustomPatientData(data.names,data.result)

			//if no query has been made yet, and the next query won't be overlaid on top of existing charts, remove them and render the new charts
			overlay = document.getElementById('overlay').checked
			if (overlay == false || (overlay == true && window.my_config == null)){

				$(".myCharts").remove()

				setWindowVals(data)
				
				for (result in data.result){
					renderChartDiv(result)

					configSeries(data.result[result],result)
					window.my_config.charts[result] = drawChart(data.result[result],result,window.my_config.options)
				}

				//otherwise, merge the chart data with what's already on the canvas
			} else {

				//merge metadata (simple concatenation)
				window.my_config['sd_array'] = $.extend({},window.my_config['sd_array'],data.sd_array)
				window.my_config['metadata_array'] = $.extend({}, window.my_config['metadata_array'], data.metadata_array)


				for (result in data.result){
					//join old and new tables of same metabolites
					if (result in window.my_config.results){
						old_table = window.my_config.results[result]
						new_table = data.result[result]
						//determine how many columns there are in both tables to merge
						number_of_old_cols = []
						number_of_new_cols = []
						for (i = 2; i < old_table.getNumberOfColumns(); i++) {
							number_of_old_cols.push(i)
						}
						for (i = 2; i < new_table.getNumberOfColumns(); i++) {
							number_of_new_cols.push(i)
						}
						//join on Age (col 0) and DatabaseID (col 1)
						data.result[result] = google.visualization.data.join(old_table, new_table, 'full',[[0,0],[1,1]],number_of_old_cols,number_of_new_cols);

						window.my_config.results[result] = data.result[result]
					//if new table was not a previously queried metabolite, add its results anyways in a new chart div
				} else {
					renderChartDiv(result)
					window.my_config.names.push(result)
				}
				configSeries(data.result[result],result)
				window.my_config.charts[result] = drawChart(data.result[result],result,window.my_config.options)
			}
		}
	});
return false;
});
});

function setWindowVals(data){
	window.my_config = {metadata_array: data.metadata_array,
		sd_array: data.sd_array,
		results: data.result,
		names: data.names,
		charts: {},
		columns: {},
		series: {},
		options: {
			hAxis: {title: 'Age',            
			logScale: document.getElementById('scale').checked? true:false},
			vAxis: {title: "mM/kg wet wgt."},
			legend: { position: 'top', maxLines : 5},
			aggregationTarget: 'series',
			selectionMode: 'multiple',
			pointSize: 4,
			explorer: {},
			trendlines: document.getElementById('trendline').checked? { 0: {pointSize: 0, type: 'linear'} }: null,
			tooltip: {isHtml: true, trigger: 'none'}
		}
	}
}

//bug exists where data is added in order of metabolites dropdown list, not order inputted by user
function addCustomPatientData(names,results){
	identifiers = $('input[name="p_identifier"]').val()
	values = $('input[name="p_values"]').val()
	ages = $('input[name="p_ages"]').val()

	if ((identifiers && values && ages) != ''){
		identifiers = $('input[name="p_identifier"]').val().split(/[,\s]+/)
		ages = $('input[name="p_ages"]').val().split(/[,\s]+/)
		values = $('input[name="p_values"]').val().split(/[,\s]+/)

		for (i in identifiers){

			for (name in names){
				value_count = countCommas(names[name]) + 1

				row = [parseFloat(ages[i]),identifiers[i]]

				for (var q = 1; q <= results[names[name]].getNumberOfColumns() -2 ; q++) {
					row.push(null)
				}


				for (var j = 1; j <= value_count; j++) {
					row.push(parseFloat(values.shift()))
					row.push(null)

				};
				if (document.getElementById('merge').checked == false){
					results[names[name]].addColumn('number','Patient: '+identifiers[i])
					results[names[name]].addColumn({'id': "Scan_ID",'label':'Scan_ID', "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } })

				} else {
					mets = $('#metabolites').val()
					for (met in mets){
						results[names[name]].addColumn('number',mets[met]+": Patient "+identifiers[i])
						results[names[name]].addColumn({'id': "Scan_ID",'label':'Scan_ID', "role": "tooltip", "type": "string", "p" : { "role" : "tooltip" } })
					}
				}
				results[names[name]].addRow(row)
			}

		}
		if (document.getElementById('overlay').checked){
			$('input[name="p_identifier"]').val('')
			$('input[name="p_values"]').val('')
			$('input[name="p_ages"]').val('')
		}
	}
	return results
} 

function countCommas(string){
	return (string.match(/,/g) || []).length;
}

function removeSidebar(){
	$('.patient').remove()
	$('#group_tab_entry').css({'display':'none'})
	$('#right').css({'display':'none'})
}

function scrollControl(){

	var scroll = $(this).scrollTop()? $(this).scrollTop():0
	var height = $('#sidebar').height()
	var max_height = $(window).height()
	var width = $(window).width()/2
	var top_height = document.getElementById('top_header').offsetHeight
	var top_height2 = 52.094


	if (scroll < $('#charts').offset().top) {

		$('.tabContent').css({
			'max-height': max_height - top_height - top_height2 + scroll,
		});

		$('#right').css({
			'position': 'static',
			'top':'0px'
		})

	} else {

		$('.tabContent').css({'max-height': max_height-top_height2 })

		$('#right').css({
			'position': 'fixed',
			'left': width + 20,
		})
	}
}

function clearSelection(){
	removeSidebar()
	if (window.my_config != undefined){
		for (c in window.my_config.charts){
			window.my_config.charts[c].setSelection()
		}
	}
}

function renderChartDiv(name) {
	$("#charts").after("<div id = " + name + " style='max-width: 700px; width: 100%; height: 400px; float: right;' class = 'myCharts'>")
}

function configSeries(table,name){
	var columns = table.getNumberOfColumns()
	window.my_config.columns[name] = []
	window.my_config.series[name] = {}

	for (var i = 0; i < columns; i++) {
		window.my_config.columns[name].push(i);
		if (i < (columns-2)/2) {
			window.my_config.series[name][i] = {}
		}
	}
}

function drawChart(table, name, options) {

		//set options custom to the chart
		options['title'] = 'Age vs. ' + name
		options['series'] = window.my_config.series[name]

		chart = new google.visualization.ScatterChart(document.getElementById(name));
		chart.draw(table, options);

		google.visualization.events.addListener(chart, 'select', function() {
			selectionMenu()
			updateSidebar()

			/*
			//get selection
			var selection = chart.getSelection();
        	// if selection length is 0, we deselected an element
        	if (selection.length > 0) {
	            // if row is defined, we clicked on a data point
	            if (selection[0].row != null) {
	            	updateSidebar(selection)
	            } else{
					// otherwise, we clicked on the legend
					showHideSeries(window.my_config.charts, chart, selection)

				}
			} else {
				removeSidebar()
			}*/
		} )
		return chart
//google.visualization.events.addListener(charts[c], 'onmouseover', sidebar_mouseover);
//google.visualization.events.addListener(charts[c], 'onmouseout', sidebar_mouseout)
};


function getScanID(c,e){
	for (q = 3; q < c.getNumberOfColumns(); q+= 2){ if (c.getValue(e.row, q) != null)
		return c.getValue(e.row, q)
	}
}

function updateSidebar(current_selection){
	console.log(current_selection)

	removeSidebar()

	var patients = {}


	if (current_selection === undefined){

		for (c in window.my_config.results){
			selection = window.my_config.charts[c].getSelection()
			if (selection.length > 0) {
				if (selection[0].row != null){
					data = window.my_config.results[c]


					for (i = 0; i < selection.length; i++){
						patient = getScanID(data,selection[i])
						if (patient in patients || patients == null){} else{
							patients[patient] = null
						}
					}
				}
			}
		} 
	} else {
		patients=current_selection
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
			i+= "<div class='title'>SD score for available metabolites:</div><div class='content'>" + addSD(r) +"</div>"
		}

		$("#selection").append("<div id='"+r+"' class='patient'>"+i+"</div>")

	}

	patient_count = Object.keys(patients).length
	if (patient_count == 0 ){
		removeSidebar()
	} else if (patient_count == 1){
		$('#right').css({'display':'inline'})
		document.getElementById('select').click()
	} else if (patient_count > 1){
		$('#right').css({'display':'inline'})
		updateRightSidebar()
	}
}


function updateRightSidebar(){

	$('#group_tab_entry').css({'display':'inline'})

	patients = window.my_config.current_selection
	sd_array = window.my_config.sd_array
	n = Object.keys(patients).length

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
				if (shared == true){
					if (met in shared_mets){						
						shared_mets[met] += sd_array[r][i][met] / n
					} else {						
						shared_mets[met] = 0
					}
				}
			}
		}
	}

    /*//get standard deviation values
    for (r in patients){
    	for (i in sd_array[r]){
    		for (met in sd_array[r][i]){
    			if (met in shared_mets){
    				shared_mets[met] += sd_array[r][i][met] / n
    			}
    		}
    	}
    } */

    var sd_array = []
    for (met in shared_mets){
    	var qq = {}

    	qq[met]=shared_mets[met]
    	sd_array.push(qq)
    }

    window.my_config.sd_array['group']=sd_array

    var i = "<div class='title'>Pooled Standard Deviation:</div><div class='content'>"+addSD('group')+"</div>"

    $("#group_select").append("<div id='"+r+"' class='patient'>"+i+"</div>")

}

function compareUniqueDict(a,b) {
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

function addSD(r){
	var complete_list = ""

	var positive = []
	var negative = []
	var zero = []

	var categories = [positive,negative,zero]
	var filtered = $('input[name="sdThreshold"]').val()? parseFloat($('input[name="sdThreshold"]').val()):0

    //sort and order
    var ordered_sd = window.my_config.sd_array[r].sort(compareUniqueDict)

    for (m in ordered_sd){
    	if (compareUniqueDict(ordered_sd[m],{'a': -filtered}) == -1){
    		negative.push(ordered_sd[m])
    	}
    	else if (compareUniqueDict(ordered_sd[m],{'a':filtered}) == 1){
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

function selectionMenu(){
	for(chart in window.my_config.charts) {
		var columns = window.my_config.columns[chart]

		var sel = window.my_config.charts[chart].getSelection();
        // if selection length is 0, we deselected an element
        if (sel.length > 0) {
            // if row is undefined, we clicked on the legend
            if (sel[0].row === null) {
            	$('.context-menu-one').contextMenu({x:cursorX,y:cursorY})
            }
        }
    }
}

function deleteSeries () {
	for(name in window.my_config.charts) {
		var columns = window.my_config.results[name].getNumberOfColumns()
		cols_to_include = []

		var sel = window.my_config.charts[name].getSelection();
        // if selection length is 0, we deselected an element
        if (sel.length > 0) {
            // if row is undefined, we clicked on the legend
            if (sel[0].row === null) {
            	if (columns <=4){
            		alert("You cannot remove the only series in a chart. Try 'clear canvas' to delete the chart altogether.")
            	} else {
            		var col = sel[0].column;
            		/*for (i=0; i<columns; i++){
            			if (i != col && (i != col + 1)){
            				cols_to_include.push(i)
            			}
            		}

            		var view = new google.visualization.DataView(window.my_config.results[name]);
            		view.setColumns(cols_to_include);*/
            		options = window.my_config.options
            		options['title'] = "Age vs. " + chart
            		options['series'] = window.my_config.series[name]

            		window.my_config.results[name].removeColumns(col, 2)

            		window.my_config.charts[name].draw(window.my_config.results[name], options);
            	}
            }
        }
    }
}

function selectSeries () {
	for(name in window.my_config.charts) {
		var columns = window.my_config.results[name].getNumberOfColumns()
		cols_to_include = []

		var sel = window.my_config.charts[name].getSelection();
        // if selection length is 0, we deselected an element
        if (sel.length > 0) {
            // if row is undefined, we clicked on the legend
            if (sel[0].row === null) {

            	var col = sel[0].column + 1
            	console.log(col)

            	patients_array = window.my_config.results[name].getDistinctValues(col)
            	patients = {}
            	patients_array.forEach(	function(val, i) {
            		if (val === null){return;} else {patients[val] = null;}
            	})

            	updateSidebar(patients)
            	
            }
        }
    }
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
            	//console.log(col)
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
                options = window.my_config.options
                options['title'] = "Age vs. " + chart
                options['series'] = window.my_config.series[chart]

                window.my_config.charts[chart].draw(view, options);
            }
        }
    }
}

$(function(){
    // make button open the menu
    $('#activate-menu').on('click', function(e) {
    	e.preventDefault();
    	$('.context-menu-one').contextMenu();
        // or $('.context-menu-one').trigger("contextmenu");
        // or $('.context-menu-one').contextMenu({x: 100, y: 100});
    });
    
    $.contextMenu({
    	selector: '.context-menu-one', 
    	trigger: 'none',
    	callback: function(key, options) {
    		if (key =='edit'){
    			showHideSeries()
    		} else if (key == 'display'){

    		} else if (key == "delete"){
    			deleteSeries()
    			clearSelection()
    		} else if (key == "select"){
    			selectSeries()
    		}
    		//clearSelection()

            //var m = "clicked: " + key;
            //window.console && console.log(m) || alert(m); 
        },
        items: {
        	"edit": {name: "Show/hide series"},
        	"select": {name: "Select all points in series"},
        	/*"display": {name: "Display complete query information"},*/
        	"sep1": "---------",
        	"delete": {name: "Delete series", icon: "delete"},
        }
    });
});
