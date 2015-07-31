$( "#allOptWindow" ).dialog({ autoOpen: false, modal: true, minWidth: 500,
	buttons: {

		"Change SD thresholds (advanced)": function() {
			$( "#metWindow" ).dialog( "open" );
		},

		Close: function() {
			$( this ).dialog( "close" );
		}
	} 
});

$( "#allOpt" ).click(function() {
	$( "#allOptWindow" ).dialog( "open" );
});

//KEYWORD OPTION WINDOW
$( "#keyOptWindow" ).dialog({ autoOpen: false, modal: true, minWidth: 500,
	buttons: {

		Close: function() {
			$( this ).dialog( "close" );
		}
	} 
});

$( "#keyOpt" ).click(function() {
	$( "#keyOptWindow" ).dialog( "open" );
});


//AGE OPTION WINDOW
$( "#ageWindow" ).dialog({ autoOpen: false, modal: true,
	buttons: {
		Close: function() {
			$( this ).dialog( "close" );
		}
	} 
});

$( "#ageOpt" ).click(function() {
	$( "#ageWindow" ).dialog( "open" );
});

//MET_THRESH OPTION WINDOW
$( "#metWindow" ).dialog({ autoOpen: false, modal: true, minWidth: 700,
	buttons: {
		Close: function() {
			$( this ).dialog( "close" );
		}
	} 
});