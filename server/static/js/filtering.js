var kwargs = '';

function filter_key() {
	var val = $(this).val();
	var key_kwarg = '';
	if(val !== 0) {
		key_kwarg = 'key=' + val;
	}

	// Remove all options from the select list first
	val_select = $('#filter_value');
	val_select.empty();


	if(key_kwarg != '') {
		var loader = $('.loader');
		loader.show();

		$.getJSON('/tables/api/metadata?' + key_kwarg, function(data){
			val_select.append($("<option />").val(0).text('Select metadata value'));
	
			// Insert the new options from the array
			$.each(data, function(value) {
				val_select.append($("<option />").val(data[value]).text(data[value]));
			});

			loader.hide();
		});
	} else {
		val_select.append($("<option/>").val(0).text('Pick a metadata key'));
	}
}

var numFilters = 0;
function add_filter(filter_key, filter_value) {
	if (!filter_key || typeof filter_key === 'object')
		filter_key = $('#filter_key').val();
	if (!filter_value)
		filter_value = $(this).val();

	// If we have a value then add another filter and redraw everything :)
	if(filter_value != 0) {
		numFilters += 1;
		$('.no_filters').hide();

		kwargs += '&key_' + numFilters + '=' + filter_key + '&value_' + numFilters + '=' + filter_value;
		$("#filters").append("<li>" + filter_key + " = \"" + filter_value + "\"</li>");
	}

	oTable.fnSettings().sAjaxSource = '/api/' + url_name + '?datatables=true' + kwargs;
	oTable.fnDraw();

	drawBarGraphs();
}

function clear_filters() {
	numFilters = 0;
	kwargs = '';
	$('#filters > li').remove();
	$('.no_filters').show();

	oTable.fnSettings().sAjaxSource = '/api/' + url_name + '?datatables=true';
	oTable.fnDraw();

	drawBarGraphs();
}

$(document).ready(function() {
	$.getJSON('/tables/api/metadata?get_keys=' + url_name, function(data){
		key_select = $('#filter_key');
		key_select.empty();
		key_select.append($("<option />").val(0).text('Select metadata key'));
		
		// Insert the new ones from the array above
		$.each(data, function(value) {
			key_select.append($("<option />").val(data[value]).text(data[value]));
		});
	});

	// Make the filtering work.
	$('#filter_key').change(filter_key);
	$('#filter_value').change(add_filter);
	$('.clear_filters').click(clear_filters);
});