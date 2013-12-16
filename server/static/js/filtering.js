function filter_key() {
	var val = $(this).val();
	var key_kwarg = '';
	if(val !== 0) {
		key_kwarg = 'key=' + val;
	}

	// Remove all options from the select list first
	val_select = $('#filter_value');
	val_select.find(':not(.no_value)').remove();


	if(key_kwarg != '') {
		var loader = $('.loader');
		loader.show();

		$.getJSON('/tables/api/metadata?' + key_kwarg, function(data){
			// Insert the new options from the array
			$.each(data, function(value) {
				val_select.append($("<option />").val(data[value]).text(data[value]));
			});

			loader.hide();
		});
	}
}

function serialise_kwargs(kwargs) {
	var kwargs_serialised = '';
	for (var i = 0; i < kwargs.length; i++) {
		kwargs_serialised += '&key_' + (i + 1) + '=' + kwargs[i]['key'] + '&value_' + (i + 1) + '=' + kwargs[i]['value'];
	}

	return kwargs_serialised;
}

var kwargs = [];
function add_filter(filter_key, filter_value) {
	if (!filter_key || typeof filter_key === 'object')
		filter_key = $('#filter_key').val();
	if (!filter_value)
		filter_value = $(this).val();

	// If we have a value then add another filter and redraw everything :)
	if(filter_value != 0) {
		$('.no_filters').hide();
		$("#filters").append("<li>" + filter_key + " = \"" + filter_value + "\"</li>");
		kwargs.push({key: filter_key, value: filter_value});
	}

	$("#filters").trigger('change', [kwargs]);
}

function clear_filters() {
	kwargs = [];
	$('#filters > li').remove();
	$('.no_filters').show();

	$("#filters").trigger('change', [kwargs]);
}

$(document).ready(function() {
	// Get initial filters from kwargs
	var init_keys = [], init_vals = [];
	for(var key in raw_kwargs){
		if(key.indexOf("key_") !== -1) {
			init_keys.push(raw_kwargs[key]);
		}
		else if(key.indexOf("value_") !== -1) {
			init_vals.push(raw_kwargs[key]);
		}
	}

	for(var i = 0; i < init_keys.length; i++) {
		add_filter(init_keys[i], init_vals[i]);
	}

	$('#filters').trigger('load', [kwargs]);

	$.getJSON('/tables/api/metadata?get_keys=' + url_name, function(data){
		key_select = $('#filter_key');
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