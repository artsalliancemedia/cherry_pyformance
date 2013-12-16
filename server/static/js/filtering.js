function filter_key() {
	var filter_key = $(this).val();

	// Remove all options (except the "No Value Selected" one) from the select list first
	val_select = $('#filter_value');
	val_select.find(':not(.no_value)').remove();

	if(filter_key != '') {
		var loader = $('.loader');
		loader.show();

		$.getJSON('/tables/api/metadata?key=' + filter_key, function(data) {
			// Insert the new options from the array
			$.each(data, function(value) { // Use $.each over the built in forEach so we don't have to deal with problems with null values, it'll just gloss over that for us :)
				val_select.append($("<option />").val(data[value]).text(data[value]));
			});

			loader.hide();
		});
	}
}

var kwargs = {num_filters: 0};
function add_filter(filter_key, filter_value) {
	if (!filter_key || typeof filter_key === 'object')
		filter_key = $('#filter_key').val();
	if (!filter_value)
		filter_value = $(this).val();

	// If we have a value then add another filter and redraw everything :)
	if(filter_value != 0) {
		$('.no_filters').hide();
		$("#filters").append("<li>" + filter_key + " = \"" + filter_value + "\"</li>");

		kwargs['num_filters'] += 1;
		kwargs['key_' + kwargs['num_filters']] = filter_key;
		kwargs['value_' + kwargs['num_filters']] = filter_value;
	}

	$("#filters").trigger('change', [kwargs]);
}

function clear_filters() {
	kwargs = {num_filters: 0};
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

	$.getJSON('/tables/api/metadata', {get_keys: url_name}, function(data){
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