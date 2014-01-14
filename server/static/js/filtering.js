function filter_key() {
	var filter_key = $(this).val();

	// Remove all options (except the "No Value Selected" one) from the select list first
	val_select = $('#filter_value');
	val_select.find(':not(.no_value)').remove();

	if(filter_key != '') {
		var loader = $('.loader');
		loader.show();

		$.getJSON('/tables/api/metadata', {key: filter_key}, function(data) {
			// Insert the new options from the array
			$.each(data, function(value) { // Use $.each over the built in forEach so we don't have to deal with problems with null values, it'll just gloss over that for us :)
				val_select.append($("<option />").val(data[value]).text(data[value]));
			});

			loader.hide();
		});
	}
}

function reset_filters() {
	$('#filter_value').val('0')
	$('.no_filters').show();
}

var kwargs = {num_filters: 0};

function update_header_links() {
	// Modify header links and breadcrumb to send filter kwargs
	$("#header1").attr("href", "/callstacks" + '?' + $.param(kwargs));
	$("#header2").attr("href", "/sqlstatements" + '?' + $.param(kwargs));
	$("#header3").attr("href", "/fileaccesses" + '?' + $.param(kwargs));
	$("#breadcrumb_link").attr("href", "/fileaccesses" + '?' + $.param(kwargs));
}

function clear_filter() {
	kwargs['num_filters'] -= 1;
	delete kwargs['key_' + $(this).attr('id')];
	delete kwargs['value_' + $(this).attr('id')];
	
	for(var i = $(this).attr('id'); i < $("#filters li").length; i++) {
		// update kwargs
		var next_key = 'key_' + (parseInt(i)+1);
		var next_value = 'value_' + (parseInt(i)+1);
		kwargs['key_' + i] = kwargs[next_key];
		kwargs['value_' + i] = kwargs[next_value];
		delete kwargs[next_key];
		delete kwargs[next_value];
		
		// update li id
		$("#filters #" + (parseInt(i)+1)).attr('id', i)
	}
	
	$(this).remove();
	
	if(kwargs['num_filters'] == 0) {
		reset_filters()
	}

	$("#filters").trigger('change', [kwargs]);
	update_header_links();
}

function clear_filters() {
	$('#filters > li').remove();
	reset_filters()

	kwargs = {num_filters: 0};
	$("#filters").trigger('change', [kwargs]);
	update_header_links();
}

function add_filter(filter_key, filter_value) {
	if (!filter_key || typeof filter_key === 'object')
		filter_key = $('#filter_key').val();
	if (!filter_value)
		filter_value = $(this).val();

	// If we have a value then add another filter and redraw everything :)
	if(filter_value != 0) {
		$('.no_filters').hide();
		$("#filters").append($("<li/>").text(filter_key + " = \"" + filter_value + "\"").attr('id', kwargs['num_filters'] + 1).click(clear_filter));

		kwargs['num_filters'] += 1;
		kwargs['key_' + kwargs['num_filters']] = filter_key;
		kwargs['value_' + kwargs['num_filters']] = filter_value;
	}

	$("#filters").trigger('change', [kwargs]);
	update_header_links();
}

$(document).ready(function() {
	// Get initial filters from kwargs
	var init_kwargs = {};
	for(var key in raw_kwargs){
		if(key.indexOf("key_") !== -1) {
			var val = "value_" + key.substring(4);
			init_kwargs[raw_kwargs[key]] = raw_kwargs[val];
		}
	}

	for(var key in init_kwargs) {
		add_filter(key, init_kwargs[key]);
	}

	update_header_links();

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