function draw(data){
	var height = 400,
		width = $('.item_container').width(),
		margins = {'x': 50, 'y': 80};

	$('#function_graph svg').remove();

	d3.select('#function_graph')
		.append('svg')
		.attr('width', width).attr('height', height)
		.selectAll('circle')
		.data(data)
		.enter()
		.append('circle')

	var x_extent = d3.extent(data, function(d){ return d[1] * 1000; });
	var x_scale = d3.time.scale().range([margins['y'], width - 10]).domain(x_extent);

	var y_extent = d3.extent(data, function(d){ return d[0]; });
	var y_scale = d3.scale.linear().range([height - margins['x'], margins['x']]).domain(y_extent);

	d3.selectAll('circle')
		.attr('cx', function(d){ return x_scale(d[1] * 1000); })
		.attr('cy', function(d){ return y_scale(d[0]); })
		.attr('r', 5)
		.attr('class', 'datum')
		.attr('data-id', function(d){ return d[2]; });

	var line = d3.svg.line()
		 .x(function(d){ return x_scale(d[1] * 1000); })
		 .y(function(d){ return y_scale(d[0]); });

	d3.select('svg').append('path').attr('d', line(data));

	var x_axis = d3.svg.axis().scale(x_scale);
	d3.select('svg')
		.append('g')
		.attr('class','x axis')
		.attr('transform', 'translate(0,' + (height - margins['x']) + ')')
		.call(x_axis);
		
	d3.select('svg')
		.append("text")
			.attr("class", "x label")
			.attr("text-anchor", "middle")
			.attr("x", width / 2)
			.attr("y", height - 5)
		.text("Time");

	var y_axis = d3.svg.axis().scale(y_scale).orient('left');
	d3.select('svg')
		.append('g')
			.attr('class', 'y axis')
			.attr('transform', 'translate(' + margins['y'] + ', 0)')
		.call(y_axis);
		
	d3.select('svg')
		.append("text")
			.attr("class", "y label")
			.attr("x", height / 2)
			.attr("y", 0)
			.attr("transform", "rotate(90)")
		.text("Duration (secs)");

	//mouseover points
	$('.datum').click(function(){
		window.location.href = '/tables/' + url_name + '/' + $(this).attr('data-id');
	});
}

function load_stats(data) {
	var item = data[0];
	if(item.length > 0){
		$('.stat_count').text(item[2]);
		$('.stat_total').text(item[3]);
		$('.stat_avg').text(item[4]);
		$('.stat_min').text(item[5]);
		$('.stat_max').text(item[6]);

		draw(item[7]);
	}
}

function filter_graph(e) {
	var start_date = new Date($('#filter_from').val()) / 1000,
		end_date = new Date($('#filter_to').val()) / 1000;

	var date_limit = '';
	if (start_date)
		date_limit += 'start_date=' + start_date + '&';
	if (end_date)
		date_limit += 'end_date=' + end_date + '&';

	$.getJSON('/api/' + url_name + '/' + item_id + '?' + date_limit, load_stats, noDataError);
}

function load_graph(e, kwargs) {
	$.getJSON('/api/' + url_name + '/' + item_id + '?', kwargs, load_stats);
}

$(document).ready(function(){
	$.datepicker.formatDate('@');
	$('#filter_from').datepicker().change(filter_graph);
	$('#filter_to').datepicker().change(filter_graph);

	$('#filters').on('load change', load_graph);
});
