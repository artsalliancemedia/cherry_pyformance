var oTable,
	numBars = 6;

function trunc(string, numChars) {
	return string.length > numChars ? string.substring(0, numChars - 3) + '...' : string;
}

function draw_bar_graph(data, selector, index, kwargs) {
	var height = 200,
		width = $('.content').width(),
		margin = 50;

	var x_domain = [0, d3.max(data, function(d){ return d[index]; })],
		x_scale = d3.scale.linear().range([0, width]).domain(x_domain);

	// Draw the bounding box
	d3.select(selector)
		.append('svg')
		.attr('width', width).attr('height', height)
		.selectAll('g')
		.data(data)
		.enter()
		.append('g');

	// Draw each of the bars
	d3.selectAll(selector + ' g')
		.append('rect')
		.attr('width', function(d){ return x_scale(d[index]); })
		.attr('height', height / numBars)
		.attr('y', function(d, i){ return height / numBars * (i) + i; });

	// Draw the overlay text
	d3.selectAll(selector + ' g')
		.append('a')
		.attr('xlink:href', function(d) { return '/' + url_name + '/' + d[0]; })
		.append('text')
		.attr('x', 10)
		.attr('y', function(d, i) { return 22 + i * (height / numBars); })
		.text(function(d) {
			return (Math.round(d[index] * 10000) / 10000) + ' - ' + trunc(d[1].replace('\n', ''), 100)
		});
};

function draw_bar_graphs(kwargs) {
	kwargs['limit'] = numBars;

	var total = $.getJSON('/api/' + url_name + '?sort=total', kwargs),
		avg = $.getJSON('/api/' + url_name + '?sort=avg', kwargs),
		count = $.getJSON('/api/' + url_name + '?sort=count', kwargs);

	// Parallelise these calls because we can, speeds up the rendering a tiny bit :)
	$.when(total, avg, count).then(function(total_res, avg_res, count_res) {
		$('#tabs svg').remove();

		draw_bar_graph(total_res[0][0], '.graph_total', 3, kwargs);
		draw_bar_graph(avg_res[0][0], '.graph_avg', 4, kwargs);
		draw_bar_graph(count_res[0][0], '.graph_count', 2, kwargs);
	});
}

$(document).ready(function() {
	oTable = $('#main').dataTable({
			"aaSorting": [[ 3, "desc" ]],
			"bServerSide": true,
			"sAjaxSource": '/api/' + url_name + '?datatables=true',
			"bProcessing": true,
			"bDeferRender": true,
			"sPaginationType": "full_numbers",
			"bAutoWidth": false,
			"sDom": 'rt<"dataTables_bottom"fpli><"clear">',
			"oLanguage": {
				"sInfo": "_START_ to _END_ of _TOTAL_",
				"sInfoEmpty": "0 to 0 of 0"
			},
			"aoColumns": [
				{"bVisible": false},
				null,
				{ "asSorting": [ "desc", "asc" ] },
				{ "asSorting": [ "desc", "asc" ] },
				{ "asSorting": [ "desc", "asc" ] },
				{ "asSorting": [ "desc", "asc" ] },
				{ "asSorting": [ "desc", "asc" ] }
			],

			// This is unneeded, can be replaced with a jquery set operation which is faster than this cursor based operation.
			"fnRowCallback": function( nRow, aData, iDisplayIndex, iDisplayIndexFull ) {
				$(nRow).click(function() {
					window.location.href = '/' + url_name + '/' + aData[0];
				});
				$('td:eq(0)', nRow).text(trunc(aData[1], 100));
			}
	});
	
	$('#tabs').tabs();
	$('#filters').on('load change', function(e, kwargs) {
		oTable.fnSettings().sAjaxSource = '/api/' + url_name + '?datatables=true';
		oTable.fnDraw();

		draw_bar_graphs(kwargs);
	});
});