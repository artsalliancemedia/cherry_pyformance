var oTable,
	numBars = 6;

function trunc(string, numChars) {
	return string.length > numChars ? string.substring(0, numChars - 3) + '...' : string;
}

function draw(data, selector, index) {
	var height = 200,
		width = $('.content').width(),
		margin = 50;

	var x_domain = [0, d3.max(data, function(d){ return d[index]; })],
		x_scale = d3.scale.linear().range([0, width]).domain(x_domain);

	d3.select(selector)
		.append('svg')
		.attr('width', width).attr('height', height)
		.selectAll('g')
		.data(data)
		.enter()
		.append('g');

	d3.selectAll(selector + ' g')
		.append('rect')
		.attr('width', function(d){ return x_scale(d[index]); })
		.attr('height', height / numBars)
		.attr('y', function(d, i){ return height / numBars * (i) + i; })
		.attr('data-id', function(d){ return d[0]; });

	d3.selectAll(selector + ' g')
		.append('text')
		.attr('x', 10)
		.attr('y', function(d, i) { return height / numBars * (i + 0.5) + i + 4; })
		.text(function(d) {
			return (Math.round(d[index] * 10000) / 10000) + ' - ' + trunc(d[1].replace('\n', ''), 100)
		});

	$(selector + ' g').click(function() {
		window.location.href = '/' + url_name + '/' + $(this).children('rect').attr('data-id');
	});
};

function drawBarGraphs() {
	var total = $.getJSON('/api/' + url_name + '?sort=total&limit=' + numBars + '&' + kwargs),
		avg = $.getJSON('/api/' + url_name + '?sort=avg&limit=' + numBars + '&' + kwargs),
		count = $.getJSON('/api/' + url_name + '?sort=count&limit=' + numBars + '&' + kwargs);

	// Parallelise these calls because we can, speeds up the rendering a tiny bit :)
	$.when(total, avg, count).then(function(total_res, avg_res, count_res) {
		$('svg').remove();

		draw(total_res[0][0], '.graph_total', 3);
		draw(avg_res[0][0], '.graph_avg', 4);
		draw(count_res[0][0], '.graph_count', 2);
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
					window.location.href = '/' + url_name + '/' + aData[0] + '?' + kwargs;
				});
				$('td:eq(0)', nRow).text(trunc(aData[1], 100));
			}
	});
	
	drawBarGraphs();
	
	$('#tabs').tabs({ active: 1 });
});