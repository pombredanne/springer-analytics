var xaxis = "log";

function plotAll() {
    var plots = [];
    for (i in summaries) {
        $(".feature")[i].textContent = summaries[i]['feature'];
        plot = plotDownloadsGraph($(".graph")[i], summaries[i]);
        plots.push(plot);
    };
    enableCrosshair(plots, $(".graph"));
    linkPlots(plots, $(".graph"));
}

function plotDownloadsGraph(graph, downloads) {
    var dataset = 
	[
	    {  label: 'max =', id: 'max', data: downloads['max'], lines: { show: true, lineWidth: 0, fill: 0.3 }, color: "rgba(50,50,255,0.3)", fillBetween: '75%' },
	    {  label: '3/4 =', id: '75%', data: downloads['75%'], lines: { show: true, lineWidth: 0, fill: 0.6 }, color: "rgba(50,50,255,0.6)", fillBetween: '25%' },
	    { label: '2/4 =', id: '50%', data: downloads['50%'], lines: { show: true, lineWidth: 0.5, shadowSize: 0 }, color: "rgb(0,0,0)"},
	    { label: '1/4 =', id: '25%', data: downloads['25%'], lines: { show: true, lineWidth: 0, fill: 0.3 }, color: "rgba(50,50,255,0.6)", fillBetween: 'min' },
	    { label: 'min =', id: 'min', data: downloads['min'], lines: { show: true, lineWidth: 0, fill: 0.0 }, color: "rgba(50,50,255,0.3)" },
	    { label: 'mean =', data: downloads['mean'], lines: { show: true }, color: "rgb(255,100,100)" }
	];
    
    var transform;
    var inverseTransform;
    var ticks;
    if (xaxis == "lin") {
	transform = function (v) {return v};
	inverseTransform = function (v) {return v};
	ticks = null;
    }
    else if (xaxis == "log") {
	transform = function (v) { return Math.log(1 + v); };
	inverseTransform = function (v) { return Math.exp(v) - 1; };
	ticks = 
	    function (axis) {
		var res = [0];
		var tick = 1;
		while (tick <= axis.max) {
		    if (tick >= axis.min) res.push(tick);
		    tick *= 10
		}
		return res
	    };
    }

    var plot = $.plot($(graph), 
	   dataset, 
	   { xaxis: { tickDecimals: 0 },
	     yaxis: { 
		 transform: transform,
		 inverseTransform: inverseTransform,
		 ticks: ticks,
	     },
	     legend: { position: 'ne' },
	     grid: { hoverable: true, autoHighlight: false },
	     crosshair: {mode: 'x'}
	   });
    
    function showTooltip(x, y, contents) {
        $('<div id="tooltip">' + contents + '</div>').css( 
	    {
		position: 'absolute',
		display: 'none',
		top: y + 5,
		left: x + 5,
		border: '1px solid #fdd',
		padding: '2px',
		'background-color': '#fee',
		opacity: 0.80
            }).appendTo("body").fadeIn(200);
    }

    return plot
};

function enableCrosshair(plots, graphs) {
    var plot = plots[0];

    function updateLegend(event, pos, item) {
        var axes = plot.getAxes();
        if (pos.x < axes.xaxis.min || pos.x > axes.xaxis.max ||
            pos.y < axes.yaxis.min || pos.y > axes.yaxis.max)
            return;

	var x = Math.round(pos.x);

	for (var i in plots) {
	    plots[i].lockCrosshair({x:x, y:0});

	    var dataset = plots[i].getData();
	    var series0 = dataset[0];
	    var index = null; // index of x in dataset
	    for (var j in series0.data)
                if (series0.data[j][0] == x) {
                    index = j;
		    break;
		}
	    $.each(plots[i].getData(), 
		   function(j, series) {
		       var y = 0.0;
		       if (index != null) y = series.data[index][1];
		       $(graphs[i]).find(".legendLabel").eq(j).text(series.label.replace(/=.*/, "= " + y.toFixed(2)));
		   }
		  );
	}
    }
    
    graphs.bind("plothover",  updateLegend);
}

function linkPlots(plots, graphs) {
    
    var xmin = Math.min.apply(Math, $.map(plots, function(plot, _) {return plot.getAxes().xaxis.min;}));
    var xmax = Math.max.apply(Math, $.map(plots, function(plot, _) {return plot.getAxes().xaxis.max;}));    
    var ymin = 0;
    var ymax = Math.max.apply(Math, $.map(plots, function(plot, _) {return plot.getAxes().yaxis.max;}));

    $.each(plots, 
	   function(_, plot) {
	       var axes = plot.getAxes();
	       axes.xaxis.options.min = xmin;
	       axes.xaxis.options.max = xmax;
	       axes.yaxis.options.min = ymin;
	       axes.yaxis.options.max = ymax;
	       plot.setupGrid();
	       plot.draw();
	   }
	  );

    var downX = null;

    graphs.bind('mousedown',
		function(event) {
		    event.preventDefault();
		    downX = event.pageX;
		    document.body.style.cursor = 'move';
	});

    graphs.bind('mouseup', 
	       function(event) {
		   event.preventDefault();
		   $.each(plots, 
			  function(_, plot) {
			      var left = downX - event.pageX;
			      plot.pan({left: left, top: 0});
			      plot.setupGrid();
			      plot.draw();
			  });
		   document.body.style.cursor = 'default';
	       });

    graphs.bind('mousewheel',
		function(event, delta) {
		    event.preventDefault();
		    $.each(plots,
			   function(_, plot) {
			       var axes = plot.getAxes();
			       var xmin = axes.xaxis.options.min;
			       var xmax = axes.xaxis.options.max;
			       axes.xaxis.options.min = xmin + 0.05 * delta * (xmax - xmin);
			       axes.xaxis.options.max = xmax - 0.05 * delta * (xmax - xmin);
			       plot.setupGrid();
			       plot.draw();
			   });
		});
}