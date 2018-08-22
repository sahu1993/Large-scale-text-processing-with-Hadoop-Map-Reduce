var fill = d3.scale.category20();

function bodyOnLoad(){
    file = "Output_twitter_immigration.csv";
    dataLoad(file);
}

function js_call(arg) {
    d3.select("svg").remove();
    if (arg=="") return; // please select - possibly you want something else here
    //debugger;
    if (arg == "TW_Immigration"){
        file = "Output_twitter_immigration.csv";
        dataLoad(file);
    }
    if (arg == "TW_Visa"){
        file = "Output_twitter_visa.csv";
        dataLoad(file);
    }
    if (arg == "TW_Daca"){
        file = "Output_twitter_daca.csv";
        dataLoad(file);
    }
    if (arg == "TW_Immigration_Pair"){
        file = "Output_twitter_immigration_pair.csv";
        dataLoad(file);
    }
    if (arg == "TW_Visa_Pair"){
        file = "Output_twitter_visa_pair.csv";
        dataLoad(file);
    }
    if (arg == "TW_Daca_Pair"){
        file = "Output_twitter_daca_pair.csv";
        dataLoad(file);
    }
  }

function dataLoad(filename){
    d3.csv(filename, function(data) {
        data.forEach(function(d) {
          d.size = +d.size;
        });

        d3.layout.cloud().size([600, 600])
            .words(data)
            .padding(5)
            .rotate(function() { return ~~(Math.random() * 2) * 90; })
            .font("Impact")
            .fontSize(function(d) { return Math.min(200, d.size/5); })
            .on("end", draw)
            .start();

        function draw(words) {
          d3.select("body").append("svg")
              .attr("width", 600)
              .attr("height", 600)
            .append("g")
              .attr("transform", "translate(300,300)")
            .selectAll("text")
              .data(data)
            .enter().append("text")
              .style("font-size", function(d) { return d.size + "px"; })
              .style("font-family", "Impact")
              .style("fill", function(d, i) { return fill(i); })
              .attr("text-anchor", "middle")
              .attr("transform", function(d) {
                return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
              })
              .text(function(d) { return d.text; });
        }
      });
}
