/*
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
var showControllersOnly = false;
var seriesFilter = "";
var filtersOnlySampleSeries = true;

/*
 * Add header in statistics table to group metrics by category
 * format
 *
 */
function summaryTableHeader(header) {
    var newRow = header.insertRow(-1);
    newRow.className = "tablesorter-no-sort";
    var cell = document.createElement('th');
    cell.setAttribute("data-sorter", false);
    cell.colSpan = 1;
    cell.innerHTML = "Requests";
    newRow.appendChild(cell);

    cell = document.createElement('th');
    cell.setAttribute("data-sorter", false);
    cell.colSpan = 3;
    cell.innerHTML = "Executions";
    newRow.appendChild(cell);

    cell = document.createElement('th');
    cell.setAttribute("data-sorter", false);
    cell.colSpan = 7;
    cell.innerHTML = "Response Times (ms)";
    newRow.appendChild(cell);

    cell = document.createElement('th');
    cell.setAttribute("data-sorter", false);
    cell.colSpan = 1;
    cell.innerHTML = "Throughput";
    newRow.appendChild(cell);

    cell = document.createElement('th');
    cell.setAttribute("data-sorter", false);
    cell.colSpan = 2;
    cell.innerHTML = "Network (KB/sec)";
    newRow.appendChild(cell);
}

/*
 * Populates the table identified by id parameter with the specified data and
 * format
 *
 */
function createTable(table, info, formatter, defaultSorts, seriesIndex, headerCreator) {
    var tableRef = table[0];

    // Create header and populate it with data.titles array
    var header = tableRef.createTHead();

    // Call callback is available
    if(headerCreator) {
        headerCreator(header);
    }

    var newRow = header.insertRow(-1);
    for (var index = 0; index < info.titles.length; index++) {
        var cell = document.createElement('th');
        cell.innerHTML = info.titles[index];
        newRow.appendChild(cell);
    }

    var tBody;

    // Create overall body if defined
    if(info.overall){
        tBody = document.createElement('tbody');
        tBody.className = "tablesorter-no-sort";
        tableRef.appendChild(tBody);
        var newRow = tBody.insertRow(-1);
        var data = info.overall.data;
        for(var index=0;index < data.length; index++){
            var cell = newRow.insertCell(-1);
            cell.innerHTML = formatter ? formatter(index, data[index]): data[index];
        }
    }

    // Create regular body
    tBody = document.createElement('tbody');
    tableRef.appendChild(tBody);

    var regexp;
    if(seriesFilter) {
        regexp = new RegExp(seriesFilter, 'i');
    }
    // Populate body with data.items array
    for(var index=0; index < info.items.length; index++){
        var item = info.items[index];
        if((!regexp || filtersOnlySampleSeries && !info.supportsControllersDiscrimination || regexp.test(item.data[seriesIndex]))
                &&
                (!showControllersOnly || !info.supportsControllersDiscrimination || item.isController)){
            if(item.data.length > 0) {
                var newRow = tBody.insertRow(-1);
                for(var col=0; col < item.data.length; col++){
                    var cell = newRow.insertCell(-1);
                    cell.innerHTML = formatter ? formatter(col, item.data[col]) : item.data[col];
                }
            }
        }
    }

    // Add support of columns sort
    table.tablesorter({sortList : defaultSorts});
}

$(document).ready(function() {

    // Customize table sorter default options
    $.extend( $.tablesorter.defaults, {
        theme: 'blue',
        cssInfoBlock: "tablesorter-no-sort",
        widthFixed: true,
        widgets: ['zebra']
    });

    var data = {"OkPercent": 100.0, "KoPercent": 0.0};
    var dataset = [
        {
            "label" : "KO",
            "data" : data.KoPercent,
            "color" : "#FF6347"
        },
        {
            "label" : "OK",
            "data" : data.OkPercent,
            "color" : "#9ACD32"
        }];
    $.plot($("#flot-requests-summary"), dataset, {
        series : {
            pie : {
                show : true,
                radius : 1,
                label : {
                    show : true,
                    radius : 3 / 4,
                    formatter : function(label, series) {
                        return '<div style="font-size:8pt;text-align:center;padding:2px;color:white;">'
                            + label
                            + '<br/>'
                            + Math.round10(series.percent, -2)
                            + '%</div>';
                    },
                    background : {
                        opacity : 0.5,
                        color : '#000'
                    }
                }
            }
        },
        legend : {
            show : true
        }
    });

    // Creates APDEX table
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.9997685185185186, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.9958333333333333, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
        switch(index){
            case 0:
                item = item.toFixed(3);
                break;
            case 1:
            case 2:
                item = formatDuration(item);
                break;
        }
        return item;
    }, [[0, 0]], 3);

    // Create statistics table
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 84.68194444444458, 22, 1086, 91.0, 123.0, 132.0, 150.38999999999987, 88.23168988194926, 18580.0419840897, 18.046520771210325], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 44.70833333333332, 25, 1086, 35.0, 39.0, 41.0, 874.949999999992, 19.805248390823568, 402.9710451394619, 4.332398085492655], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 116.35833333333333, 95, 146, 114.0, 138.0, 142.0, 145.79, 7.500468779298706, 5985.103072848303, 1.8604678417401086], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 35.54999999999998, 28, 41, 36.0, 39.900000000000006, 40.0, 41.0, 19.960079840319363, 486.2540543912176, 4.3662674650698605], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 107.86666666666665, 87, 354, 101.5, 127.0, 130.0, 308.8499999999983, 7.48176320219465, 5398.640131086726, 1.8558279817943761], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 32.90833333333335, 22, 39, 33.0, 36.900000000000006, 37.0, 38.78999999999999, 19.986675549633578, 413.5914598600933, 4.508712941372418], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 99.10000000000001, 76, 171, 94.5, 123.9, 141.84999999999997, 168.6899999999999, 5.055824731409311, 2.567410996418791, 0.8590952180324416], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 105.67499999999998, 83, 134, 103.0, 125.0, 130.0, 134.0, 7.493443237167478, 5427.221396122143, 1.858725177969277], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 97.80833333333335, 78, 165, 95.0, 121.50000000000003, 135.95, 163.73999999999995, 5.050717622795573, 4.103708068521402, 0.8532950671324551], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 34.66666666666668, 28, 40, 35.0, 38.0, 39.0, 40.0, 19.973368841544605, 407.15244257656457, 4.369174434087883], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 104.04166666666667, 84, 133, 101.0, 122.9, 127.0, 132.79, 7.487365071441942, 5344.2530573407375, 1.8572175079553253], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 34.44166666666666, 23, 40, 35.0, 38.0, 39.0, 40.0, 19.9700449326011, 399.4984086370444, 4.36844732900649], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 105.43333333333335, 84, 132, 104.0, 124.0, 128.0, 131.57999999999998, 7.494847292486416, 5408.0812605396295, 1.8590734495034664], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 97.06666666666666, 73, 150, 90.0, 124.9, 138.95, 149.57999999999998, 5.059235212277078, 2.504914309203592, 0.8695560521101227], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 99.62499999999997, 75, 166, 95.0, 123.9, 135.89999999999998, 165.79, 5.035669324381032, 3.894775493075955, 0.8359997901804449], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 100.30833333333335, 79, 172, 96.5, 119.9, 131.95, 168.84999999999988, 5.030181086519114, 3.625267228370221, 0.830176370724346], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 98.86666666666666, 77, 164, 94.0, 127.9, 135.89999999999998, 161.0599999999999, 5.0471063257065945, 4.214136629374159, 0.8477561406460297], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 100.125, 74, 161, 96.0, 128.30000000000004, 145.89999999999998, 160.57999999999998, 5.04201680672269, 3.7371980042016806, 0.8419774159663865], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 109.72500000000002, 81, 353, 101.5, 143.9, 146.95, 315.82999999999856, 5.017561465127947, 3.3319744104365276, 0.8231936778725539], "isController": false}]}, function(index, item){
        switch(index){
            // Errors pct
            case 3:
                item = item.toFixed(2) + '%';
                break;
            // Mean
            case 4:
            // Mean
            case 7:
            // Median
            case 8:
            // Percentile 1
            case 9:
            // Percentile 2
            case 10:
            // Percentile 3
            case 11:
            // Throughput
            case 12:
            // Kbytes/s
            case 13:
            // Sent Kbytes/s
                item = item.toFixed(2);
                break;
        }
        return item;
    }, [[0, 0]], 0, summaryTableHeader);

    // Create error table
    createTable($("#errorsTable"), {"supportsControllersDiscrimination": false, "titles": ["Type of error", "Number of errors", "% in errors", "% in all samples"], "items": []}, function(index, item){
        switch(index){
            case 2:
            case 3:
                item = item.toFixed(2) + '%';
                break;
        }
        return item;
    }, [[1, 1]]);

        // Create top5 errors by sampler
    createTable($("#top5ErrorsBySamplerTable"), {"supportsControllersDiscrimination": false, "overall": {"data": ["Total", 2160, 0, null, null, null, null, null, null, null, null, null, null], "isController": false}, "titles": ["Sample", "#Samples", "#Errors", "Error", "#Errors", "Error", "#Errors", "Error", "#Errors", "Error", "#Errors", "Error", "#Errors"], "items": [{"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}]}, function(index, item){
        return item;
    }, [[0, 0]], 0);

});
