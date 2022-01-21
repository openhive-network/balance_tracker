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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.9986111111111111, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.9958333333333333, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 84.27546296296313, 22, 1163, 88.0, 119.0, 128.0, 150.38999999999987, 88.37247361099747, 18609.688561185663, 18.075316054332706], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 44.78333333333333, 23, 1092, 33.0, 45.900000000000006, 55.849999999999966, 879.479999999992, 19.730351857941468, 401.4471493752055, 4.3160144689246955], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 118.88333333333338, 87, 1163, 106.0, 130.9, 137.0, 951.1099999999919, 7.265241872010656, 5797.4005002421745, 1.802120542471393], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 35.19166666666666, 24, 57, 34.0, 46.0, 48.0, 56.78999999999999, 19.897197811308242, 484.72216464931194, 4.352512021223678], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 110.44166666666663, 80, 1160, 99.0, 121.9, 135.0, 952.3099999999921, 7.244626901714562, 5227.528921908959, 1.7970070635112292], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 32.08333333333333, 22, 54, 31.0, 38.0, 46.69999999999993, 53.369999999999976, 19.933554817275745, 412.4922134551495, 4.496729651162791], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 96.01666666666667, 74, 136, 95.0, 116.80000000000001, 123.0, 135.36999999999998, 5.071636870800051, 2.5754405984531505, 0.8617820464054773], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 102.775, 79, 140, 99.0, 127.60000000000002, 134.95, 140.0, 7.316627034936894, 5299.1600169197, 1.8148664715566125], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 97.79166666666667, 72, 160, 96.0, 122.80000000000001, 138.84999999999997, 158.31999999999994, 5.063932143309279, 4.11444486643879, 0.8555275984301811], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 33.78333333333333, 22, 59, 33.0, 41.900000000000006, 44.94999999999999, 58.15999999999997, 19.897197811308242, 405.5997139777815, 4.352512021223678], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 101.35000000000001, 80, 144, 98.5, 122.80000000000001, 127.94999999999999, 141.89999999999992, 7.30682579309505, 5215.389621262863, 1.812435304146624], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 33.85833333333334, 24, 58, 33.0, 41.0, 45.0, 56.94999999999996, 19.92362609995019, 398.56980532957, 4.3582932093641045], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 108.49166666666669, 78, 1142, 95.0, 126.80000000000001, 133.95, 934.3099999999921, 7.281995266703077, 5254.492934188968, 1.8062761696704899], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 96.42499999999997, 76, 170, 94.0, 120.9, 133.0, 164.5399999999998, 5.076786394212464, 2.5136042010407413, 0.8725726615052671], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 95.13333333333334, 73, 151, 93.0, 115.9, 118.94999999999999, 146.16999999999982, 5.047318611987382, 3.9037854889589907, 0.8379337539432177], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 106.03333333333327, 75, 1149, 96.0, 123.80000000000001, 129.95, 936.479999999992, 5.039052658100277, 3.631660997732426, 0.8316405265810027], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 95.94166666666665, 74, 156, 93.0, 118.9, 126.89999999999998, 151.37999999999982, 5.060728744939271, 4.2255108173076925, 0.8500442813765182], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 103.16666666666663, 75, 1158, 93.5, 115.80000000000001, 127.0, 949.259999999992, 5.05348269182178, 3.7456966436452457, 0.8438921292007076], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 104.80833333333332, 80, 211, 101.0, 130.8, 148.69999999999993, 203.0199999999997, 5.0242840395243675, 3.3364386199966503, 0.8242966002344666], "isController": false}]}, function(index, item){
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
