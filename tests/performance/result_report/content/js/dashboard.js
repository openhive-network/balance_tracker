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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.444212962962963, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 5545.729629629627, 77, 19108, 6182.5, 15992.8, 17509.95, 18171.17, 1.3631613225694077, 22.75004721366039, 0.39078474546748865], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 10123.733333333334, 6469, 18946, 6978.5, 17793.7, 18124.3, 18812.439999999995, 0.0795002086880478, 1.4233953379752622, 0.023756898299358033], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 9859.758333333337, 5173, 17161, 6622.5, 16220.4, 16509.85, 17161.0, 0.07733025043401603, 3.3372836202930043, 0.02582709535979832], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 10101.824999999999, 5914, 18665, 7144.0, 17736.1, 18034.75, 18645.05, 0.07952987241420217, 1.8100812563333946, 0.023765762655025263], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 10170.775000000001, 5161, 18529, 6560.0, 16162.9, 16774.249999999996, 18481.329999999998, 0.07673105254542478, 3.0110943510599117, 0.025626972627475854], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 199.6583333333334, 87, 470, 190.5, 302.40000000000003, 368.39999999999964, 464.1199999999998, 2.540327702273593, 1.5579353486599772, 0.6400435031119014], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 9545.108333333339, 5826, 18146, 6951.5, 17708.4, 18059.85, 18138.44, 0.07949599538923227, 1.4287541983822565, 0.024221436095156708], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 199.4333333333334, 83, 388, 192.0, 275.1, 307.79999999999995, 386.53, 2.5371051630090067, 2.47020883546873, 0.6342762907522517], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 9624.758333333333, 5156, 17382, 6489.0, 15938.7, 16184.65, 17259.359999999997, 0.07810608368285805, 3.056510727870594, 0.026086211542517047], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 10028.441666666662, 5709, 18493, 7030.0, 17835.2, 18040.3, 18464.44, 0.07957332781624928, 1.4388474002399136, 0.023778748351340116], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 9314.883333333335, 5146, 16601, 6489.5, 16036.2, 16340.85, 16592.18, 0.07802974884174592, 3.0279200195074374, 0.026060716898317484], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 202.3666666666667, 81, 602, 189.5, 298.9000000000001, 349.29999999999984, 563.1499999999985, 2.546473134708429, 1.5617042271454036, 0.6440786541889483], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 9997.099999999993, 5927, 18780, 7089.0, 17878.3, 18169.1, 18743.039999999997, 0.07944794272862635, 1.378313186107468, 0.023741279760702796], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 9469.616666666661, 5170, 19108, 6482.5, 16009.300000000001, 16290.3, 18617.019999999982, 0.07791256651785367, 3.0574595438998355, 0.02602157983311128], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 194.10000000000008, 85, 414, 187.5, 260.30000000000007, 319.54999999999967, 403.7099999999996, 2.5270073914966202, 2.3715372101838397, 0.6243485059068797], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 201.6416666666667, 77, 485, 191.5, 292.0, 318.6499999999999, 469.24999999999943, 2.52956428255233, 2.240541801049769, 0.6225099601593626], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 186.975, 79, 351, 188.0, 243.70000000000002, 306.84999999999997, 350.37, 2.53169898099116, 2.5316989809911603, 0.6304523829616658], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 193.50000000000003, 79, 492, 189.0, 282.8, 310.9, 468.0599999999991, 2.5287114108102413, 2.291644716046781, 0.6272389632283216], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 209.4583333333334, 105, 467, 202.0, 290.40000000000003, 344.95, 450.19999999999936, 2.5297775903868454, 2.0974425529672183, 0.6200919679561505], "isController": false}]}, function(index, item){
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
