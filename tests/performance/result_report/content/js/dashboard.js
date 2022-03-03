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
            "label" : "FAIL",
            "data" : data.KoPercent,
            "color" : "#FF6347"
        },
        {
            "label" : "PASS",
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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.9939814814814815, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.99375, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.990625, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [0.9895833333333334, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [0.9916666666666667, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [0.9989583333333333, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.99375, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.9927083333333333, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.996875, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [0.9979166666666667, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [0.9916666666666667, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [0.99375, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [0.9927083333333333, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [0.99375, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 8640, 0, 0.0, 82.23599537037019, 17, 3260, 51.0, 139.0, 183.0, 1046.0, 182.91133880938267, 37826.11845308663, 37.769127360487765], "isController": false}, "titles": ["Label", "#Samples", "FAIL", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 480, 0, 0.0, 61.52500000000005, 22, 1120, 43.0, 78.0, 89.94999999999999, 1038.23, 20.66115702479339, 402.3477853822314, 4.559981921487603], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 480, 0, 0.0, 151.29166666666654, 91, 1297, 128.5, 209.90000000000003, 251.74999999999994, 1168.32, 10.25224801896666, 8180.92347658002, 2.5630620047416643], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 480, 0, 0.0, 67.39583333333336, 21, 1372, 44.0, 78.0, 87.0, 1061.42, 20.674505750096912, 503.65841840031015, 4.5629280268768575], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 480, 0, 0.0, 158.00000000000003, 85, 3260, 124.0, 210.0, 241.0, 1236.28, 10.243059260365763, 7195.459043767739, 2.560764815091441], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 480, 0, 0.0, 61.14166666666668, 20, 1072, 39.0, 72.90000000000003, 88.89999999999998, 1050.52, 20.62298603651987, 265.31954887218046, 4.692534908700322], "isController": false}, {"data": ["find_matching_accounts 7", 480, 0, 0.0, 42.193749999999994, 17, 1039, 34.0, 65.90000000000003, 76.94999999999999, 108.57, 18.524950793099457, 6.6212226467523445, 3.1839759175639686], "isController": false}, {"data": ["find_matching_accounts 6", 480, 0, 0.0, 49.99374999999999, 17, 1108, 33.0, 66.0, 74.94999999999999, 1052.9, 18.521376755672172, 7.5785711529557025, 3.1652743478931935], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 480, 0, 0.0, 144.5666666666664, 87, 1247, 121.0, 197.90000000000003, 228.95, 1216.52, 10.28784533939173, 7254.427926141844, 2.571961334847933], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 480, 0, 0.0, 80.48958333333336, 31, 3071, 57.5, 96.0, 112.94999999999999, 1055.6599999999999, 20.622100017185083, 420.3766755456264, 4.551361917855302], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 480, 0, 0.0, 141.63333333333335, 84, 1329, 120.0, 199.0, 226.0, 1150.47, 10.276172125883107, 7138.908959537572, 2.5690430314707773], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 480, 0, 0.0, 59.62291666666664, 21, 1284, 43.0, 80.0, 91.0, 1041.1399999999999, 20.62919030428056, 396.8500193398659, 4.552926766374419], "isController": false}, {"data": ["find_matching_accounts 8", 480, 0, 0.0, 45.85416666666665, 17, 1067, 33.5, 67.0, 75.94999999999999, 112.51999999999998, 18.529241459177765, 6.387521713954834, 3.220903300521135], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 480, 0, 0.0, 148.22083333333302, 92, 1214, 133.0, 212.0, 256.9, 330.76, 10.262989095574085, 7405.498316228351, 2.565747273893521], "isController": false}, {"data": ["find_matching_accounts 3", 480, 0, 0.0, 56.31875, 18, 1335, 33.0, 66.0, 78.0, 1060.9, 18.504240555127218, 12.685524286815728, 3.1081341557440245], "isController": false}, {"data": ["find_matching_accounts 2", 480, 0, 0.0, 53.270833333333314, 17, 1294, 34.5, 66.0, 73.0, 1071.19, 18.50923533721513, 12.99623067134539, 3.090897697913855], "isController": false}, {"data": ["find_matching_accounts 5", 480, 0, 0.0, 54.170833333333356, 18, 1261, 33.0, 66.0, 79.94999999999999, 1044.33, 18.517804097064158, 7.83028239651248, 3.1465799930558234], "isController": false}, {"data": ["find_matching_accounts 4", 480, 0, 0.0, 49.210416666666625, 18, 1093, 32.0, 64.0, 72.0, 1037.61, 18.507807981492192, 11.983082706766918, 3.126807403123193], "isController": false}, {"data": ["find_matching_accounts 1", 480, 0, 0.0, 55.34791666666667, 18, 1098, 37.0, 71.0, 84.94999999999999, 1067.1399999999999, 18.49425907374586, 12.10073591739231, 3.0703359790398395], "isController": false}]}, function(index, item){
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
    createTable($("#top5ErrorsBySamplerTable"), {"supportsControllersDiscrimination": false, "overall": {"data": ["Total", 8640, 0, null, null, null, null, null, null, null, null, null, null], "isController": false}, "titles": ["Sample", "#Samples", "#Errors", "Error", "#Errors", "Error", "#Errors", "Error", "#Errors", "Error", "#Errors", "Error", "#Errors"], "items": [{"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}, {"data": [], "isController": false}]}, function(index, item){
        return item;
    }, [[0, 0]], 0);

});
