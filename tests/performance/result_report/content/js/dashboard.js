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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.99375, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.9875, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.990625, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.990625, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [0.9885416666666667, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [0.9895833333333334, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.9979166666666667, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.9916666666666667, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.9947916666666666, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [0.99375, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [0.9979166666666667, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [0.9927083333333333, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 8640, 0, 0.0, 133.04421296296337, 24, 1571, 119.0, 203.0, 250.0, 1066.5900000000001, 110.63873380115761, 23298.5713165241, 22.629558725605698], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 480, 0, 0.0, 74.27499999999998, 25, 1303, 43.0, 76.90000000000003, 87.94999999999999, 1077.0, 21.808268968650612, 443.72586324398, 4.770558836892321], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 480, 0, 0.0, 168.85624999999976, 94, 1228, 151.5, 180.0, 188.95, 1204.71, 9.879592466810744, 7883.557811052794, 2.4506020376659463], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 480, 0, 0.0, 68.29166666666667, 26, 1102, 44.0, 77.0, 90.89999999999998, 1061.1399999999999, 21.84399745153363, 532.1487894784746, 4.778374442522981], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 480, 0, 0.0, 165.58541666666687, 91, 1404, 143.5, 177.0, 186.95, 1198.47, 9.874308284133221, 7125.036642940899, 2.4492913126658578], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 480, 0, 0.0, 67.44374999999992, 24, 1091, 41.0, 76.0, 87.0, 1059.95, 21.78649237472767, 450.83571623093684, 4.914726307189542], "isController": false}, {"data": ["find_matching_accounts 7", 480, 0, 0.0, 158.90833333333333, 75, 1302, 133.5, 241.80000000000007, 262.0, 1178.8799999999999, 6.206682528188683, 3.1518309713458157, 1.0546511327195613], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 480, 0, 0.0, 149.10208333333344, 82, 1216, 141.0, 177.0, 186.0, 391.13999999999794, 9.890789202555121, 7163.529646610344, 2.4533793529775396], "isController": false}, {"data": ["find_matching_accounts 6", 480, 0, 0.0, 158.32291666666666, 76, 1282, 143.0, 245.80000000000007, 262.0, 300.38, 6.205077822017684, 5.041625730389369, 1.0483188117275972], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 480, 0, 0.0, 58.331250000000026, 25, 1069, 42.0, 75.90000000000003, 84.94999999999999, 1036.8, 21.788470267816614, 444.15285973672263, 4.766227871084884], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 480, 0, 0.0, 158.44583333333333, 84, 1217, 144.0, 178.0, 183.0, 1154.38, 9.880405920009881, 7052.332702085177, 2.4508038121899505], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 480, 0, 0.0, 58.91041666666666, 27, 1111, 43.0, 76.0, 85.94999999999999, 1054.71, 21.78649237472767, 435.83622685185185, 4.765795206971678], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 480, 0, 0.0, 148.79166666666669, 82, 1203, 141.0, 173.0, 181.95, 1119.42, 9.878779147543684, 7128.262698347363, 2.4504002963633744], "isController": false}, {"data": ["find_matching_accounts 8", 480, 0, 0.0, 154.65416666666655, 76, 1290, 130.0, 240.0, 260.95, 487.66999999999797, 6.209171463682814, 3.0742675118038933, 1.0672013453204836], "isController": false}, {"data": ["find_matching_accounts 3", 480, 0, 0.0, 149.80625000000006, 76, 318, 141.0, 238.0, 261.9, 292.38, 6.196346737236172, 4.792486929581101, 1.0286903762989736], "isController": false}, {"data": ["find_matching_accounts 2", 480, 0, 0.0, 166.90000000000015, 76, 1304, 145.0, 255.80000000000007, 284.95, 1192.0, 6.194427596174941, 4.464343326149518, 1.0223225231968407], "isController": false}, {"data": ["find_matching_accounts 5", 480, 0, 0.0, 153.6500000000001, 78, 1289, 139.0, 238.90000000000003, 262.9, 308.38, 6.202111302055742, 5.1785206672438076, 1.0417608827671754], "isController": false}, {"data": ["find_matching_accounts 4", 480, 0, 0.0, 158.0979166666667, 77, 1307, 142.0, 241.80000000000007, 260.0, 465.6499999999981, 6.1984271491109135, 4.594341998217953, 1.0350889086894202], "isController": false}, {"data": ["find_matching_accounts 1", 480, 0, 0.0, 176.42291666666677, 85, 1571, 144.5, 277.80000000000007, 306.95, 1159.75, 6.190752563358483, 4.111046624105243, 1.0156703424260012], "isController": false}]}, function(index, item){
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
