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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.9199074074074074, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.8625, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.8333333333333334, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.8166666666666667, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [0.8833333333333333, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.8791666666666667, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.8333333333333334, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.8958333333333334, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.8291666666666667, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [0.9041666666666667, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.8208333333333333, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 257.40231481481493, 8, 1033, 268.0, 702.0, 766.0, 874.0, 31.090767769237413, 608.503271007859, 8.973680801450902], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 482.7499999999999, 221, 1033, 346.0, 862.8, 890.0, 1019.5599999999995, 1.7486338797814207, 31.308060109289617, 0.5259562841530054], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 440.475, 214, 781, 333.5, 721.0, 740.8499999999999, 780.79, 1.7921682248573731, 77.34325995400101, 0.6020565130380239], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 443.6083333333334, 202, 822, 354.5, 713.8, 725.8499999999999, 807.0899999999995, 1.7865916298182143, 68.36155589798561, 0.6001831256420563], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 448.9083333333334, 225, 903, 338.5, 843.8, 855.95, 902.79, 1.7527971721538957, 39.893252826385435, 0.5272085244369139], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 28.20833333333333, 8, 126, 24.0, 48.900000000000006, 57.89999999999998, 123.68999999999991, 15.879317189360858, 7.040244144501786, 4.031857880111155], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 415.3416666666666, 199, 820, 309.5, 760.6, 776.9, 816.0099999999999, 1.7627875547198637, 31.681974762758177, 0.5405422775215207], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 29.049999999999994, 10, 108, 25.5, 49.900000000000006, 58.89999999999998, 102.32999999999979, 15.854141894569954, 8.205757035275466, 3.994500594530321], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 429.12499999999994, 200, 791, 330.5, 700.9, 716.8499999999999, 780.2899999999996, 1.8149094813896156, 69.24659515419168, 0.6096961539043241], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 442.14166666666677, 225, 975, 338.5, 832.6, 871.0, 958.8299999999994, 1.7561061273469627, 31.753965873004258, 0.5282037961160786], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 431.0583333333333, 214, 832, 306.5, 701.0, 724.4499999999998, 831.79, 1.8083182640144664, 68.39469371609404, 0.6074819168173599], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 27.866666666666678, 11, 69, 24.0, 51.0, 53.0, 67.94999999999996, 15.904572564612325, 7.051441351888668, 4.053802186878728], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 405.4666666666668, 197, 934, 308.5, 794.2, 808.8, 923.4999999999995, 1.7599178704993768, 30.53216891545061, 0.5293502969861407], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 441.79166666666663, 236, 828, 335.5, 708.9, 732.4499999999998, 816.6599999999996, 1.7979413571460676, 70.55515185113046, 0.6039959246662572], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 32.666666666666664, 10, 98, 28.5, 54.0, 64.94999999999999, 94.21999999999986, 15.919342000530644, 107.14463385513399, 3.9642892677102677], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 34.25833333333333, 11, 91, 30.0, 53.900000000000006, 61.849999999999966, 90.36999999999998, 15.942606616181745, 358.5218214428059, 3.954513750498206], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 27.54166666666666, 9, 68, 25.0, 44.900000000000006, 50.0, 68.0, 15.818613234906406, 8.434534010018455, 3.970101173213815], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 29.0, 11, 110, 24.5, 51.900000000000006, 62.94999999999999, 102.64999999999972, 15.818613234906406, 12.574561692591615, 3.9546533087266016], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 43.983333333333334, 19, 100, 40.0, 63.0, 70.0, 94.7499999999998, 15.898251192368837, 471.1564735691574, 3.927985890302067], "isController": false}]}, function(index, item){
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
