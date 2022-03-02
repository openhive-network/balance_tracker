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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.91875, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.8833333333333333, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.8375, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.825, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [0.875, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.875, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.8458333333333333, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.875, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.8458333333333333, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [0.8916666666666667, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.7916666666666666, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [0.9916666666666667, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 250.3287037037035, 13, 1370, 272.0, 658.0, 733.0, 836.5599999999995, 32.56150506512301, 514.5290170495658, 6.712983900144718], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 443.3416666666666, 285, 938, 328.5, 780.6, 800.0, 919.7299999999993, 1.8560336560769635, 33.09866268908343, 0.40781989513409844], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 426.29999999999995, 226, 980, 339.0, 665.7, 691.8499999999999, 949.5499999999988, 1.8370814898730883, 75.55535834570811, 0.45927037246827207], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 425.72500000000014, 230, 1370, 300.5, 663.0, 742.4999999999999, 1270.4599999999962, 1.8318932616859525, 66.37929636216529, 0.4579733154214881], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 450.8916666666665, 254, 1040, 325.5, 792.4000000000001, 802.95, 1032.4399999999996, 1.858736059479554, 42.17188952137546, 0.4084136849442379], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 25.508333333333336, 15, 51, 23.0, 36.0, 39.0, 50.78999999999999, 16.211834639286682, 5.794464333963793, 2.786409078627398], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 413.1083333333333, 224, 972, 306.0, 723.9, 741.9, 958.3499999999995, 1.8677624206200971, 33.43550091053418, 0.4213409366828539], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 26.991666666666678, 15, 58, 25.0, 40.0, 45.94999999999999, 57.369999999999976, 16.203078584931138, 6.629970631920065, 2.7690808128544426], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 402.17499999999984, 198, 876, 295.0, 657.0, 675.9, 839.0399999999986, 1.866222920327833, 67.4191254801636, 0.4665557300819583], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 448.44166666666655, 263, 1005, 328.5, 782.0, 792.9, 979.169999999999, 1.8608401693364554, 33.515112542062745, 0.40887601377021726], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 411.53333333333325, 194, 875, 322.5, 661.0, 685.8499999999999, 862.1899999999995, 1.861244241775627, 66.62127460332232, 0.46531106044390674], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 25.69166666666667, 13, 62, 23.0, 39.0, 46.94999999999999, 60.529999999999944, 16.233766233766232, 5.596210430194805, 2.821885146103896], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 403.2166666666665, 261, 1002, 306.5, 723.0, 739.95, 979.1099999999991, 1.8630068931255046, 32.18781050114885, 0.40935210054027193], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 452.17499999999995, 196, 864, 366.0, 668.9, 727.0499999999995, 855.5999999999997, 1.8439694515727523, 68.62123426863562, 0.46099236289318807], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 26.6, 13, 69, 25.0, 36.0, 40.0, 67.52999999999994, 16.224986479177932, 11.122988777717685, 2.7252906976744184], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 26.79166666666666, 13, 69, 25.0, 36.900000000000006, 42.0, 66.89999999999992, 16.23157040443663, 11.396971797646422, 2.7105454483971325], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 26.649999999999995, 14, 62, 24.0, 39.900000000000006, 49.74999999999994, 61.78999999999999, 16.200891049007694, 6.850572093965168, 2.7528857837181047], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 26.316666666666666, 15, 74, 24.0, 40.900000000000006, 42.94999999999999, 68.95999999999981, 16.203078584931138, 10.490860450985688, 2.7374341749932487], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 44.45833333333336, 16, 1066, 25.5, 40.900000000000006, 47.849999999999966, 1064.53, 16.176867080075493, 10.584473577783768, 2.6856126988406577], "isController": false}]}, function(index, item){
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
