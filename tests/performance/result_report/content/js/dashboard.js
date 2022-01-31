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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [1.0, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 8640, 0, 0.0, 86.08252314814807, 5, 410, 77.0, 158.0, 188.0, 260.0, 140.22787028922, 2432.693361086766, 40.245520498587986], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 480, 0, 0.0, 60.28541666666667, 6, 283, 48.5, 122.80000000000007, 154.74999999999994, 225.19, 28.542546232978534, 582.780817030386, 8.55718915383243], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 480, 0, 0.0, 59.76041666666666, 13, 273, 51.0, 106.0, 124.0, 203.69999999999993, 27.47095518800435, 1182.5387741086247, 9.17486979911864], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 480, 0, 0.0, 60.99583333333338, 11, 241, 51.0, 112.90000000000003, 154.79999999999995, 217.95, 27.381631488876213, 1071.5202509982885, 9.14503707929264], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 480, 0, 0.0, 53.02916666666666, 7, 229, 44.0, 102.80000000000007, 128.84999999999997, 206.19, 28.583338295718452, 698.3657625200977, 8.569418805454655], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 480, 0, 0.0, 50.45208333333333, 5, 305, 39.0, 99.0, 126.64999999999992, 200.41999999999996, 28.535758872837526, 592.5350008917424, 8.750222935616193], "isController": false}, {"data": ["find_matching_accounts 7", 480, 0, 0.0, 120.18958333333332, 65, 379, 99.5, 191.0, 229.74999999999994, 287.09, 7.908524730615877, 4.8501499324480175, 1.9925775200184532], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 480, 0, 0.0, 56.18125, 11, 267, 44.0, 111.80000000000007, 133.95, 187.27999999999997, 27.69124264451367, 1086.6649359640014, 9.248442367601246], "isController": false}, {"data": ["find_matching_accounts 6", 480, 0, 0.0, 121.29791666666662, 68, 386, 103.0, 180.0, 228.0, 289.9, 7.894477155356731, 7.686321995986974, 1.973619288839183], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 480, 0, 0.0, 55.34791666666672, 7, 317, 46.0, 107.0, 134.95, 191.51999999999998, 28.520499108734402, 583.416889483066, 8.550579322638146], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 480, 0, 0.0, 56.487499999999955, 10, 243, 48.0, 106.90000000000003, 130.0, 195.14, 27.61477390403866, 1071.582671729375, 9.222903003106662], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 480, 0, 0.0, 54.93958333333336, 9, 310, 45.0, 105.0, 129.95, 186.38, 27.499283872815813, 1076.1243196791752, 9.18433113720997], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 480, 0, 0.0, 54.71458333333332, 7, 266, 43.0, 113.0, 135.0, 186.89999999999998, 28.515416146854392, 572.4803956513991, 8.549055426840136], "isController": false}, {"data": ["find_matching_accounts 8", 480, 0, 0.0, 120.66458333333337, 68, 316, 101.0, 187.0, 236.0, 275.19, 7.914392652805487, 4.853748619103365, 2.0017848604263877], "isController": false}, {"data": ["find_matching_accounts 3", 480, 0, 0.0, 122.07708333333338, 68, 410, 105.0, 183.80000000000007, 216.95, 272.17999999999995, 7.868078549650853, 7.384007310756319, 1.9439686260367832], "isController": false}, {"data": ["find_matching_accounts 2", 480, 0, 0.0, 126.20208333333329, 69, 325, 106.0, 184.90000000000003, 227.5999999999999, 287.38, 7.8635671106305605, 6.9650931341229665, 1.9351747186317394], "isController": false}, {"data": ["find_matching_accounts 5", 480, 0, 0.0, 121.54166666666653, 67, 385, 104.0, 185.7000000000001, 220.89999999999998, 282.76, 7.875953728771843, 7.875953728771844, 1.961297071129707], "isController": false}, {"data": ["find_matching_accounts 4", 480, 0, 0.0, 119.36875000000005, 67, 318, 101.5, 181.80000000000007, 217.0, 260.19, 7.870916961826053, 7.13301849665486, 1.9523563557654466], "isController": false}, {"data": ["find_matching_accounts 1", 480, 0, 0.0, 135.95000000000007, 73, 315, 116.5, 197.0, 242.95, 293.9, 7.852889208821412, 6.510842713173221, 1.9248781166154048], "isController": false}]}, function(index, item){
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
