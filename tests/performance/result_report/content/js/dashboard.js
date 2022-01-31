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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.9944444444444445, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.9875, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.996875, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.9927083333333333, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [0.9927083333333333, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [0.99375, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.990625, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.99375, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.99375, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.990625, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [0.99375, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.99375, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [0.9989583333333333, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [0.996875, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [0.9989583333333333, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [0.99375, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 8640, 0, 0.0, 130.33472222222238, 24, 3115, 118.0, 202.0, 252.0, 1057.5900000000001, 110.39699474847629, 23247.665323971734, 22.58011448577233], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 480, 0, 0.0, 76.6541666666667, 27, 3115, 44.0, 75.0, 87.0, 1078.42, 20.83242914804045, 423.8707629877175, 4.557093876133848], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 480, 0, 0.0, 153.86041666666677, 96, 1165, 147.0, 177.0, 188.0, 247.68999999999966, 10.21624382768602, 8152.193432870764, 2.534107355695556], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 480, 0, 0.0, 63.3125, 27, 1096, 44.0, 72.0, 85.0, 1071.38, 20.83242914804045, 507.5056421162276, 4.557093876133848], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 480, 0, 0.0, 152.42708333333346, 84, 1213, 136.0, 170.90000000000003, 180.0, 1144.19, 10.207987750414699, 7365.81081727702, 2.5320594615286462], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 480, 0, 0.0, 58.583333333333336, 24, 1118, 41.0, 73.90000000000003, 82.0, 1050.57, 20.81887578070784, 430.812478313671, 4.6964456106870225], "isController": false}, {"data": ["find_matching_accounts 7", 480, 0, 0.0, 152.96250000000012, 75, 1289, 128.0, 236.0, 259.95, 477.2699999999981, 6.2057196049025185, 3.1513419868645602, 1.054487510989295], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 480, 0, 0.0, 154.60000000000005, 82, 1172, 134.0, 172.90000000000003, 187.79999999999995, 1141.52, 10.231269316849623, 7410.126691889588, 2.5378343813279334], "isController": false}, {"data": ["find_matching_accounts 6", 480, 0, 0.0, 161.80833333333337, 77, 1253, 134.5, 250.80000000000007, 268.0, 1183.57, 6.202992944095525, 5.039931767077615, 1.0479665813755137], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 480, 0, 0.0, 59.56666666666672, 26, 1096, 44.0, 68.90000000000003, 77.94999999999999, 1070.09, 20.81887578070784, 424.3879033657182, 4.55412907702984], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 480, 0, 0.0, 155.9916666666667, 85, 1210, 135.5, 174.0, 183.0, 1154.1399999999999, 10.228435049437437, 7300.74529065803, 2.5371313501534267], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 480, 0, 0.0, 60.44791666666668, 24, 1075, 44.0, 72.90000000000003, 82.94999999999999, 1064.95, 20.833333333333332, 416.76839192708337, 4.557291666666667], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 480, 0, 0.0, 148.18124999999998, 86, 1190, 132.0, 171.0, 178.0, 1136.57, 10.219288907813498, 7373.965430061742, 2.534862678305301], "isController": false}, {"data": ["find_matching_accounts 8", 480, 0, 0.0, 146.87500000000003, 75, 1142, 128.0, 244.90000000000003, 258.95, 298.19, 6.208769887466046, 3.07406868451688, 1.0671323244082267], "isController": false}, {"data": ["find_matching_accounts 3", 480, 0, 0.0, 159.78125, 78, 1309, 141.0, 252.80000000000007, 272.0, 315.79999999999995, 6.186603425831647, 4.7849510871666645, 1.027072834366582], "isController": false}, {"data": ["find_matching_accounts 2", 480, 0, 0.0, 159.1041666666666, 77, 1273, 140.5, 257.90000000000003, 276.0, 303.71, 6.182459846211311, 4.455718131351512, 1.020347376962609], "isController": false}, {"data": ["find_matching_accounts 5", 480, 0, 0.0, 145.4395833333333, 78, 320, 129.0, 230.80000000000007, 258.0, 289.57, 6.197786873603884, 5.174909938409493, 1.0410345139256523], "isController": false}, {"data": ["find_matching_accounts 4", 480, 0, 0.0, 157.55833333333348, 77, 1292, 132.5, 241.90000000000003, 262.95, 498.08999999999793, 6.192749322668043, 4.59013353115727, 1.0341407560314797], "isController": false}, {"data": ["find_matching_accounts 1", 480, 0, 0.0, 178.87083333333328, 82, 1346, 141.5, 289.90000000000003, 310.95, 1220.62, 6.176652254478073, 4.101683137739345, 1.0133570105003087], "isController": false}]}, function(index, item){
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
