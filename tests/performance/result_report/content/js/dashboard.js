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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [1.0, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [1.0, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 46.42037037037024, 4, 205, 38.5, 93.0, 97.0, 125.77999999999975, 108.17849451595133, 1805.410568062303, 31.012107477337608], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 18.65833333333333, 5, 74, 11.5, 37.70000000000002, 64.39999999999986, 73.57999999999998, 48.84004884004884, 874.4467338217339, 14.59478021978022], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 27.224999999999994, 11, 73, 21.0, 51.900000000000006, 57.94999999999999, 72.78999999999999, 38.61003861003861, 1666.2644787644788, 12.895149613899614], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 21.758333333333336, 8, 79, 15.5, 45.0, 57.94999999999999, 78.78999999999999, 37.890748342279764, 1486.9158509630565, 12.654917903378593], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 16.26666666666667, 6, 65, 11.0, 32.0, 40.0, 65.0, 49.87531172069826, 1135.1504052369078, 14.904145885286782], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 13.716666666666665, 4, 69, 8.0, 32.0, 46.94999999999999, 67.52999999999994, 50.56890012642225, 908.8574589127686, 15.407711757269277], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 78.71666666666664, 66, 152, 72.0, 95.9, 99.94999999999999, 145.06999999999974, 6.224712107065048, 3.8174992219109867, 1.5683356676003737], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 20.449999999999996, 8, 66, 16.0, 39.0, 46.94999999999999, 65.78999999999999, 39.33136676499508, 1539.147000983284, 13.136061946902654], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 79.14166666666667, 68, 127, 72.0, 95.0, 105.59999999999991, 124.89999999999992, 6.218905472636816, 6.054930425995025, 1.554726368159204], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 15.75, 5, 58, 10.0, 36.0, 43.0, 57.78999999999999, 49.87531172069826, 901.846945137157, 14.904145885286782], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 19.658333333333346, 9, 71, 15.5, 34.0, 52.69999999999993, 70.15999999999997, 39.25417075564279, 1523.2458292443573, 13.110279685966635], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 16.63333333333333, 5, 59, 10.0, 39.900000000000006, 52.799999999999955, 58.579999999999984, 49.91680532445923, 865.9883267470882, 14.91654534109817], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 15.741666666666664, 7, 51, 13.0, 34.50000000000003, 39.94999999999999, 50.78999999999999, 38.77221324717286, 1521.506462035541, 12.949313408723746], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 77.60000000000001, 67, 125, 72.0, 94.9, 98.89999999999998, 123.10999999999993, 6.2321474941573625, 3.8220592054011946, 1.5762951181511295], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 81.65, 65, 157, 73.0, 100.70000000000002, 110.0, 153.84999999999988, 6.198347107438017, 5.817003486570248, 1.5314275568181819], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 82.10833333333332, 69, 155, 74.0, 98.80000000000001, 113.79999999999995, 151.00999999999985, 6.1849293887228125, 5.478252886300381, 1.5220724667560044], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 79.89166666666662, 66, 144, 73.0, 96.0, 98.0, 141.0599999999999, 6.2121447429725105, 6.212144742972511, 1.5469696381425686], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 79.99999999999996, 66, 172, 72.5, 97.0, 100.89999999999998, 165.27999999999975, 6.2057196049025185, 5.6239333919429075, 1.5393093551223045], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 90.60000000000001, 71, 205, 80.5, 114.0, 135.95, 191.5599999999995, 6.164594677899927, 5.111075079626015, 1.5110481095242987], "isController": false}]}, function(index, item){
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
