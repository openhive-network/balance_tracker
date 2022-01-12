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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.44375, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 8711.313425925942, 80, 37336, 6586.5, 20754.600000000002, 26460.8, 34276.85, 0.6177827035143542, 10.310287887454864, 0.17710307103786063], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 7982.316666666669, 6090, 18707, 6763.5, 9569.7, 17487.249999999996, 18685.16, 0.09789660975882354, 1.752769964177999, 0.029254260338085945], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 23280.216666666664, 19785, 34359, 20348.5, 33603.7, 33879.75, 34337.79, 0.03518193899979507, 1.518320554959906, 0.01175021790813468], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 7806.483333333334, 5831, 18211, 6724.0, 9561.5, 15973.94999999999, 18117.129999999997, 0.09796174267409435, 2.229586303478948, 0.029273723885032098], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 23258.816666666655, 19947, 37336, 20468.0, 33907.1, 34615.5, 37196.34999999999, 0.03510643246385427, 1.3776532052026562, 0.011724999904920078], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 191.86666666666667, 81, 349, 185.5, 279.9000000000001, 309.79999999999995, 347.31999999999994, 2.54966535642197, 1.5636619568681611, 0.642396154254754], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 7859.241666666667, 5829, 17927, 6733.5, 9551.8, 17317.3, 17850.139999999996, 0.09802496042242223, 1.7617689175920495, 0.02986698012870677], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 195.99166666666665, 80, 440, 188.0, 261.8, 320.84999999999997, 433.0699999999997, 2.544259514470476, 2.477174546803774, 0.636064878617619], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 22845.683333333338, 18837, 36257, 20465.5, 33566.1, 34175.7, 36145.7, 0.035380059975098334, 1.3845212532442779, 0.011816387218245733], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 7925.875, 6042, 17714, 6709.5, 9639.8, 17212.85, 17711.48, 0.09794918885827977, 1.7711202938475665, 0.029269972451790634], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 23243.766666666666, 18946, 34436, 20415.0, 33933.0, 34111.1, 34401.35, 0.035331465083974056, 1.3710264615007746, 0.0118001572839054], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 197.76666666666662, 80, 387, 187.5, 291.40000000000003, 318.95, 383.6399999999999, 2.5552574421873, 1.5670914782164302, 0.6463004663344831], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 7966.141666666668, 5872, 17694, 6742.5, 9608.1, 17371.8, 17687.91, 0.0979671809943669, 1.6995966507469997, 0.02927534900808229], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 23043.875, 19984, 36452, 20640.5, 33653.3, 34274.25, 36128.389999999985, 0.035253938525944696, 1.3834416657485953, 0.01177426462487606], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 192.32500000000005, 88, 412, 187.5, 255.50000000000003, 284.69999999999993, 400.0299999999995, 2.538393197106232, 2.3822225218936413, 0.6271616004569107], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 207.48333333333332, 82, 708, 192.0, 312.9000000000001, 336.79999999999995, 635.9699999999973, 2.543935892815501, 2.2532713425621673, 0.6260467236225647], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 194.4166666666667, 90, 315, 189.5, 262.0, 288.0, 314.78999999999996, 2.5388765471278956, 2.538876547127896, 0.6322397651539193], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 194.24166666666667, 85, 515, 188.0, 257.0, 290.5999999999999, 473.20999999999844, 2.539091428450519, 2.301051607033283, 0.6298136941664374], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 217.1333333333333, 101, 794, 205.5, 282.9, 319.74999999999994, 700.3399999999965, 2.540489044140997, 2.106323436011432, 0.6227175293744046], "isController": false}]}, function(index, item){
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
