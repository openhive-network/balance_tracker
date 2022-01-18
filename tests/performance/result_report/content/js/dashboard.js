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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.444212962962963, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [0.9958333333333333, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 5818.415277777788, 84, 22821, 6515.5, 16881.8, 18466.699999999997, 19624.78, 1.3180343714074412, 277.55485574388393, 0.26958493816222073], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 11346.408333333336, 6398, 22821, 7736.0, 19493.8, 19766.35, 22442.579999999987, 0.07433148124059243, 1.512398839499749, 0.01626001152137959], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 10048.324999999999, 6343, 18063, 6728.0, 16920.7, 17086.899999999998, 18061.11, 0.07613044189915001, 60.74934182853902, 0.018883918205453223], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 10052.775000000005, 6239, 19195, 6700.0, 16891.0, 17180.3, 19155.519999999997, 0.07609495889921032, 54.90808620702774, 0.01887511675820256], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 11159.424999999994, 6427, 21711, 7574.0, 19525.600000000002, 19679.2, 21652.829999999998, 0.07445097062350826, 1.8137245245840208, 0.016286149823892433], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 126.15000000000003, 84, 279, 109.0, 194.0, 239.74999999999994, 273.7499999999998, 3.7562212414311205, 1.9074560991642409, 0.6382641562588036], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 10525.924999999996, 5859, 18724, 7342.5, 18405.7, 18492.85, 18714.34, 0.07528008898106518, 1.5577979350671591, 0.01698212944787701], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 125.55833333333331, 87, 283, 108.5, 187.8, 242.95, 278.79999999999984, 3.755280863714599, 3.0511657017681117, 0.6344370990455328], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 10094.541666666662, 5825, 18157, 6765.0, 16827.2, 17033.1, 17983.329999999994, 0.07670716591951757, 55.55613873865293, 0.01902697279644283], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 10613.633333333337, 6377, 19825, 7463.0, 19379.5, 19530.4, 19802.739999999998, 0.07516611711883263, 1.5322436804087032, 0.016442588119744635], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 9780.749999999996, 6334, 17204, 6714.0, 16877.1, 16947.7, 17181.32, 0.07655121467639889, 54.63992461619135, 0.01898828957793488], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 124.47500000000004, 85, 276, 109.0, 209.30000000000004, 218.79999999999995, 271.5899999999998, 3.756691606924835, 1.8600025827254796, 0.645681369940206], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 10092.749999999996, 6144, 18302, 6914.5, 16877.6, 17011.6, 18218.42, 0.07607879734637155, 54.896424462947095, 0.018871107935525756], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 9968.474999999999, 5749, 18555, 7080.5, 18276.7, 18478.4, 18551.64, 0.07523477953388293, 1.5050629479996012, 0.01645760802303689], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 130.45000000000016, 87, 296, 110.5, 213.1000000000001, 266.0499999999998, 295.58, 3.7528146109582186, 2.9025675506629973, 0.6230258631473605], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 135.62500000000003, 88, 289, 110.5, 224.9, 273.0, 288.37, 3.753166734432177, 2.704918994151315, 0.619419119256873], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 128.0583333333334, 87, 289, 108.0, 217.0, 240.34999999999985, 287.31999999999994, 3.754223501439119, 3.1346299743461397, 0.6305922287573521], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 128.06666666666658, 86, 294, 108.0, 204.0, 233.69999999999993, 291.0599999999999, 3.7529319781078967, 2.7817142298670836, 0.6267103205629397], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 150.08333333333337, 95, 1255, 120.5, 239.10000000000005, 294.5999999999997, 1059.4899999999925, 3.7466046395454122, 2.4879796434481247, 0.614677323675419], "isController": false}]}, function(index, item){
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
