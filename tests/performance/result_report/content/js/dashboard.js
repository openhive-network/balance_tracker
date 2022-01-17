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
    createTable($("#apdexTable"), {"supportsControllersDiscrimination": true, "overall": {"data": [0.4444444444444444, 500, 1500, "Total"], "isController": false}, "titles": ["Apdex", "T (Toleration threshold)", "F (Frustration threshold)", "Label"], "items": [{"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 1"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 2"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 2"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 1"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 7"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 6"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 5"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 3"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 8"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_block 4"], "isController": false}, {"data": [0.0, 500, 1500, "get_balance_for_coin_by_time 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 3"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 2"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 5"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 4"], "isController": false}, {"data": [1.0, 500, 1500, "find_matching_accounts 1"], "isController": false}]}, function(index, item){
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
    createTable($("#statisticsTable"), {"supportsControllersDiscrimination": true, "overall": {"data": ["Total", 2160, 0, 0.0, 5554.330555555569, 81, 19552, 6140.0, 16344.7, 18132.95, 18694.239999999998, 1.3275470311193003, 22.155673826918598, 0.3805750060999557], "isController": false}, "titles": ["Label", "#Samples", "KO", "Error %", "Average", "Min", "Max", "Median", "90th pct", "95th pct", "99th pct", "Transactions\/s", "Received", "Sent"], "items": [{"data": ["get_balance_for_coin_by_block 1", 120, 0, 0.0, 9940.449999999997, 5563, 19287, 6980.5, 18340.8, 18605.1, 19213.92, 0.07478681084732834, 1.339005263745037, 0.02234840246023679], "isController": false}, {"data": ["get_balance_for_coin_by_time 2", 120, 0, 0.0, 9954.366666666663, 5834, 17480, 7761.5, 16421.1, 16819.8, 17423.51, 0.07896814951302975, 3.40796920242169, 0.026374128060015795], "isController": false}, {"data": ["get_balance_for_coin_by_block 2", 120, 0, 0.0, 9936.125000000002, 5602, 19552, 7056.0, 18488.9, 18728.8, 19519.449999999997, 0.0748516066897376, 1.7036050249131098, 0.02236776528033174], "isController": false}, {"data": ["get_balance_for_coin_by_time 1", 120, 0, 0.0, 9556.233333333339, 5817, 18648, 6509.0, 16305.5, 17101.75, 18630.989999999998, 0.07835501481889218, 3.0748221830882456, 0.02616935065240344], "isController": false}, {"data": ["find_matching_accounts 7", 120, 0, 0.0, 196.0083333333332, 86, 443, 193.0, 252.9, 275.6499999999999, 439.0099999999999, 2.5253588113977865, 1.5487552085525484, 0.6362720442779578], "isController": false}, {"data": ["get_balance_for_coin_by_block 5", 120, 0, 0.0, 10052.724999999999, 5558, 19018, 7012.5, 18325.0, 18644.0, 18978.1, 0.07513359692703589, 1.3503503103956724, 0.022892267813706246], "isController": false}, {"data": ["find_matching_accounts 6", 120, 0, 0.0, 198.37499999999997, 86, 469, 189.5, 278.8, 323.9, 453.6699999999994, 2.5213262175904525, 2.454845936462579, 0.6303315543976131], "isController": false}, {"data": ["get_balance_for_coin_by_time 5", 120, 0, 0.0, 8977.733333333337, 5731, 17027, 6420.5, 16209.0, 16344.5, 16973.659999999996, 0.07973755712033129, 3.1203548719979644, 0.026631098178860644], "isController": false}, {"data": ["get_balance_for_coin_by_block 3", 120, 0, 0.0, 10289.741666666665, 5626, 19530, 6990.0, 18385.2, 18504.25, 19366.829999999994, 0.07489725033469709, 1.3542944210910657, 0.022381404885173154], "isController": false}, {"data": ["get_balance_for_coin_by_time 4", 120, 0, 0.0, 9530.424999999997, 5783, 17983, 6451.5, 16467.3, 16654.95, 17768.589999999993, 0.07956789329415057, 3.087607234312858, 0.02657443311191357], "isController": false}, {"data": ["find_matching_accounts 8", 120, 0, 0.0, 192.43333333333325, 89, 465, 192.5, 256.9, 273.95, 439.79999999999905, 2.5281786579584957, 1.5504845675761085, 0.6394514379016116], "isController": false}, {"data": ["get_balance_for_coin_by_block 4", 120, 0, 0.0, 10084.399999999996, 5603, 18853, 6965.5, 18329.5, 18482.75, 18834.1, 0.0749423412362114, 1.3001471602161088, 0.022394879314727233], "isController": false}, {"data": ["get_balance_for_coin_by_time 3", 120, 0, 0.0, 10069.750000000004, 5808, 17517, 7484.5, 16511.5, 16765.149999999998, 17486.969999999998, 0.07955270167604284, 3.1218220353028374, 0.026569359348834618], "isController": false}, {"data": ["find_matching_accounts 3", 120, 0, 0.0, 199.55000000000004, 84, 433, 195.5, 286.6, 314.6499999999999, 411.5799999999992, 2.509987659227342, 2.3555645903490974, 0.620143435336443], "isController": false}, {"data": ["find_matching_accounts 2", 120, 0, 0.0, 201.33333333333337, 81, 405, 204.0, 265.30000000000007, 304.74999999999994, 394.9199999999996, 2.5100926642541888, 2.223294967264208, 0.6177181165938043], "isController": false}, {"data": ["find_matching_accounts 5", 120, 0, 0.0, 192.63333333333338, 89, 398, 191.5, 259.0, 294.84999999999997, 383.71999999999946, 2.5175705444246304, 2.5175705444246304, 0.6269340711213679], "isController": false}, {"data": ["find_matching_accounts 4", 120, 0, 0.0, 197.375, 85, 367, 194.0, 262.30000000000007, 302.0, 366.37, 2.513457470205056, 2.277820832373332, 0.6234552709297698], "isController": false}, {"data": ["find_matching_accounts 1", 120, 0, 0.0, 208.29166666666669, 102, 375, 202.5, 271.80000000000007, 307.9, 372.4799999999999, 2.5041213664155593, 2.076170937584775, 0.6138031864944388], "isController": false}]}, function(index, item){
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
