import React, { useRef } from "react";
import {
  Chart,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Title,
} from "chart.js";
import zoomPlugin from "chartjs-plugin-zoom";
import { Line } from "react-chartjs-2";

Chart.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  zoomPlugin,
  Tooltip,
  Title
);

export default function LineChart({
  setChartXStartAfterZoom,
  setChartXEndAfterZoom,
  setDatesChartXStartAfterZoom,
  setDatesChartXEndAfterZoom,
  accountData,
  currentCurrency,
  datesData,
}) {
  const chartAccountData = {
    labels: accountData.block,
    datasets: [
      {
        label: "Balance",
        data: accountData.balance !== 0 ? accountData.balance : 0, // <==== if no balances, show 0 on chart
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };
  const chartDatesData = {
    labels: datesData.time,
    datasets: [
      {
        label: "Balance",
        data: datesData.balance !== 0 ? datesData.balance : 0, // <==== if no balances, show 0 on chart
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };

  const showingBlocksChart =
    localStorage.getItem("Chart Value") === "Choose Dates";

  let xAxisFirstValue = "";
  let xAxisLastValue = "";

  let chartOptions = {
    plugins: {
      title: {
        display: true,
        text: `Selected currency is : ${currentCurrency}`,
      },
      zoom: {
        pan: {
          enabled: true,
          mode: "xy",
        },
        zoom: {
          wheel: {
            enabled: true,
          },
          pinch: {
            enabled: true,
          },
          mode: "xy",
          onZoom: function (chart) {
            try {
              const chartXaxis =
                chart.chart.$context.chart._metasets[0].iScale._labelItems;
              xAxisFirstValue = chartXaxis[0].label;
              xAxisLastValue = chartXaxis.at(-1).label;
            } catch (error) {
              console.log(error);
            }
          },
        },
      },
    },
  };
  const chartRef = useRef("");

  const handleZoomIn = (e) => {
    chartRef.current.zoom(1.05); // <==== zoom 5%
    setChartXStartAfterZoom(xAxisFirstValue);
    setChartXEndAfterZoom(xAxisLastValue);
    setDatesChartXStartAfterZoom(xAxisFirstValue);
    setDatesChartXEndAfterZoom(xAxisLastValue);
    e.target.disabled = true;
    setTimeout(() => {
      e.target.disabled = false;
    }, 1500);
  };

  return (
    <div
      style={{
        // height: "1200px",

        display: "flex",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <div style={{ width: "1200px" }}>
        <Line
          data={showingBlocksChart === true ? chartAccountData : chartDatesData}
          options={chartOptions}
          ref={chartRef}
        />
      </div>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
        }}
      >
        <p>Zooming 10% and showing 1000 points every zoom</p>
        <button onClick={handleZoomIn}>Zoom in</button>
      </div>
    </div>
  );
}
