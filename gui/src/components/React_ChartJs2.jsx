import React from "react";
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

export default function LineChart({ accountData, currentCurrency }) {
  const chartData = {
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

  const chartOptions = {
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
        },
      },
    },
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
        <Line data={chartData} options={chartOptions} />
      </div>
    </div>
  );
}
