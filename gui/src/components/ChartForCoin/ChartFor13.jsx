import React, { useRef, useEffect, useState } from "react";
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
import { Button, Stack } from "@mui/material";
import styles from "./chartForCoin.module.css";

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
  accountName,
  currentStartBlock,
  currentEndBlock,
  findCurrency13,
  currentStartDate,
  currentEndDate,
}) {
  const [blocksData13, setBlocksData13] = useState("");
  const [datesData13, setDatesData13] = useState("");
  const [chartXStartAfterZoom13, setChartXStartAfterZoom13] = useState("");
  const [chartXEndAfterZoom13, setChartXEndAfterZoom13] = useState("");
  const [datesChartXStartAfterZoom13, setDatesChartXStartAfterZoom13] =
    useState("");
  const [datesChartXEndAfterZoom13, setDatesChartXEndAfterZoom13] =
    useState("");

  const apiBodyFor13 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency13[0],
    _start_block: !chartXStartAfterZoom13
      ? currentStartBlock
      : chartXStartAfterZoom13,
    _end_block: !chartXEndAfterZoom13 ? currentEndBlock : chartXEndAfterZoom13,
  });

  useEffect(() => {
    if (accountName) {
      if (findCurrency13) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_block", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: apiBodyFor13,
        })
          .then((response) => response.json())
          .then((res) => setBlocksData13(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
    }
  }, [accountName, findCurrency13, apiBodyFor13]);

  const account_Dates_Data_Body13 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency13[0],
    _start_time: !datesChartXStartAfterZoom13
      ? currentStartDate
      : datesChartXStartAfterZoom13,
    _end_time: !datesChartXEndAfterZoom13
      ? currentEndDate
      : datesChartXEndAfterZoom13,
  });

  useEffect(() => {
    if (currentStartDate) {
      if (findCurrency13) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_time", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: account_Dates_Data_Body13,
        })
          .then((response) => response.json())
          .then((res) => setDatesData13(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
    }
  }, [currentStartDate, findCurrency13, account_Dates_Data_Body13]);
  const chartData13 = {
    labels: blocksData13.block,
    datasets: [
      {
        label: "Balance",
        data: blocksData13.balance,
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };
  const chartDatesData13 = {
    labels: datesData13.time,
    datasets: [
      {
        label: "Balance",
        data: datesData13.balance,
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };

  const showingBlocksChart =
    localStorage.getItem("Chart Value") === "Choose Dates";

  let xAxisFirstValue13 = "";
  let xAxisLastValue13 = "";

  let chartOptions13 = {
    plugins: {
      title: {
        display: true,
        text: `Selected currency is : ${findCurrency13}`,
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
              xAxisFirstValue13 = chartXaxis[0].label;
              xAxisLastValue13 = chartXaxis.at(-1).label;
            } catch (error) {
              console.log(error);
            }
          },
        },
      },
    },
  };
  const chartRef13 = useRef("");

  const handleZoomIn13 = (e) => {
    if (chartRef13) {
      chartRef13.current.zoom(1.05); // <==== zoom 5%
      setChartXStartAfterZoom13(xAxisFirstValue13);
      setChartXEndAfterZoom13(xAxisLastValue13);
      setDatesChartXStartAfterZoom13(xAxisFirstValue13);
      setDatesChartXEndAfterZoom13(xAxisLastValue13);
      e.target.disabled = true;
      setTimeout(() => {
        e.target.disabled = false;
      }, 2000);
    }
  };

  const handleResetChartFor13 = () => {
    setChartXStartAfterZoom13(currentStartBlock);
    setChartXEndAfterZoom13(currentEndBlock);
    setDatesChartXStartAfterZoom13(currentStartDate);
    setDatesChartXEndAfterZoom13(currentEndDate);
  };

  return (
    <div className={styles.chartFor13}>
      <Line
        data={showingBlocksChart === true ? chartData13 : chartDatesData13}
        options={chartOptions13}
        ref={chartRef13}
      />
      <Stack className={styles.chartFor13__stack} spacing={2}>
        <Button
          className={styles["stack__button--reset"]}
          variant="outlined"
          onClick={handleResetChartFor13}
        >
          Reset Chart {findCurrency13}
        </Button>

        <Button variant="outlined" onClick={handleZoomIn13}>
          Zoom in for {findCurrency13}
        </Button>
      </Stack>
    </div>
  );
}
