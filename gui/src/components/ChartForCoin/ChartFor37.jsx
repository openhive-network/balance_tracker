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
  findCurrency37,
  accountName,
  currentStartBlock,
  currentEndBlock,
  currentStartDate,
  currentEndDate,
}) {
  const [blocksData37, setBlocksData37] = useState("");
  const [datesData37, setDatesData37] = useState("");

  const [chartXStartAfterZoom37, setChartXStartAfterZoom37] = useState("");
  const [chartXEndAfterZoom37, setChartXEndAfterZoom37] = useState("");
  const [datesChartXStartAfterZoom37, setDatesChartXStartAfterZoom37] =
    useState("");
  const [datesChartXEndAfterZoom37, setDatesChartXEndAfterZoom37] =
    useState("");

  const apiBodyFor37 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency37[0],
    _start_block: !chartXStartAfterZoom37
      ? currentStartBlock
      : chartXStartAfterZoom37,
    _end_block: !chartXEndAfterZoom37 ? currentEndBlock : chartXEndAfterZoom37,
  });

  useEffect(() => {
    if (accountName) {
      if (findCurrency37) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_block", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: apiBodyFor37,
        })
          .then((response) => response.json())
          .then((res) => setBlocksData37(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
    }
  }, [accountName, findCurrency37, apiBodyFor37]);

  const account_Dates_Data_Body37 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency37[0],
    _start_time: !datesChartXStartAfterZoom37
      ? currentStartDate
      : datesChartXStartAfterZoom37,
    _end_time: !datesChartXEndAfterZoom37
      ? currentEndDate
      : datesChartXEndAfterZoom37,
  });

  useEffect(() => {
    if (currentStartDate) {
      if (findCurrency37) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_time", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: account_Dates_Data_Body37,
        })
          .then((response) => response.json())
          .then((res) => setDatesData37(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
    }
  }, [currentStartDate, findCurrency37, account_Dates_Data_Body37]);

  const chartData37 = {
    labels: blocksData37.block,
    datasets: [
      {
        label: "Balance",
        data: blocksData37.balance,
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };
  const chartDatesData37 = {
    labels: datesData37.time,
    datasets: [
      {
        label: "Balance",
        data: datesData37.balance,
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };

  const showingBlocksChart =
    localStorage.getItem("Chart Value") === "Choose Dates";

  let xAxisFirstValue37 = "";
  let xAxisLastValue37 = "";

  let chartOptions37 = {
    plugins: {
      title: {
        display: true,
        text: `Selected currency is : ${findCurrency37}`,
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
              xAxisFirstValue37 = chartXaxis[0].label;
              xAxisLastValue37 = chartXaxis.at(-1).label;
            } catch (error) {
              console.log(error);
            }
          },
        },
      },
    },
  };
  const chartRef37 = useRef("");

  const handleZoomIn37 = (e) => {
    if (chartRef37) {
      chartRef37.current.zoom(1.05); // <==== zoom 5%
      setChartXStartAfterZoom37(xAxisFirstValue37);
      setChartXEndAfterZoom37(xAxisLastValue37);
      setDatesChartXStartAfterZoom37(xAxisFirstValue37);
      setDatesChartXEndAfterZoom37(xAxisLastValue37);
      e.target.disabled = true;
      setTimeout(() => {
        e.target.disabled = false;
      }, 2000);
    }
  };

  const handleResetChartFor37 = () => {
    setChartXStartAfterZoom37(currentStartBlock);
    setChartXEndAfterZoom37(currentEndBlock);
    setDatesChartXStartAfterZoom37(currentStartDate);
    setDatesChartXEndAfterZoom37(currentEndDate);
  };

  return (
    <div
      style={{
        width: "70vw",
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <Line
        data={showingBlocksChart === true ? chartData37 : chartDatesData37}
        options={chartOptions37}
        ref={chartRef37}
      />
      <Stack style={{ marginLeft: "50px" }} spacing={2}>
        <Button
          style={{ marinBottom: "25px" }}
          variant="outlined"
          onClick={handleResetChartFor37}
        >
          Reset Chart {findCurrency37}
        </Button>
        <Button variant="outlined" onClick={handleZoomIn37}>
          Zoom in for {findCurrency37}
        </Button>
      </Stack>
    </div>
  );
}
