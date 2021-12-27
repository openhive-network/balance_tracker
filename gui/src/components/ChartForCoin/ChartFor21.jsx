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
  accountName,
  currentStartBlock,
  currentEndBlock,
  findCurrency21,
  currentStartDate,
  currentEndDate,
}) {
  const [blocksData21, setBlocksData21] = useState("");
  const [datesData21, setDatesData21] = useState("");
  const [chartXStartAfterZoom21, setChartXStartAfterZoom21] = useState("");
  const [chartXEndAfterZoom21, setChartXEndAfterZoom21] = useState("");
  const [datesChartXStartAfterZoom21, setDatesChartXStartAfterZoom21] =
    useState("");
  const [datesChartXEndAfterZoom21, setDatesChartXEndAfterZoom21] =
    useState("");

  const apiBodyFor21 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency21[0],
    _start_block: !chartXStartAfterZoom21
      ? currentStartBlock
      : chartXStartAfterZoom21,
    _end_block: !chartXEndAfterZoom21 ? currentEndBlock : chartXEndAfterZoom21,
  });

  useEffect(() => {
    if (accountName) {
      if (findCurrency21) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_block", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: apiBodyFor21,
        })
          .then((response) => response.json())
          .then((res) => setBlocksData21(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
    }
  }, [accountName, findCurrency21, apiBodyFor21]);

  const account_Dates_Data_Body21 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency21[0],
    _start_time: !datesChartXStartAfterZoom21
      ? currentStartDate
      : datesChartXStartAfterZoom21,
    _end_time: !datesChartXEndAfterZoom21
      ? currentEndDate
      : datesChartXEndAfterZoom21,
  });

  useEffect(() => {
    if (currentStartDate) {
      if (findCurrency21) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_time", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: account_Dates_Data_Body21,
        })
          .then((response) => response.json())
          .then((res) => setDatesData21(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
    }
  }, [currentStartDate, findCurrency21, account_Dates_Data_Body21]);

  const chartData21 = {
    labels: blocksData21.block,
    datasets: [
      {
        label: "Balance",
        data: blocksData21.balance,
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };
  const chartDatesData21 = {
    labels: datesData21.time,
    datasets: [
      {
        label: "Balance",
        data: datesData21.balance,
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };

  const showingBlocksChart =
    localStorage.getItem("Chart Value") === "Choose Dates";

  let xAxisFirstValue21 = "";
  let xAxisLastValue21 = "";

  let chartOptions21 = {
    plugins: {
      title: {
        display: true,
        text: `Selected currency is : ${findCurrency21}`,
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
              xAxisFirstValue21 = chartXaxis[0].label;
              xAxisLastValue21 = chartXaxis.at(-1).label;
            } catch (error) {
              console.log(error);
            }
          },
        },
      },
    },
  };
  const chartRef21 = useRef("");

  const handleZoomIn21 = (e) => {
    if (chartRef21) {
      chartRef21.current.zoom(1.05); // <==== zoom 5%
      setChartXStartAfterZoom21(xAxisFirstValue21);
      setChartXEndAfterZoom21(xAxisLastValue21);
      setDatesChartXStartAfterZoom21(xAxisFirstValue21);
      setDatesChartXEndAfterZoom21(xAxisLastValue21);
      e.target.disabled = true;
      setTimeout(() => {
        e.target.disabled = false;
      }, 2000);
    }
  };

  const handleResetChartFor21 = () => {
    setChartXStartAfterZoom21(currentStartBlock);
    setChartXEndAfterZoom21(currentEndBlock);
    setDatesChartXStartAfterZoom21(currentStartDate);
    setDatesChartXEndAfterZoom21(currentEndDate);
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
        data={showingBlocksChart === true ? chartData21 : chartDatesData21}
        options={chartOptions21}
        ref={chartRef21}
      />
      <Stack style={{ marginLeft: "50px" }} spacing={2}>
        <Button
          style={{ marinBottom: "25px" }}
          variant="outlined"
          onClick={handleResetChartFor21}
        >
          Reset Chart {findCurrency21}
        </Button>

        <Button variant="outlined" onClick={handleZoomIn21}>
          Zoom in for {findCurrency21}
        </Button>
      </Stack>
    </div>
  );
}
