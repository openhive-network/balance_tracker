import React from "react";
import Paper from "@material-ui/core/Paper";
import {
  Chart,
  ArgumentAxis,
  ValueAxis,
  LineSeries,
  ZoomAndPan,
} from "@devexpress/dx-react-chart-material-ui";

export default function Graph({
  balance,
  // currentStartBlock,
  // currentEndBlock,
  // currentIncrement,
}) {
  let yArr = [];
  for (let i = 0; i <= 10000; i = i + 10) {
    yArr.push(i);
  }

  const generateData = (n) => {
    const result = [];
    for (let i = 0; i < n; i++) {
      result.push({
        x: yArr[i],
        y: balance[i],
      });
    }
    return result;
  };

  const data = generateData(balance.length);
  console.log(data);
  console.log(balance);
  return (
    <div style={{ marginTop: "100px" }}>
      <Paper>
        <Chart data={data}>
          <ArgumentAxis showGrid={true} />
          <ValueAxis />
          <LineSeries valueField="y" argumentField="x" />
          <ZoomAndPan />
        </Chart>
      </Paper>
    </div>
  );
}
