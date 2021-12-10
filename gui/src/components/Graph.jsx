import React from "react";
import Paper from "@material-ui/core/Paper";
import {
  Chart,
  ArgumentAxis,
  ValueAxis,
  LineSeries,
  ZoomAndPan,
} from "@devexpress/dx-react-chart-material-ui";
import Loader from "./Loader";

export default function Graph({ balance }) {
  let blocks = balance.block;
  let balances = balance.balance;

  const generateData = (n) => {
    const result = [];
    for (let i = 0; i < n; i++) {
      result.push({
        x: blocks[i],
        y: balances[i],
      });
    }
    return result;
  };

  const data = generateData(balances?.length);
  console.log(data.length);
  return (
    <div style={{ marginTop: "100px" }}>
      {data.length === 0 ? (
        <Loader />
      ) : (
        <Paper>
          <Chart data={data}>
            <ArgumentAxis showGrid={true} />
            <ValueAxis />
            <LineSeries valueField="y" argumentField="x" />
            <ZoomAndPan />
          </Chart>
        </Paper>
      )}
    </div>
  );
}
