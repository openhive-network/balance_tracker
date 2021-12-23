import React from "react";
import ChartFor13 from "./ChartForCoin/ChartFor13";
import ChartFor21 from "./ChartForCoin/ChartFor21";
import ChartFor37 from "./ChartForCoin/ChartFor37";
import styles from "../App.module.css";

export default function AllChartsSecond({
  accountName,
  currentStartBlock,
  currentEndBlock,
  findCurrency13,
  findCurrency21,
  findCurrency37,
  currentStartDate,
  currentEndDate,
}) {
  return (
    <div
      style={{
        width: "70vw",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <div
        className={findCurrency13[0] === 13 ? styles.showChart : styles.hide}
      >
        <ChartFor13
          accountName={accountName}
          currentStartBlock={currentStartBlock}
          currentEndBlock={currentEndBlock}
          findCurrency13={findCurrency13}
          currentStartDate={currentStartDate}
          currentEndDate={currentEndDate}
        />
      </div>
      <div
        className={findCurrency21[0] === 21 ? styles.showChart : styles.hide}
      >
        <ChartFor21
          accountName={accountName}
          currentStartBlock={currentStartBlock}
          currentEndBlock={currentEndBlock}
          findCurrency21={findCurrency21}
          currentStartDate={currentStartDate}
          currentEndDate={currentEndDate}
        />
      </div>
      <div
        className={findCurrency37[0] === 37 ? styles.showChart : styles.hide}
      >
        <ChartFor37
          accountName={accountName}
          currentStartBlock={currentStartBlock}
          currentEndBlock={currentEndBlock}
          findCurrency37={findCurrency37}
          currentStartDate={currentStartDate}
          currentEndDate={currentEndDate}
        />
      </div>
    </div>
  );
}
