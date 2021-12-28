import React from "react";
import ChartFor13 from "../ChartForCoin/ChartFor13";
import ChartFor21 from "../ChartForCoin/ChartFor21";
import ChartFor37 from "../ChartForCoin/ChartFor37";
import styles from "./charts.module.css";

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
    <div className={styles.charts}>
      <div
        className={
          findCurrency13[0] === 13
            ? styles["charts--show"]
            : styles["charts--hidden"]
        }
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
        className={
          findCurrency21[0] === 21
            ? styles["charts--show"]
            : styles["charts--hidden"]
        }
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
        className={
          findCurrency37[0] === 37
            ? styles["charts--show"]
            : styles["charts--hidden"]
        }
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
