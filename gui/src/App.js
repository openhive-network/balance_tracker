import React, { useState, useEffect } from "react";
import styles from "./App.module.css";
import { Button } from "@mui/material";
import Parameters from "./components/Parameters/Parameters"; // <==== HERE PAREMETERS = INPUT FIELDS
import Dropdown from "./components/Dropdown/Dropdown";
import LineChart from "./components/React_ChartJs2";
import moment from "moment";
import "./App.module.css";

export default function App() {
  const today = moment().format("YYYY MM DD hh:mm:ss");
  const [value, setValue] = useState("");
  const [names, setNames] = useState("");
  const [accountName, setAccountName] = useState("");
  const [currency, setCurrency] = useState("");
  const [startBlock, setStartBlock] = useState("");
  const [endBlock, setEndBlock] = useState("");
  const [currentStartBlock, setcurrentStartBlock] = useState("");
  const [currentEndBlock, setcurrentEndBlock] = useState("");
  const [startDate, setStartDate] = useState(today);
  const [endDate, setEndDate] = useState(today);
  const [currentStartDate, setcurrentStartDate] = useState("");
  const [currentEndDate, setcurrentEndDate] = useState("");
  const [dateIncrement, setDateIncrement] = useState("");
  const [currentDateIncrement, setCurrentDateIncrement] = useState("");
  const [chartXStartAfterZoom, setChartXStartAfterZoom] = useState("");
  const [chartXEndAfterZoom, setChartXEndAfterZoom] = useState("");

  /// functions used for getting values from inputs
  const getCurrency = (e) => setCurrency(e.target.value);
  const getStartBlock = (e) => setStartBlock(e.target.value);
  const getEndBlock = (e) => setEndBlock(e.target.value);
  const getStartDate = (newValue) => setStartDate(newValue);
  const getEndDate = (newValue) => setEndDate(newValue);
  const getDateIncrement = (e) => setDateIncrement(e.target.value);

  // calculate block increment number
  const Block_Increment_Number = Math.round(
    (currentEndBlock - currentStartBlock) / 1000
  );
  const currentBlockIncrement =
    Block_Increment_Number >= 1 ? Block_Increment_Number : 1;

  // ////////////////////////////////// Get data of all names
  const account_Names_Data = JSON.stringify({ _partial_account_name: value });

  /// fetch account names
  useEffect(() => {
    fetch("http://localhost:3000/rpc/find_matching_accounts", {
      method: "post",
      headers: { "Content-Type": "application/json" },
      body: account_Names_Data,
    })
      .then((response) => response.json())
      .then((res) => setNames(JSON.parse(res)))
      .catch((err) => console.log(err));
  }, [account_Names_Data]);

  // ////////////////////////////////// Account data of blocks and balances

  // fetch functions data parameters

  /// 3 different parameters for 3 different charts
  const [data13, setData13] = useState("");
  const [data21, setData21] = useState("");
  const [data37, setData37] = useState("");
  const [findCurrency13, setFindCurrency13] = useState("");
  const [findCurrency21, setFindCurrency21] = useState("");
  const [findCurrency37, setFindCurrency37] = useState("");

  const balance_For_Coin_Data13 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency13[0],
    _start_block: !chartXStartAfterZoom
      ? currentStartBlock
      : chartXStartAfterZoom,
    _end_block: !chartXEndAfterZoom ? currentEndBlock : chartXEndAfterZoom,
    _block_increment: currentBlockIncrement,
  });

  const balance_For_Coin_Data21 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency21[0],
    _start_block: !chartXStartAfterZoom
      ? currentStartBlock
      : chartXStartAfterZoom,
    _end_block: !chartXEndAfterZoom ? currentEndBlock : chartXEndAfterZoom,
    _block_increment: currentBlockIncrement,
  });

  const balance_For_Coin_Data37 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency37[0],
    _start_block: !chartXStartAfterZoom
      ? currentStartBlock
      : chartXStartAfterZoom,
    _end_block: !chartXEndAfterZoom ? currentEndBlock : chartXEndAfterZoom,
    _block_increment: currentBlockIncrement,
  });

  ///fetch balance for coin by block
  // &&
  ///show three different charts for different currencies
  useEffect(() => {
    if (accountName) {
      if (findCurrency13) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_block", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: balance_For_Coin_Data13,
        })
          .then((response) => response.json())
          .then((res) => setData13(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
      if (findCurrency21) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_block", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: balance_For_Coin_Data21,
        })
          .then((response) => response.json())
          .then((res) => setData21(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
      if (findCurrency37) {
        fetch("http://localhost:3000/rpc/get_balance_for_coin_by_block", {
          method: "post",
          headers: { "Content-Type": "application/json" },
          body: balance_For_Coin_Data37,
        })
          .then((response) => response.json())
          .then((res) => setData37(JSON.parse(res)))
          .catch((err) => console.log(err));
      }
    }
  }, [
    accountName,
    findCurrency13,
    findCurrency21,
    findCurrency37,
    balance_For_Coin_Data13,
    balance_For_Coin_Data21,
    balance_For_Coin_Data37,
  ]);

  // ////////////////////////////////// Accout data of dates and balances
  const [datesData, setDatesData] = useState("");

  const account_Dates_Data_Body = JSON.stringify({
    _account_name: "dantheman",
    _coin_type: 21,
    _start_time: currentStartDate,
    _end_time: currentEndDate,
    _time_increment: "1 day 02:30:00",
  });

  useEffect(() => {
    fetch("http://localhost:3000/rpc/get_balance_for_coin_by_time", {
      method: "post",
      headers: { "Content-Type": "application/json" },
      body: account_Dates_Data_Body,
    })
      .then((response) => response.json())
      .then((res) => setDatesData(JSON.parse(res)))
      .catch((err) => console.log(err));
  }, [account_Dates_Data_Body]);

  console.log(
    ` ${new Date().getFullYear()} ${new Date().getMonth()} ${new Date().getDate()}`
  );
  // ///// Submit for with "enter" or button

  const handleSubmit = (e) => {
    e.preventDefault();
    names.filter((name) => name === value && setAccountName(name));
    setcurrentStartBlock(startBlock);
    setcurrentEndBlock(endBlock);
    setcurrentStartDate(moment(startDate).format("YYYY MM DD HH:mm:ss"));
    setcurrentEndDate(moment(endDate).format("YYYY MM DD HH:mm:ss"));
    setCurrentDateIncrement(dateIncrement);
    setFindCurrency13(() => currency.filter((num) => num === 13 && num));
    setFindCurrency21(() => currency.filter((num) => num === 21 && num));
    setFindCurrency37(() => currency.filter((num) => num === 37 && num));

    // setValue("");
    // setCurrency("");
    // setStartBlock("");
    // setEndBlock("");
  };

  //// Date string refactoring
  // const cutStartDateString =
  //   currentStartDate.substring(0, 10) + currentStartDate.substring(11);
  // const cutEndDateString =
  //   currentEndDate.substring(0, 10) + currentEndDate.substring(11);

  // const refactoredStartDate = moment
  //   .utc(cutStartDateString)
  //   .format("DD-MM-YYYY HH:mm:ss");
  // const refactoredEndDate = moment
  //   .utc(cutEndDateString)
  //   .format("DD-MM-YYYY HH:mm:ss");

  /////////////////////////////////
  const handleResetChart = () => {
    setChartXStartAfterZoom(currentStartBlock);
    setChartXEndAfterZoom(currentEndBlock);
  };

  return (
    <div>
      <div className={styles.container}>
        <div className={styles.container__input}>
          <Parameters
            startBlock={startBlock}
            endBlock={endBlock}
            startDate={startDate}
            endDate={endDate}
            dateIncrement={dateIncrement}
            currency={currency}
            setCurrency={setCurrency}
            handleSubmit={handleSubmit}
            getCurrency={getCurrency}
            getStartBlock={getStartBlock}
            getEndBlock={getEndBlock}
            getStartDate={getStartDate}
            getEndDate={getEndDate}
            getDateIncrement={getDateIncrement}
            value={value}
            setValue={setValue}
          />
          <Dropdown value={value} setValue={setValue} names={names} />
        </div>
        {accountName && (
          <div style={{ display: "flex", flexDirection: "column" }}>
            Showing balances for {accountName}
            <Button onClick={handleResetChart}>Reset Chart</Button>
          </div>
        )}
      </div>
      <div style={{ marginTop: "100px" }}>
        <div
          className={findCurrency13[0] === 13 ? styles.showChart : styles.hide}
        >
          <LineChart
            setChartXStartAfterZoom={setChartXStartAfterZoom}
            setChartXEndAfterZoom={setChartXEndAfterZoom}
            accountData={data13}
            currentCurrency={findCurrency13}
          />
        </div>
        <div
          className={findCurrency21[0] === 21 ? styles.showChart : styles.hide}
        >
          <LineChart
            setChartXStartAfterZoom={setChartXStartAfterZoom}
            setChartXEndAfterZoom={setChartXEndAfterZoom}
            accountData={data21}
            currentCurrency={findCurrency21}
          />
        </div>
        <div
          className={findCurrency37[0] === 37 ? styles.showChart : styles.hide}
        >
          <LineChart
            setChartXStartAfterZoom={setChartXStartAfterZoom}
            setChartXEndAfterZoom={setChartXEndAfterZoom}
            accountData={data37}
            currentCurrency={findCurrency37}
          />
        </div>
      </div>
      {/* <LineChart datesData={datesData} /> */}
    </div>
  );
}
