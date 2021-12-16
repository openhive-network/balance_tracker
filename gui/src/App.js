import React, { useState, useEffect } from "react";
import styles from "./App.module.css";
import Parameters from "./components/Parameters/Parameters"; // <==== HERE PAREMETERS = INPUT FIELDS
import Dropdown from "./components/Dropdown/Dropdown";
import LineChart from "./components/React_ChartJs2";
import "./App.module.css";

export default function App() {
  const [value, setValue] = useState("");
  const [names, setNames] = useState("");
  const [accountName, setAccountName] = useState("");
  const [currency, setCurrency] = useState("");
  const [startBlock, setStartBlock] = useState("");
  const [endBlock, setEndBlock] = useState("");
  // const [currentCurrency, setcurrentCurrency] = useState("");
  const [currentStartBlock, setcurrentStartBlock] = useState("");
  const [currentEndBlock, setcurrentEndBlock] = useState("");
  /// functions used for getting values from inputs
  const getCurrency = (e) => setCurrency(e.target.value);
  const getStartBlock = (e) => setStartBlock(e.target.value);
  const getEndBlock = (e) => setEndBlock(e.target.value);

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
    _start_block: currentStartBlock,
    _end_block: currentEndBlock,
    _block_increment: currentBlockIncrement,
  });

  const balance_For_Coin_Data21 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency21[0],
    _start_block: currentStartBlock,
    _end_block: currentEndBlock,
    _block_increment: currentBlockIncrement,
  });

  const balance_For_Coin_Data37 = JSON.stringify({
    _account_name: accountName,
    _coin_type: findCurrency37[0],
    _start_block: currentStartBlock,
    _end_block: currentEndBlock,
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

  const handleSubmit = (e) => {
    e.preventDefault();
    names.filter((name) => name === value && setAccountName(name));
    // setcurrentCurrency(currency);
    setcurrentStartBlock(startBlock);
    setcurrentEndBlock(endBlock);
    setFindCurrency13(() => currency.filter((num) => num === 13 && num));
    setFindCurrency21(() => currency.filter((num) => num === 21 && num));
    setFindCurrency37(() => currency.filter((num) => num === 37 && num));

    // setValue("");
    // setCurrency("");
    // setStartBlock("");
    // setEndBlock("");
  };

  return (
    <div>
      <div className={styles.container}>
        <div className={styles.container__input}>
          <Parameters
            startBlock={startBlock}
            endBlock={endBlock}
            currency={currency}
            setCurrency={setCurrency}
            handleSubmit={handleSubmit}
            getCurrency={getCurrency}
            getStartBlock={getStartBlock}
            getEndBlock={getEndBlock}
            value={value}
            setValue={setValue}
          />
          <Dropdown value={value} setValue={setValue} names={names} />
        </div>
        {accountName && `Showing balances for ${accountName}`}
      </div>
      <div style={{ marginTop: "100px" }}>
        <div
          className={findCurrency13[0] === 13 ? styles.showChart : styles.hide}
        >
          <LineChart accountData={data13} currentCurrency={findCurrency13} />
        </div>
        <div
          className={findCurrency21[0] === 21 ? styles.showChart : styles.hide}
        >
          <LineChart accountData={data21} currentCurrency={findCurrency21} />
        </div>
        <div
          className={findCurrency37[0] === 37 ? styles.showChart : styles.hide}
        >
          <LineChart accountData={data37} currentCurrency={findCurrency37} />
        </div>
      </div>
    </div>
  );
}
