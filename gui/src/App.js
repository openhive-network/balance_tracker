import React, { useState, useEffect } from "react";
import { Button } from "@mui/material";
import styles from "./App.module.css";
import Parameters from "./components/Parameters/Parameters"; // <==== HERE PAREMETERS = INPUT FIELDS
import Dropdown from "./components/Dropdown/Dropdown";
import Graph from "./components/Graph";

export default function App() {
  const [value, setValue] = useState("");
  const [names, setNames] = useState("");
  const [accountName, setAccountName] = useState("");
  const [currentCurrency, setcurrentCurrency] = useState("");
  const [currentStartBlock, setcurrentStartBlock] = useState("");
  const [currentEndBlock, setcurrentEndBlock] = useState("");
  const [currentIncrement, setCurrentIncrement] = useState("");

  const [currency, setCurrency] = useState("");
  const [startBlock, setStartBlock] = useState("");
  const [endBlock, setEndBlock] = useState("");
  const [blockIncrement, setBlockIncrement] = useState("");
  const [balance, setBalance] = useState("");

  /// functions used for getting values from inputs

  const getCurrency = (e) => setCurrency(e.target.value);
  const getStartBlock = (e) => setStartBlock(e.target.value);
  const getEndBlock = (e) => setEndBlock(e.target.value);
  const getBlockIncrement = (e) => setBlockIncrement(e.target.value);

  // fetch functions data parameters
  const account_Names_Data = JSON.stringify({ _partial_account_name: value });
  const balance_For_Coin_Data = JSON.stringify({
    _account_name: accountName,
    _coin_type: currentCurrency,
    _start_block: currentStartBlock,
    _end_block: currentEndBlock,
    _block_increment: currentIncrement,
  });

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

  ///fetch balance for coin by block

  if (accountName) {
    fetch("http://localhost:3000/rpc/get_balance_for_coin_by_block", {
      method: "post",
      headers: { "Content-Type": "application/json" },
      body: balance_For_Coin_Data,
    })
      .then((response) => response.json())
      .then((res) => setBalance(JSON.parse(res)))
      .catch((err) => console.log(err));
  }

  const handleSubmit = (e) => {
    e.preventDefault();
    names.filter((name) => name === value && setAccountName(name));
    setcurrentCurrency(currency);
    setcurrentStartBlock(startBlock);
    setcurrentEndBlock(endBlock);
    setCurrentIncrement(blockIncrement);
    setValue("");
    setCurrency("");
    setStartBlock("");
    setEndBlock("");
    setBlockIncrement("");
  };

  return (
    <div>
      <div className={styles.container}>
        <div className={styles.container__input}>
          <Parameters
            startBlock={startBlock}
            endBlock={endBlock}
            blockIncrement={blockIncrement}
            currency={currency}
            handleSubmit={handleSubmit}
            getCurrency={getCurrency}
            getStartBlock={getStartBlock}
            getEndBlock={getEndBlock}
            getBlockIncrement={getBlockIncrement}
            value={value}
            setValue={setValue}
          />
          <Dropdown value={value} setValue={setValue} names={names} />
        </div>
        <Button onClick={handleSubmit} color="secondary" variant="contained">
          Show Balances
        </Button>
      </div>

      {!accountName ? "" : <Graph balance={balance} />}
    </div>
  );
}
