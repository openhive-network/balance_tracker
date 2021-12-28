import React, { useState, useEffect } from "react";
import styles from "./App.module.css";
import Parameters from "../Parameters/Parameters"; // <==== HERE PAREMETERS = INPUT FIELDS
import moment from "moment";
import Charts from "../Charts/Charts";
import { Alert, AlertTitle } from "@mui/material";

export default function App() {
  const today = moment().format("YYYY MM DD HH:mm:ss");
  const [value, setValue] = useState("");
  const [names, setNames] = useState([]);
  const [accountName, setAccountName] = useState("");
  const [currency, setCurrency] = useState([]);
  const [startBlock, setStartBlock] = useState("");
  const [endBlock, setEndBlock] = useState("");
  const [currentStartBlock, setCurrentStartBlock] = useState("");
  const [currentEndBlock, setCurrentEndBlock] = useState("");
  const [startDate, setStartDate] = useState(today);
  const [endDate, setEndDate] = useState(today);
  const [currentStartDate, setCurrentStartDate] = useState("");
  const [currentEndDate, setCurrentEndDate] = useState("");

  // ////////////////////////////////// Get data of all names
  const accountNamesData = JSON.stringify({ _partial_account_name: value });
  console.log(currentStartBlock, currentEndBlock);
  // fetch account names
  useEffect(() => {
    fetch("http://localhost:3000/rpc/find_matching_accounts", {
      method: "post",
      headers: { "Content-Type": "application/json" },
      body: accountNamesData,
    })
      .then((response) => response.json())
      .then((res) => setNames(JSON.parse(res)))
      .catch((err) => console.log(err));
  }, [accountNamesData]);

  // / find 3 different currencies
  const [findCurrency13, setFindCurrency13] = useState("");
  const [findCurrency21, setFindCurrency21] = useState("");
  const [findCurrency37, setFindCurrency37] = useState("");

  const handleSubmit = (e) => {
    e.preventDefault();
    names.filter((name) => name === value && setAccountName(value));
    setCurrentStartBlock(startBlock);
    setCurrentEndBlock(endBlock);
    setCurrentStartDate(moment(startDate).format("YYYY MM DD HH:mm:ss"));
    setCurrentEndDate(moment(endDate).format("YYYY MM DD HH:mm:ss"));
    setFindCurrency13(() => currency.filter((num) => num === 13 && num));
    setFindCurrency21(() => currency.filter((num) => num === 21 && num));
    setFindCurrency37(() => currency.filter((num) => num === 37 && num));
  };

  return (
    <div>
      <div className={styles.container}>
        <div className={styles.container__form}>
          <Parameters
            startBlock={startBlock}
            endBlock={endBlock}
            startDate={startDate}
            endDate={endDate}
            setCurrency={setCurrency}
            handleSubmit={handleSubmit}
            setStartBlock={setStartBlock}
            setEndBlock={setEndBlock}
            setStartDate={setStartDate}
            setEndDate={setEndDate}
            setValue={setValue}
            names={names}
            value={value}
            currentStartBlock={currentStartBlock}
            currentEndBlock={currentEndBlock}
            currentStartDate={currentStartDate}
            currentEndDate={currentEndDate}
          />
        </div>
        {accountName && (
          <>
            <div className={styles.content}>
              <h1>Showing balances for {accountName}</h1>
              <Alert severity="info">
                <div className={styles.alert}>
                  <AlertTitle>Information</AlertTitle>
                  <p className={styles.alert__paragraph}>
                    IMPORTANT : When switching from blocks to dates , you may
                    need to reset chart !
                  </p>
                  <li>
                    RESET CHART button updates chart to values entered in
                    start/end block or start/end date , also it refreshes chart
                    in case no values is shown
                  </li>
                  <li>
                    ZOOM IN button zooms chart on every click and always showing
                    1000 points on chart X axis. If you want to zoom in any
                    exact point, use your mouse wheel, or pinch
                  </li>
                </div>
              </Alert>
            </div>

            <div className={styles.content__charts}>
              <Charts
                accountName={accountName}
                currentStartBlock={currentStartBlock}
                currentEndBlock={currentEndBlock}
                findCurrency13={findCurrency13}
                findCurrency21={findCurrency21}
                findCurrency37={findCurrency37}
                currentStartDate={currentStartDate}
                currentEndDate={currentEndDate}
              />
            </div>
          </>
        )}
      </div>
    </div>
  );
}
