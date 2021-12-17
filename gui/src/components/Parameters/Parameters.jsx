import React, { useState } from "react";
import { TextField, Button } from "@mui/material";
import styles from "./parameters.module.css";
import InputCheckbox from "./Input_Checkbox/Input_Checkbox";

export default function Parameters({
  value,
  setValue,
  handleSubmit,
  setCurrency,
  startBlock,
  getStartBlock,
  endBlock,
  getEndBlock,
  startDate,
  getStartDate,
  endDate,
  getEndDate,
  dateIncrement,
  getDateIncrement,
}) {
  const [showDates, setShowDates] = useState(false);
  const [showDatesBtnText, setShowDatesBtnText] = useState("Choose Dates");

  const handleDatesButton = () => {
    setShowDates(!showDates);
    setShowDatesBtnText(() =>
      showDatesBtnText === "Choose Dates" ? "Choose Blocks" : "Choose Dates"
    );
  };
  return (
    <>
      <form
        style={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        }}
        onSubmit={handleSubmit}
      >
        <TextField
          className={styles.input}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          label="Search for Account"
        />

        <InputCheckbox handleSubmit={handleSubmit} setCurrency={setCurrency} />
        <div
          className="blocks__parameters"
          style={showDates === true ? { display: "none" } : { display: "flex" }}
        >
          <TextField
            className={styles.input}
            value={startBlock}
            onChange={getStartBlock}
            id="outlined-basic"
            label="Start Block"
            variant="outlined"
          />
          <TextField
            className={styles.input}
            value={endBlock}
            onChange={getEndBlock}
            id="outlined-basic"
            label="End Block"
            variant="outlined"
          />
        </div>
        <div
          // className={
          //   showDates !== false ? styles["dates_parameters"] : styles.hidden
          // }
          className="dates__parameters"
          style={
            showDates === false ? { display: "none" } : { display: "flex" }
          }
        >
          <TextField
            className={styles.input}
            value={startDate}
            onChange={getStartDate}
            id="outlined-basic"
            label="Start Date"
            variant="outlined"
          />
          <TextField
            className={styles.input}
            value={endDate}
            onChange={getEndDate}
            id="outlined-basic"
            label="End Date"
            variant="outlined"
          />
          <TextField
            className={styles.input}
            value={dateIncrement}
            onChange={getDateIncrement}
            id="outlined-basic"
            label="Date increment"
            variant="outlined"
          />
        </div>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <Button
            style={{ margin: "25px" }}
            onClick={handleDatesButton}
            type="button"
            color="success"
            variant="contained"
          >
            {showDatesBtnText}
          </Button>

          <Button type="submit" color="secondary" variant="contained">
            Show Balances
          </Button>
        </div>
      </form>
    </>
  );
}
