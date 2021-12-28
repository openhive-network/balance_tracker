import React, { useState } from "react";
import { TextField, Button, Stack } from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import styles from "./parameters.module.css";
import InputCheckbox from "./Input_Checkbox/Input_Checkbox";
import AdapterDateFns from "@mui/lab/AdapterDateFns";
import LocalizationProvider from "@mui/lab/LocalizationProvider";
import DateTimePicker from "@mui/lab/DateTimePicker";

export default function Parameters({
  setValue,
  handleSubmit,
  setCurrency,
  startBlock,
  setStartBlock,
  endBlock,
  setEndBlock,
  startDate,
  setStartDate,
  endDate,
  setEndDate,
  names,
  value,
}) {
  const [showDates, setShowDates] = useState(false);
  const [showDatesBtnText, setShowDatesBtnText] = useState("Choose Dates");

  const handleDatesButton = () => {
    setShowDates(!showDates);
    setShowDatesBtnText(() =>
      showDatesBtnText === "Choose Dates" ? "Choose Blocks" : "Choose Dates"
    );
  };
  localStorage.setItem("Chart Value", showDatesBtnText);

  const re = /^[0-9\b]+$/; ///<==== type only numbers validation //

  return (
    <>
      <form required onSubmit={handleSubmit}>
        <Stack direction={{ xs: "column", md: "row" }}>
          <Autocomplete
            className={styles.input}
            inputValue={value}
            onInputChange={(event, newInputValue) => setValue(newInputValue)}
            id="controllable-states-demo"
            options={names !== null ? names : [""]}
            renderInput={(params) => (
              <TextField
                required={true}
                {...params}
                label="Search for account"
              />
            )}
          />
          <InputCheckbox
            handleSubmit={handleSubmit}
            setCurrency={setCurrency}
          />
          <div
            style={
              showDates === true ? { display: "none" } : { display: "block" }
            }
          >
            <TextField
              required={showDates === true ? false : true}
              className={styles.input}
              value={startBlock}
              onChange={(e) => {
                if (e.target.value === "" || re.test(e.target.value))
                  setStartBlock(e.target.value);
              }}
              id="outlined-basic"
              label="Start Block"
              variant="outlined"
            />
            <TextField
              required={showDates === true ? false : true}
              className={styles.input}
              value={endBlock}
              onChange={(e) => {
                if (e.target.value === "" || re.test(e.target.value))
                  setEndBlock(e.target.value);
              }}
              id="outlined-basic"
              label="End Block"
              variant="outlined"
            />
          </div>

          <div
            style={
              showDates === false ? { display: "none" } : { display: "block" }
            }
          >
            <LocalizationProvider dateAdapter={AdapterDateFns}>
              <DateTimePicker
                label="START DATE"
                value={startDate}
                onChange={(newValue) => setStartDate(newValue)}
                renderInput={(params) => (
                  <TextField className={styles["input__date"]} {...params} />
                )}
              />
              <DateTimePicker
                label="END DATE"
                value={endDate}
                onChange={(newValue) => setEndDate(newValue)}
                renderInput={(params) => (
                  <TextField className={styles["input__date"]} {...params} />
                )}
              />
            </LocalizationProvider>
          </div>
        </Stack>
        <div className={styles.form__buttons}>
          <Button
            size="small"
            className={styles["form__button--show-dates"]}
            onClick={handleDatesButton}
            type="button"
            color="warning"
            variant="contained"
          >
            {showDatesBtnText}
          </Button>

          <Button
            size="large"
            type="submit"
            color="secondary"
            variant="contained"
          >
            Show Balances
          </Button>
        </div>
      </form>
    </>
  );
}
