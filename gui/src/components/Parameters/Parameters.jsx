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
  currentStartBlock,
  currentEndBlock,
  currentStartDate,
  currentEndDate,
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

  // const startBlockErrors = () => {
  //   if (startBlock > endBlock) {
  //     return true && "Start block must be lower than End block";
  //   }
  // };

  // const endBlockErrors = () => {
  //   if (currentStartBlock > currentEndBlock) {
  //     return true && "End block must be higher than start block";
  //   }

  //   if (currentEndBlock !== "" && currentStartBlock === currentEndBlock) {
  //     return true && "block can't be equal";
  //   }
  // };

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
              helperText={
                currentStartBlock > currentEndBlock &&
                "Start block must be lower than end block"
              }
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
              helperText={
                currentStartBlock > currentEndBlock &&
                "End block must be higher than start block"
              }
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
                  <TextField
                    helperText={
                      currentStartDate > currentEndDate &&
                      "Start date must be lower than end date"
                    }
                    className={styles["input--date"]}
                    {...params}
                  />
                )}
              />
              <DateTimePicker
                label="END DATE"
                value={endDate}
                onChange={(newValue) => setEndDate(newValue)}
                renderInput={(params) => (
                  <TextField
                    helperText={
                      currentStartDate > currentEndDate &&
                      "End date must be higher than start date"
                    }
                    className={styles["input--date"]}
                    {...params}
                  />
                )}
              />
            </LocalizationProvider>
          </div>
        </Stack>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <Button
            size="small"
            style={{ margin: "25px" }}
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
