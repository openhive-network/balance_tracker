import React, { useState } from "react";
import { TextField, Button, Autocomplete, Stack } from "@mui/material";
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

  return (
    <>
      <form onSubmit={handleSubmit}>
        <Stack direction={{ xs: "column", md: "row" }}>
          <Autocomplete
            className={styles.input}
            freeSolo
            disableClearable
            options={names && names.map((name) => name)}
            renderInput={(params) => {
              setValue(params.inputProps.value);
              return (
                <TextField
                  {...params}
                  label="Search for account"
                  InputProps={{
                    ...params.InputProps,
                    type: "search",
                  }}
                />
              );
            }}
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
              className={styles.input}
              value={startBlock}
              onChange={(e) => setStartBlock(e.target.value)}
              id="outlined-basic"
              label="Start Block"
              variant="outlined"
            />
            <TextField
              className={styles.input}
              value={endBlock}
              onChange={(e) => setEndBlock(e.target.value)}
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
                  <TextField className={styles["input--date"]} {...params} />
                )}
              />
              <DateTimePicker
                label="END DATE"
                value={endDate}
                onChange={(newValue) => setEndDate(newValue)}
                renderInput={(params) => (
                  <TextField className={styles["input--date"]} {...params} />
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
