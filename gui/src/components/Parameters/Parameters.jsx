import React from "react";
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
}) {
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

        <Button type="submit" color="secondary" variant="contained">
          Show Balances
        </Button>
      </form>
    </>
  );
}
