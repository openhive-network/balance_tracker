import React from "react";
import {
  FormControl,
  InputLabel,
  TextField,
  Select,
  MenuItem,
} from "@mui/material";
import styles from "./parameters.module.css";

export default function Input({
  value,
  setValue,
  handleSubmit,
  currency,
  getCurrency,
  startBlock,
  getStartBlock,
  endBlock,
  getEndBlock,
  blockIncrement,
  getBlockIncrement,
}) {
  return (
    <>
      <form style={{ display: "flex" }} onSubmit={handleSubmit}>
        <TextField
          className={styles.input}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          label="Search for Account"
        />
        <div>
          <FormControl onSubmit={handleSubmit} style={{ width: "100px" }}>
            <InputLabel id="demo-simple-select-label">Crypto</InputLabel>
            <Select
              labelId="demo-simple-select-label"
              id="demo-simple-select"
              value={currency}
              label="Crypto"
              onChange={getCurrency}
            >
              <MenuItem value="Hive">Hive</MenuItem>
              <MenuItem value="HBD">HBD</MenuItem>
            </Select>
          </FormControl>
          <TextField
            value={startBlock}
            onChange={getStartBlock}
            id="outlined-basic"
            label="Start Block"
            variant="outlined"
          />
          <TextField
            value={endBlock}
            onChange={getEndBlock}
            id="outlined-basic"
            label="End Block"
            variant="outlined"
          />
          <TextField
            value={blockIncrement}
            onChange={getBlockIncrement}
            id="outlined-basic"
            label="Block Increment"
            variant="outlined"
          />
        </div>
      </form>
    </>
  );
}
