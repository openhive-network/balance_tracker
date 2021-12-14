import React from "react";
import {
  FormControl,
  FormControlLabel,
  Checkbox,
  InputLabel,
  TextField,
  Select,
  MenuItem,
  ListItemIcon,
  ListItemText,
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
}) {
  const currencyNumber = [13, 21, 37];
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
              {currencyNumber.map((singleNumber) => (
                <MenuItem value={singleNumber}>
                  <ListItemIcon>
                    <Checkbox />
                  </ListItemIcon>
                  <ListItemText primary={singleNumber} />
                </MenuItem>
              ))}
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
        </div>
      </form>
    </>
  );
}
