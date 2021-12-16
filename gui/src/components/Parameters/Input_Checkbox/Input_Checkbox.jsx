import React, { useState } from "react";
import {
  Checkbox,
  InputLabel,
  ListItemIcon,
  ListItemText,
  MenuItem,
  FormControl,
  Select,
} from "@mui/material";
import styles from "../parameters.module.css";

export default function App({ setCurrency }) {
  const [selected, setSelected] = useState([]);
  const options = [13, 21, 37];    // <======= crypto currencies nai number

  const isAllSelected =
    options.length > 0 && selected.length === options.length;

  const handleChange = (event) => {
    const value = event.target.value;
    if (value[value.length - 1] === "all") {
      setSelected(selected.length === options.length ? [] : options);
      return;
    }
    setSelected(value);
  };
  setCurrency(selected);
  return (
    <FormControl className={styles.input}>
      <InputLabel id="mutiple-select-label">Currency</InputLabel>
      <Select
        labelId="mutiple-select-label"
        multiple
        value={selected}
        onChange={handleChange}
        renderValue={(selected) => selected.join(", ")}
        laber="Currency"
      >
        <MenuItem value="all">
          <ListItemIcon>
            <Checkbox
              checked={isAllSelected}
              indeterminate={
                selected.length > 0 && selected.length < options.length
              }
            />
          </ListItemIcon>
          <ListItemText primary="Select All" />
        </MenuItem>
        {options.map((option) => (
          <MenuItem key={option} value={option}>
            <ListItemIcon>
              <Checkbox checked={selected.indexOf(option) > -1} />
            </ListItemIcon>
            <ListItemText primary={option} />
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
}
