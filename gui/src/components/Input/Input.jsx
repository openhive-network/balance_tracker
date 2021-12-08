import React from "react";
import { TextField } from "@mui/material";
import styles from "./Input.module.css";

export default function Input({ value, setValue, handleSubmit }) {
  return (
    <>
      <form onSubmit={handleSubmit}>
        <TextField
          className={styles.input}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          label="Search for Account"
        />
      </form>
    </>
  );
}
