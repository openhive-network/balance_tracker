import React, { useState, useEffect } from "react";
import { Button } from "@mui/material";
import styles from "./App.module.css";
import Input from "./components/Input/Input";
import Dropdown from "./components/Dropdown/Dropdown";

export default function App() {
  const [value, setValue] = useState("");
  const [names, setNames] = useState("");
  const [submitMessage, setSubmitMessage] = useState("");

  const data = JSON.stringify({ _partial_account_name: value });

  //fetch account names
  useEffect(() => {
    fetch("http://localhost:3000/rpc/find_matching_accounts", {
      method: "post",
      headers: { "Content-Type": "application/json" },

      body: data,
    })
      .then((response) => response.json())
      .then((res) => setNames(JSON.parse(res)));
  }, [data]);

  function handleSubmit(e) {
    e.preventDefault();
    names.filter(
      (name) => name === value && setSubmitMessage(`Showing "${name}" balances`)
    );
    setValue("");
  }

  return (
    <div className={styles.container}>
      <div className={styles.container__input}>
        <Input handleSubmit={handleSubmit} value={value} setValue={setValue} />
        <Dropdown value={value} setValue={setValue} names={names} />
      </div>
      <Button onClick={handleSubmit} color="secondary" variant="contained">
        Show Balances
      </Button>

      <h1>{submitMessage}</h1>
    </div>
  );
}
